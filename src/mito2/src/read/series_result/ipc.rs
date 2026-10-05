// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Completed IPC files with one shared decoder/directory and independent cursors.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_ipc::reader::{FileDecoder, read_footer_length};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_ipc::{Block, root_as_footer, root_as_message};
use datatypes::arrow::array::Array;
use datatypes::arrow::buffer::Buffer;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::read::series_result::resources::{Charge, Kind, OwnedFile};
use crate::read::series_result::{
    BatchData, StoreOptions, StoreResources, arrow, checked_add, checked_mul, convert, fail,
    normalized_size, schema_memory, unique_batch_bytes,
};

/// Conservatively reserves per-batch IPC/compression scratch before Arrow allocates.
pub(crate) fn write_bound(batch: &RecordBatch, options: &StoreOptions) -> Result<usize> {
    let dictionary = options
        .fixed_keys
        .as_ref()
        .map_or(0, |a| a.get_array_memory_size());
    checked_add(
        checked_mul(normalized_size(batch)?, 3)?,
        checked_add(dictionary, 4096)?,
    )
}

struct CountWriter {
    file: File,
    owned: Arc<OwnedFile>,
}

impl Write for CountWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        #[cfg(test)]
        {
            if let Some((started, resume)) = self
                .owned
                .resources
                .faults
                .write_gate
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                let _ = started.send(());
                let _ = resume.recv();
            }
            let limit = *self
                .owned
                .resources
                .faults
                .write_after
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if limit.is_some_and(|n| self.owned.bytes.load(Ordering::SeqCst) >= n) {
                return Err(std::io::Error::other("injected write failure"));
            }
        }
        self.owned.add_bytes(buf.len())?;
        let result = self.file.write(buf);
        let written = result.as_ref().copied().unwrap_or(0);
        let unused = buf.len() - written;
        self.owned.bytes.fetch_sub(unused, Ordering::SeqCst);
        self.owned.resources.disk_shrink(unused);
        self.owned.resources.count("write_calls", 1);
        self.owned
            .resources
            .count("filesystem_write_bytes", written);
        result
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub(crate) struct OpenWriter {
    // Closing the writer precedes releasing its file owner.
    writer: Option<FileWriter<CountWriter>>,
    owner: Arc<OwnedFile>,
    schema: SchemaRef,
    pub(crate) rows: Vec<usize>,
    bounds: Vec<usize>,
    metadata: Arc<Mutex<Charge>>,
    schema_bound: usize,
}

impl Drop for OpenWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            self.owner.resources.drop_blocking(writer);
        }
    }
}

impl OpenWriter {
    pub(crate) fn new(resources: Arc<StoreResources>, schema: SchemaRef) -> Result<Self> {
        let dictionary = resources
            .options
            .fixed_keys
            .as_ref()
            .map_or(0, |a| a.get_array_memory_size());
        let base = checked_add(dictionary, 4096)?;
        let schema_bound = checked_mul(schema_memory(&schema)?, 2)?;
        if checked_add(base, schema_bound)? > resources.options.file_metadata_bytes {
            return Err(fail("schema/dictionary exceeds file metadata limit"));
        }
        let metadata = Arc::new(Mutex::new(resources.reserve(Kind::Metadata, base)?));
        let _workspace = resources.reserve(
            Kind::Workspace,
            checked_mul(checked_add(base, schema_bound)?, 2)?,
        )?;
        let path = resources
            .root
            .join(format!("{}.partial", uuid::Uuid::new_v4()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| fail(format!("create {}: {e}", path.display())))?;
        let owner = Arc::new(OwnedFile {
            metadata: metadata.clone(),
            path,
            partial_link: None,
            bytes: AtomicUsize::new(0),
            resources: resources.clone(),
        });
        let options =
            arrow(IpcWriteOptions::default().try_with_compression(resources.options.compression))?;
        let writer = arrow(FileWriter::try_new_with_options(
            CountWriter {
                file,
                owned: owner.clone(),
            },
            &schema,
            options,
        ))?;
        resources.count("files", 1);
        resources.count("file_opens", 1);
        Ok(Self {
            writer: Some(writer),
            owner,
            schema,
            rows: vec![],
            bounds: vec![],
            metadata,
            schema_bound,
        })
    }

    pub(crate) fn fits(&self, bound: usize) -> bool {
        let options = &self.owner.resources.options;
        let metadata_bytes = self
            .metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .bytes();
        let Some(footer_bound) = metadata_bytes.checked_add(self.schema_bound) else {
            return false;
        };
        self.rows.len() < options.file_batches
            && footer_bound
                .checked_add(256)
                .is_some_and(|n| n <= options.file_metadata_bytes)
            && self
                .owner
                .bytes
                .load(Ordering::SeqCst)
                .checked_add(bound)
                .and_then(|n| n.checked_add(footer_bound))
                .is_some_and(|n| n <= options.file_bytes)
    }

    pub(crate) fn append(&mut self, batch: &RecordBatch) -> Result<()> {
        let resources = &self.owner.resources;
        let bound = write_bound(batch, &resources.options)?;
        if !self.fits(bound) {
            return Err(fail("single batch exceeds IPC file limits"));
        }
        let _workspace = resources.reserve(Kind::Workspace, bound)?;
        self.metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .grow(256)?;
        self.rows
            .try_reserve_exact(1)
            .map_err(|e| fail(e.to_string()))?;
        self.bounds
            .try_reserve_exact(1)
            .map_err(|e| fail(e.to_string()))?;
        resources.serialization.measure(|| {
            arrow(
                self.writer
                    .as_mut()
                    .ok_or_else(|| fail("closed IPC writer"))?
                    .write(batch),
            )
        })?;
        self.rows.push(batch.num_rows());
        self.bounds.push(bound);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Arc<FilePart>> {
        let resources = self.owner.resources.clone();
        let _workspace = resources.reserve(
            Kind::Workspace,
            checked_mul(
                checked_add(
                    self.metadata
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .bytes(),
                    self.schema_bound,
                )?,
                3,
            )?,
        )?;
        #[cfg(test)]
        if resources.faults.finish.load(Ordering::SeqCst) {
            return Err(fail("injected finalization failure"));
        }
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| fail("closed IPC writer"))?;
        resources.serialization.measure(|| arrow(writer.finish()))?;
        drop(writer);
        // Only finalized files reach initialization. No handle to a partial file escapes.
        resources.initialization.measure(|| {
            let mut file = File::open(&self.owner.path).map_err(|e| fail(e.to_string()))?;
            resources.count("file_opens", 1);
            let size = usize::try_from(file.metadata().map_err(|e| fail(e.to_string()))?.len())
                .map_err(|_| fail("file size overflow"))?;
            if size < 10 {
                return Err(fail("truncated IPC file"));
            }
            let mut trailer = [0; 10];
            read_at(&mut file, size - 10, &mut trailer, &resources)?;
            let footer_len = arrow(read_footer_length(trailer))?;
            if footer_len > resources.options.file_metadata_bytes || footer_len > size - 10 {
                return Err(fail("IPC footer exceeds bounds"));
            }
            let footer_offset = size - 10 - footer_len;
            let mut footer_bytes = vec![0; footer_len];
            read_at(&mut file, footer_offset, &mut footer_bytes, &resources)?;
            let footer = root_as_footer(&footer_bytes)
                .map_err(|e| fail(format!("invalid IPC footer: {e}")))?;
            let blocks = footer
                .recordBatches()
                .ok_or_else(|| fail("missing record directory"))?;
            if blocks.len() != self.rows.len() {
                return Err(fail("IPC directory size mismatch"));
            }
            let ipc_schema = footer.schema().ok_or_else(|| fail("missing IPC schema"))?;
            if !ipc_schema.endianness().equals_to_target_endianness()
                || arrow_ipc::convert::fb_to_schema(ipc_schema) != *self.schema
            {
                return Err(fail("IPC schema does not match finalized result"));
            }
            let mut decoder = FileDecoder::new(self.schema.clone(), footer.version());
            let mut directory = Vec::with_capacity(blocks.len());
            for block in blocks {
                block_range(block, footer_offset)?;
                directory.push(*block);
            }
            if let Some(dictionaries) = footer.dictionaries() {
                if dictionaries.len() > 1 {
                    return Err(fail("IPC dictionary replacement is forbidden"));
                }
                for block in dictionaries {
                    let range = block_range(block, footer_offset)?;
                    if range.len() > resources.options.file_metadata_bytes {
                        return Err(fail("dictionary block exceeds metadata bound"));
                    }
                    let mut bytes = vec![0; range.len()];
                    read_at(&mut file, range.start, &mut bytes, &resources)?;
                    let expected = resources
                        .options
                        .fixed_keys
                        .as_ref()
                        .ok_or_else(|| fail("unexpected dictionary"))?
                        .len();
                    validate_message(
                        &bytes,
                        block,
                        expected,
                        resources.options.file_metadata_bytes,
                        true,
                    )?;
                    arrow(decoder.read_dictionary(block, &Buffer::from(bytes)))?;
                }
            }
            resources.count("footer_bytes", footer_len);
            resources.count("serialized_bytes", size);
            resources.count("metadata_initializations", 1);
            resources.count(
                "batch_directory_bytes",
                checked_mul(directory.len(), std::mem::size_of::<Block>())?,
            );
            drop(file);
            let owner = Arc::get_mut(&mut self.owner)
                .ok_or_else(|| fail("unfinalized file has other owners"))?;
            let finalized = owner.path.with_extension("arrow");
            // hard_link is an atomic no-clobber publication within this filesystem.
            // Register both owned names before unlinking, so an unlink error is recoverable.
            std::fs::hard_link(&owner.path, &finalized).map_err(|e| fail(e.to_string()))?;
            let partial = std::mem::replace(&mut owner.path, finalized);
            owner.partial_link = Some(partial.clone());
            std::fs::remove_file(&partial).map_err(|e| fail(e.to_string()))?;
            owner.partial_link = None;
            Ok(Arc::new(FilePart {
                owner: self.owner.clone(),
                decoder,
                directory,
                rows: std::mem::take(&mut self.rows),
                bounds: std::mem::take(&mut self.bounds),
                footer_offset,
            }))
        })
    }
}

pub(crate) struct FilePart {
    owner: Arc<OwnedFile>,
    decoder: FileDecoder,
    directory: Vec<Block>,
    pub(crate) rows: Vec<usize>,
    bounds: Vec<usize>,
    footer_offset: usize,
}

impl FilePart {
    pub(crate) fn replay_bytes(&self, batch: usize) -> Result<usize> {
        checked_mul(self.bounds[batch], 4)
    }

    pub(crate) fn read(
        &self,
        batch: usize,
        schema: &SchemaRef,
        open: &mut Option<(usize, File)>,
        pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Result<Arc<BatchData>> {
        let resources = &self.owner.resources;
        #[cfg(test)]
        if resources.faults.read.load(Ordering::SeqCst) {
            return Err(fail("injected read failure"));
        }
        let block = self
            .directory
            .get(batch)
            .ok_or_else(|| fail("invalid batch index"))?;
        let range = block_range(block, self.footer_offset)?;
        let bound = self.bounds[batch];
        if range.len() > bound {
            return Err(fail("encoded block exceeds admitted bound"));
        }
        let _workspace = resources.reserve_in(Kind::Workspace, checked_mul(bound, 3)?, pool)?;
        let id = Arc::as_ptr(&self.owner) as usize;
        if open.as_ref().is_none_or(|(current, _)| *current != id) {
            let file = File::open(&self.owner.path)
                .map_err(|e| fail(format!("open {}: {e}", self.owner.path.display())))?;
            resources.count("file_opens", 1);
            *open = Some((id, file));
        }
        let file = &mut open.as_mut().ok_or_else(|| fail("missing cursor file"))?.1;
        let mut bytes = vec![0; range.len()];
        resources.count("logical_requested_bytes", range.len());
        read_at(file, range.start, &mut bytes, resources)?;
        validate_message(&bytes, block, self.rows[batch], bound, false)?;
        let buffer = Buffer::from(bytes);
        let decoded = resources
            .decoding
            .measure(|| arrow(self.decoder.read_record_batch(block, &buffer)))?
            .ok_or_else(|| fail("missing IPC record batch"))?;
        let compact = resources
            .reconstruction
            .measure(|| convert(&decoded, schema))?;
        let size = unique_batch_bytes(&compact)?;
        let charge = resources.reserve_in(Kind::Payload, size, pool)?;
        resources.count("decoded_rows", compact.num_rows());
        resources.count("decoded_batches", 1);
        resources.peak("replay_payload_bytes", size);
        BatchData::new(compact, charge)
    }
}

fn read_at(
    file: &mut File,
    offset: usize,
    bytes: &mut [u8],
    resources: &StoreResources,
) -> Result<()> {
    file.seek(SeekFrom::Start(
        u64::try_from(offset).map_err(|_| fail("seek overflow"))?,
    ))
    .map_err(|e| fail(e.to_string()))?;
    resources.count("seeks", 1);
    let mut read = 0;
    while read < bytes.len() {
        let n = file
            .read(&mut bytes[read..])
            .map_err(|e| fail(e.to_string()))?;
        resources.count("read_calls", 1);
        resources.count("filesystem_read_bytes", n);
        if n == 0 {
            return Err(fail("unexpected EOF in IPC block"));
        }
        read = checked_add(read, n)?;
    }
    Ok(())
}

fn block_range(block: &Block, limit: usize) -> Result<std::ops::Range<usize>> {
    let offset = usize::try_from(block.offset()).map_err(|_| fail("negative IPC block offset"))?;
    let meta =
        usize::try_from(block.metaDataLength()).map_err(|_| fail("negative IPC metadata size"))?;
    let body = usize::try_from(block.bodyLength()).map_err(|_| fail("negative IPC body size"))?;
    let end = checked_add(offset, checked_add(meta, body)?)?;
    if meta < 8 || offset < 8 || end > limit {
        return Err(fail("IPC block outside file"));
    }
    Ok(offset..end)
}

/// Checks decoded allocation claims before Arrow decompresses untrusted block bytes.
fn validate_message(
    bytes: &[u8],
    block: &Block,
    rows: usize,
    bound: usize,
    dictionary: bool,
) -> Result<()> {
    let metadata_len =
        usize::try_from(block.metaDataLength()).map_err(|_| fail("invalid metadata length"))?;
    if bytes.len() < metadata_len || metadata_len < 8 || bytes[..4] != [255; 4] {
        return Err(fail("invalid IPC continuation marker"));
    }
    let message = root_as_message(&bytes[8..metadata_len]).map_err(|e| fail(e.to_string()))?;
    let batch = if dictionary {
        let dictionary = message
            .header_as_dictionary_batch()
            .ok_or_else(|| fail("expected dictionary batch"))?;
        if dictionary.isDelta() {
            return Err(fail("dictionary deltas are forbidden"));
        }
        dictionary
            .data()
            .ok_or_else(|| fail("missing dictionary data"))?
    } else {
        message
            .header_as_record_batch()
            .ok_or_else(|| fail("expected record batch"))?
    };
    if usize::try_from(batch.length()).ok() != Some(rows) {
        return Err(fail("IPC row count mismatch"));
    }
    let body = &bytes[metadata_len..];
    let mut total = 0usize;
    for buffer in batch.buffers().ok_or_else(|| fail("missing IPC buffers"))? {
        let start = usize::try_from(buffer.offset()).map_err(|_| fail("negative buffer offset"))?;
        let len = usize::try_from(buffer.length()).map_err(|_| fail("negative buffer size"))?;
        let end = checked_add(start, len)?;
        if end > body.len() {
            return Err(fail("buffer outside record batch"));
        }
        let decoded = if batch.compression().is_some() && len > 0 {
            if len < 8 {
                return Err(fail("truncated compressed buffer"));
            }
            let prefix: [u8; 8] = body[start..start + 8]
                .try_into()
                .map_err(|_| fail("invalid compressed length"))?;
            let decoded = i64::from_le_bytes(prefix);
            if decoded == -1 {
                len - 8
            } else {
                usize::try_from(decoded).map_err(|_| fail("invalid decoded length"))?
            }
        } else {
            len
        };
        total = checked_add(total, decoded)?;
        if total > bound {
            return Err(fail("decoded buffer claims exceed admitted size"));
        }
    }
    if let Some(nodes) = batch.nodes() {
        for node in nodes {
            let len = usize::try_from(node.length()).map_err(|_| fail("negative array length"))?;
            let nulls =
                usize::try_from(node.null_count()).map_err(|_| fail("negative null count"))?;
            if len > bound || nulls > len {
                return Err(fail("invalid IPC field node"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
impl FilePart {
    pub(crate) fn test_first_offset(&self) -> u64 {
        self.directory[0].offset() as u64
    }
}
