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

//! Parquet page reader.

// use std::collections::VecDeque;

use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::Result;
use parquet::file::reader::ChunkReader;
use parquet::file::serialized_reader::SerializedPageReader;

use crate::cache::{CacheManagerRef, PageKey};

// /// A reader that reads from cached pages.
// pub(crate) struct CachedPageReader {
//     /// Cached pages.
//     pages: VecDeque<Page>,
// }

// impl CachedPageReader {
//     /// Returns a new reader from existing pages.
//     pub(crate) fn new(pages: &[Page]) -> Self {
//         Self {
//             pages: pages.iter().cloned().collect(),
//         }
//     }
// }

// impl PageReader for CachedPageReader {
//     fn get_next_page(&mut self) -> Result<Option<Page>> {
//         Ok(self.pages.pop_front())
//     }

//     fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
//         Ok(self.pages.front().map(page_to_page_meta))
//     }

//     fn skip_next_page(&mut self) -> Result<()> {
//         // When the `SerializedPageReader` is in `SerializedPageReaderState::Pages` state, it never pops
//         // the dictionary page. So it always return the dictionary page as the first page. See:
//         // https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L766-L770
//         // But the `GenericColumnReader` will read the dictionary page before skipping records so it won't skip dictionary page.
//         // So we don't need to handle the dictionary page specifically in this method.
//         // https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/reader.rs#L322-L331
//         self.pages.pop_front();
//         Ok(())
//     }
// }

// impl Iterator for CachedPageReader {
//     type Item = Result<Page>;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.get_next_page().transpose()
//     }
// }

/// A reader that reads from cached pages.
pub(crate) struct CachedPageReader<R: ChunkReader> {
    page_key: PageKey,
    cache: CacheManagerRef,
    page_idx: usize,
    inner_reader: SerializedPageReader<R>,
}

impl<R: ChunkReader> CachedPageReader<R> {
    /// Returns a new reader from cache.
    pub(crate) fn new(
        page_key: PageKey,
        cache: CacheManagerRef,
        inner_reader: SerializedPageReader<R>,
    ) -> Self {
        Self {
            page_key,
            cache,
            page_idx: 0,
            inner_reader,
        }
    }
}

impl<R: ChunkReader> PageReader for CachedPageReader<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        if let Some(page) = self
            .cache
            .get_one_page(self.page_key.clone(), self.page_idx)
        {
            self.page_idx += 1;
            return Ok(Some(page));
        }

        if let Some(page) = self.inner_reader.get_next_page()? {
            self.cache
                .put_one_page(self.page_key.clone(), self.page_idx, page.clone());
            self.page_idx += 1;
            return Ok(Some(page));
        }

        Ok(None)
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        if let Some(page) = self
            .cache
            .get_one_page(self.page_key.clone(), self.page_idx)
        {
            return Ok(Some(page_to_page_meta(&page)));
        }

        self.inner_reader.peek_next_page()
    }

    fn skip_next_page(&mut self) -> Result<()> {
        self.page_idx += 1;
        self.inner_reader.skip_next_page()
    }
}

impl<R: ChunkReader> Iterator for CachedPageReader<R> {
    type Item = Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

/// Get [PageMetadata] from `page`.
///
/// The conversion is based on [decode_page()](https://github.com/apache/arrow-rs/blob/1d6feeacebb8d0d659d493b783ba381940973745/parquet/src/file/serialized_reader.rs#L438-L481)
/// and [PageMetadata](https://github.com/apache/arrow-rs/blob/65f7be856099d389b0d0eafa9be47fad25215ee6/parquet/src/column/page.rs#L279-L301).
fn page_to_page_meta(page: &Page) -> PageMetadata {
    match page {
        Page::DataPage { num_values, .. } => PageMetadata {
            num_rows: None,
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        Page::DataPageV2 {
            num_values,
            num_rows,
            ..
        } => PageMetadata {
            num_rows: Some(*num_rows as usize),
            num_levels: Some(*num_values as usize),
            is_dict: false,
        },
        Page::DictionaryPage { .. } => PageMetadata {
            num_rows: None,
            num_levels: None,
            is_dict: true,
        },
    }
}
