# Native metric storage metadata fixture

`delta.json` freezes the PoC metadata shape: two authoritative samples artifacts
and one independently owned native series index covering both region-qualified IDs.
It contains metadata only, not codec payloads. The shared `FileMeta` fields remain
outside the format enum, and native per-object sizes sum to `file_size`.

Run:

```shell
cargo nextest run -p mito2 -E 'test(test_native_metric_manifest_fixture)'
```

The test checks decoding, checkpoint replay, automatic index retirement when a
covered sample is removed, rejection of stale coverage, and default Parquet
interpretation of the existing legacy edit fixture. These are new-reader checks;
old readers do not understand the native format enum and cannot be used for a
downgrade after native artifacts are published. Native creation remains opt-in in
this PoC. Do not enable it on production regions as part of this experiment.
