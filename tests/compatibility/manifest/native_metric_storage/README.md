# Narthex storage metadata fixtures

`delta.json` freezes the PoC metadata shape: two authoritative samples artifacts
and one independently owned series index covering both region-qualified IDs.
It contains metadata only, not codec payloads. The shared `FileMeta` fields remain
outside the format enum, and per-object sizes sum to `file_size`.

Run:

```shell
cargo nextest run -p mito2 -E 'test(test_native_metric_manifest_fixture)'
```

The test checks decoding, checkpoint replay, automatic index retirement when a
covered sample is removed, rejection of stale coverage, and default Parquet
interpretation of the existing legacy edit fixture. These are new-reader checks;
old readers do not understand the Narthex format enum and cannot be used for a
downgrade after Narthex artifacts are published. Narthex creation remains opt-in in
this PoC. Do not enable it on production regions as part of this experiment.

The fixture retains the original `native/<file-id>/` object paths. Replay preserves
these paths, and the test checks that storage listings still recover their owners.
New writers use `<file-id>/` directly under the region directory; existing objects
are not relocated by this change.

`metrics-delta.json` retains the intermediate `metrics_samples` and
`metrics_series_index` names. `narthex-delta.json` carries the same metadata with
the canonical `narthex_samples` and `narthex_series_index` names. The test compares
all three fixtures and checks that serialization emits Narthex names. Readers
retain aliases for both previous spellings; object paths and codec payloads are
unchanged. The region option is now `sst_format = 'narthex'`, with `native` and
`metrics` accepted as legacy aliases. Readers predating this rename do not
recognize the new enum names.
