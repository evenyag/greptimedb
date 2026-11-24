# Historical series-index catalog fixture

`legacy_catalog.json` records the optional local Parquet catalog layout at
`3ad5f963285e51166fd8789920e4b3bf2cf60423`. The fixture is synthetic.
Native Narthex maintenance ignores these catalogs and discovers exact keys
from authoritative samples when an index lacks certified coverage.

The [Narthex manifest fixtures](../../manifest/narthex/) exercise the upgrade
from source lists to window certificates, including checkpoint restore and
independent index replacement. Run them with
`cargo test -p mito2 --lib test_narthex_manifest_fixture`.
