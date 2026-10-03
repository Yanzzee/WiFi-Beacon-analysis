# Corrected Golden Contracts

`corrected_semantics_v1.json` is the authoritative corrected metric/schema
golden. Its dedicated human-readable inputs and expected tables are under
`tests/fixtures/corrected_semantics/`.

The tests compare logical content, not Parquet or PNG bytes. Floating-point
values use the tolerances declared in the golden, and timestamp/duration/QBSS
storage is validated separately by the artifact-writer tests.

`legacy_corrected_comparison_v1.json` is explanatory, not a second production
oracle. It uses the same small selected-observation input to preserve a frozen
snapshot of the former location-wide calculation and the current corrected
classroom result. Tests verify the input hash and corrected values but do not
execute or maintain legacy code.

Historical reproduction of the original refactored fingerprint is available
from Git commit `4a20d1c8f6164fdb81fc75f2cf80b9fba3f1c5fb`.
