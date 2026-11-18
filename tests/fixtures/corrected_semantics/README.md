# Corrected-semantics oracle fixture

This directory encodes the owner-accepted logical cases in
[`docs/corrected_semantics_fixture_spec.md`](../../../docs/corrected_semantics_fixture_spec.md).
It is test data only and is not a production input or output directory.

`input/` contains small synthetic class, identity, and beacon-observation
records. Tests should select one `fixture_id` at a time. `expected/` contains
the five production-shaped logical artifacts for the integrated `CF05_10`
case. Focused CF-01 through CF-04, CF-08, and CF-09 assertions live in the
corrected semantic golden rather than creating additional durable artifact
types.

Identifiers recorded in these retained historical fixtures:

```text
beacon.metric/corrected-cu/v1
beacon.schema/analysis-results/v1
```

Current Alpha 0.2 output uses `beacon.metric/corrected-cu/v2` with the same
analysis schema ID. Observation time controls bin placement under
[ADR-0019](../../../docs/decisions/0019-observation-time-and-native-tsf-diagnostics.md).
V2 tests preserve the integrated fixture's measurement values while replacing
old mapped-TSF metadata. The original CSVs and v1 golden remain historical evidence.

CSV nulls are empty fields. Times are UTC ISO-8601 strings. Durations are
integer microseconds, TSF is integer microseconds, raw QBSS CU is nullable
`0..255`, and ratios are unrounded decimal `float64` expectations. Semicolon-
separated values in logical CSV list columns represent Parquet list values in
the physical artifacts. The identity map deliberately has no static preferred-
CU-source flag; CU preference is selected per radio/class session.

Production calculations do not read these expected files. Focused timing,
selection, identity, summary, artifact-writer, and manifest tests calculate
from `input/` and compare logical results with `expected/` and
[`corrected_semantics_v1.json`](../../golden/corrected_semantics_v1.json).

Valid Aruba time precedes frame-only time. Within the same timestamp-source
class, `source_rank` is caller-declared precedence for overlapping capture
sources and lower values win. Capture discovery assigns that rank before the
adapter boundary. Equal ranks use observation time and stable source-path/frame
order.
