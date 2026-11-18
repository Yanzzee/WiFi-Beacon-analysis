# Test Fixtures

`corrected_semantics/` contains the small, repository-owned oracle for the
supported calculation. Its CSV inputs cover timestamp precedence, TSF
ordering/duplicates/fallback, fixed bins and terminal partial intervals,
first-observed selection, CU missingness states, radio membership/fallback,
coverage, both streak interpretations, and the five-artifact contract.

Optional real-capture tests use owner-provided files under
`data/captures_test/` and are marked `requires_external_tools`. These files are
evidence for decoder/adapter/band behavior; they are not the calculation
oracle and are not required for the ordinary unit suite.

The retained mapped-TSF cases describe historical metric v1. Alpha 0.2 uses
observation-time bins and independent native TSF diagnostics under
[ADR-0019](../../docs/decisions/0019-observation-time-and-native-tsf-diagnostics.md).
Current tests preserve the integrated measurement values and separately verify
the v2 timing metadata; historical expected CSVs are not regenerated.
