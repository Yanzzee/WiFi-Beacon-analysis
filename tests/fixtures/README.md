# Test Fixtures

`corrected_semantics/` contains the small, repository-owned oracle for the
supported calculation. Its CSV inputs cover timestamp precedence, TSF
ordering/duplicates/fallback, fixed bins and terminal partial intervals,
first-observed selection, CU missingness states, radio membership/fallback,
coverage, both streak interpretations, and the five-artifact contract.

Optional real-capture tests use owner-provided files under
`Data/captures_test/` and are marked `requires_external_tools`. These files are
evidence for decoder/adapter/band behavior; they are not the calculation
oracle and are not required for the ordinary unit suite.
