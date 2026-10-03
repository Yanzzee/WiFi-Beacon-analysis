This project analyzes Wi-Fi beacon captures.

Accuracy is more important than ease of use.

Do not change analysis math unless explicitly instructed.

Do not silently infer missing fields.

Preserve traceability from summary outputs back to source rows.

The purpose of this project is to measure WiFi metrics from beacon captures, including channel utilization, station count, admission capacity, among others. These are compared year over year in different capture projects. As 6 GHz radios are installed and 6 GHz clients are adopted, the main questions are how the 5 and 6 GHz bands differ in measurements, and yearly projects will be compared to see long term trends.


Do not compare PNG bytes in regression tests.

Use golden fingerprints for parquet/csv outputs.


Working on these phases individually:
✅ Freeze behavior
✅ Build golden dataset
✅ Build regression tests
✅ Fix reproducible environment
✅ Commit
✅ Mechanically convert one notebook at a time
✅ Refactor into modules
Add 6 GHz support & post-refactor design items


After refactoring has been completed, it became evident that decisions would be needed that would not maintain an exact output of the golden fingerprint. Because of this, the refactored git version is referenced for legacy reports only, and there is no need to keep a separate legacy process or maintain golden fingerprint compatibility from the original input goldent test. 