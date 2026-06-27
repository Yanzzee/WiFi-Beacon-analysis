This project analyzes Wi-Fi beacon captures.

Accuracy is more important than ease of use.

Do not change analysis math unless explicitly instructed.

Do not silently infer missing fields.

Preserve traceability from summary outputs back to source rows.

Do not modify process_pcapng.py unless the task explicitly includes capture conversion.

For now, tests should start from Sample/*.parquet and Data/grouped_data/*.parquet, not raw pcapng files.

Do not compare PNG bytes in regression tests.

Use golden fingerprints for parquet/csv outputs.