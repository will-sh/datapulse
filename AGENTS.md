# DataPulse agent notes

- **Web UI**: FastAPI + Jinja2 (`templates/`, `static/`, `app/live/`). Do not add Next.js.
- **CAI deploy**: Python only (`scripts/cai_start_application.py`, `scripts/cai_spark_kafka_stream.py`).
- **Lakehouse ingest**: CSA Flink primary (`scripts/csa_flink_kafka_iceberg.py`); CAI Trino batch fallback (`scripts/ingest_kafka_iceberg.py`).
- **Sync to CAI**: `python3 scripts/cai_project_sync.py upload` from `main`; Kafka certs are gitignored and uploaded via `CAI_SYNC_EXTRA`.
