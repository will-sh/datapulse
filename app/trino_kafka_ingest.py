"""Poll Kafka via CLI and ingest micro-batches into Iceberg through Trino INSERT ... SELECT."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.kafka_cli_tools import (
    kafka_config_dir,
    list_topic_end_offsets,
    list_topic_partitions,
    prepare_kafka_properties,
    read_partition_messages,
)
from app.trino_lakehouse import TrinoOAuthClient, bootstrap_via_trino, get_trino_settings


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _checkpoint_path() -> Path:
    raw = _env(
        "TRINO_INGEST_CHECKPOINT",
        "config/lakehouse/.checkpoints/trino-ingest-offset.json",
    )
    return Path(raw).expanduser()


@dataclass
class IngestRow:
    event_id: str | None
    project_id: str | None
    event_name: str | None
    properties_json: str | None
    event_timestamp: int | None
    source: str | None
    user_id: str | None
    anonymous_id: str | None
    session_id: str | None
    page_path: str | None
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp: datetime | None
    ingested_at: datetime
    raw_payload: str


def load_checkpoint(topic: str) -> dict[str, Any]:
    path = _checkpoint_path()
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("topic") == topic:
            return payload

    return {
        "topic": topic,
        "partitions": {},
        "updated_at": None,
    }


def save_checkpoint(payload: dict[str, Any]) -> None:
    path = _checkpoint_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = datetime.now(UTC).isoformat()
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _partition_next_offset(checkpoint: dict[str, Any], partition: int) -> int:
    partitions = checkpoint.setdefault("partitions", {})
    entry = partitions.get(str(partition), {})
    if "next_offset" in entry:
        return int(entry["next_offset"])
    explicit = _env("TRINO_INGEST_START_OFFSET")
    if explicit.isdigit():
        return int(explicit)
    return 0


def _update_partition_offset(checkpoint: dict[str, Any], partition: int, next_offset: int) -> None:
    checkpoint.setdefault("partitions", {})[str(partition)] = {
        "next_offset": next_offset,
        "updated_at": datetime.now(UTC).isoformat(),
    }


def parse_kafka_message(raw: str, *, partition: int, offset: int) -> IngestRow:
    payload = json.loads(raw)
    properties = payload.get("properties")
    if isinstance(properties, dict):
        properties_json = json.dumps(properties, ensure_ascii=False)
    else:
        properties_json = None

    ts_raw = payload.get("timestamp")
    event_timestamp = int(ts_raw) if ts_raw is not None else None
    ingested_at = datetime.now(UTC)

    return IngestRow(
        event_id=str(payload["event_id"]) if payload.get("event_id") else None,
        project_id=str(payload["project_id"]) if payload.get("project_id") else None,
        event_name=str(payload.get("name") or payload.get("event_name") or "unknown"),
        properties_json=properties_json,
        event_timestamp=event_timestamp,
        source=str(payload["source"]) if payload.get("source") else None,
        user_id=str(payload["user_id"]) if payload.get("user_id") else None,
        anonymous_id=str(payload["anonymous_id"]) if payload.get("anonymous_id") else None,
        session_id=str(payload["session_id"]) if payload.get("session_id") else None,
        page_path=str(payload["page_path"]) if payload.get("page_path") else None,
        kafka_partition=partition,
        kafka_offset=offset,
        kafka_timestamp=None,
        ingested_at=ingested_at,
        raw_payload=raw,
    )


def _sql_str(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _sql_bigint(value: int | None) -> str:
    return "NULL" if value is None else str(int(value))


def _sql_timestamp(value: datetime | None) -> str:
    if value is None:
        return "NULL"
    text = value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"TIMESTAMP '{text[:-3]}'"


def build_insert_sql(settings, rows: list[IngestRow]) -> str:
    if not rows:
        raise ValueError("build_insert_sql requires at least one row")

    value_rows: list[str] = []
    for row in rows:
        value_rows.append(
            "("
            + ", ".join(
                [
                    _sql_str(row.event_id),
                    _sql_str(row.project_id),
                    _sql_str(row.event_name),
                    _sql_str(row.properties_json),
                    _sql_bigint(row.event_timestamp),
                    _sql_str(row.source),
                    _sql_str(row.user_id),
                    _sql_str(row.anonymous_id),
                    _sql_str(row.session_id),
                    _sql_str(row.page_path),
                    str(row.kafka_partition),
                    str(row.kafka_offset),
                    _sql_timestamp(row.kafka_timestamp),
                    _sql_timestamp(row.ingested_at),
                    _sql_str(row.raw_payload),
                ]
            )
            + ")"
        )

    values_sql = ",\n      ".join(value_rows)
    return f"""
INSERT INTO {settings.qualified_table}
SELECT
  event_id,
  project_id,
  event_name,
  properties_json,
  event_timestamp,
  source,
  user_id,
  anonymous_id,
  session_id,
  page_path,
  kafka_partition,
  kafka_offset,
  kafka_timestamp,
  ingested_at,
  raw_payload
FROM (
  VALUES
      {values_sql}
) AS incoming(
  event_id,
  project_id,
  event_name,
  properties_json,
  event_timestamp,
  source,
  user_id,
  anonymous_id,
  session_id,
  page_path,
  kafka_partition,
  kafka_offset,
  kafka_timestamp,
  ingested_at,
  raw_payload
)
""".strip()


def initialize_checkpoint_offsets(checkpoint: dict[str, Any], topic: str) -> dict[str, Any]:
    if checkpoint.get("partitions"):
        return checkpoint

    start_mode = _env("TRINO_INGEST_START_MODE", "earliest").lower()
    partitions = list_topic_partitions(topic)
    if start_mode == "latest":
        end_offsets = list_topic_end_offsets(topic)
        for partition in partitions:
            _update_partition_offset(checkpoint, partition, end_offsets.get(partition, 0))
        return checkpoint

    for partition in partitions:
        if _env("TRINO_INGEST_START_OFFSET").isdigit():
            _update_partition_offset(checkpoint, partition, int(_env("TRINO_INGEST_START_OFFSET")))
        else:
            _update_partition_offset(checkpoint, partition, 0)
    return checkpoint


def poll_kafka_batch(
    *,
    topic: str,
    checkpoint: dict[str, Any],
    max_messages_per_partition: int,
    timeout_ms: int,
) -> tuple[list[IngestRow], dict[str, Any]]:
    rows: list[IngestRow] = []
    partitions = list_topic_partitions(topic)
    for partition in partitions:
        start_offset = _partition_next_offset(checkpoint, partition)
        messages = read_partition_messages(
            topic=topic,
            partition=partition,
            start_offset=start_offset,
            max_messages=max_messages_per_partition,
            timeout_ms=timeout_ms,
        )
        if not messages:
            continue
        for offset, raw in messages:
            rows.append(parse_kafka_message(raw, partition=partition, offset=offset))
        _update_partition_offset(checkpoint, partition, messages[-1][0] + 1)
    return rows, checkpoint


def ingest_batch_via_trino(
    client: TrinoOAuthClient,
    settings,
    rows: list[IngestRow],
) -> dict[str, Any]:
    sql = build_insert_sql(settings, rows)
    result = client.execute(sql, catalog=settings.catalog, schema=settings.database)
    return {"inserted_rows": len(rows), "result": result}


def run_trino_ingest(*, ensure_table: bool = True) -> dict[str, Any]:
    """Poll Kafka and write micro-batches to Iceberg using Trino INSERT ... SELECT."""
    settings = get_trino_settings()
    topic = _env("KAFKA_TOPIC", "datapulse-events")
    max_messages = int(_env("TRINO_INGEST_BATCH_SIZE", "50"))
    max_batches = int(_env("TRINO_INGEST_MAX_BATCHES", "10"))
    poll_interval_sec = float(_env("TRINO_INGEST_POLL_INTERVAL_SEC", "2"))
    timeout_ms = int(_env("TRINO_INGEST_TIMEOUT_MS", "10000"))
    stop_on_empty = _env("TRINO_INGEST_STOP_ON_EMPTY", "true").lower() in {"1", "true", "yes"}

    kafka_config_dir().mkdir(parents=True, exist_ok=True)
    prepare_kafka_properties()

    client = TrinoOAuthClient(settings)
    client.info()

    bootstrap_result: dict[str, Any] | None = None
    if ensure_table and _env("TRINO_INGEST_ENSURE_TABLE", "true").lower() in {"1", "true", "yes"}:
        bootstrap_result = bootstrap_via_trino()

    checkpoint = load_checkpoint(topic)
    checkpoint = initialize_checkpoint_offsets(checkpoint, topic)
    batches: list[dict[str, Any]] = []
    total_inserted = 0

    for batch_index in range(max_batches):
        rows, checkpoint = poll_kafka_batch(
            topic=topic,
            checkpoint=checkpoint,
            max_messages_per_partition=max_messages,
            timeout_ms=timeout_ms,
        )
        if not rows:
            batches.append({"batch": batch_index + 1, "inserted_rows": 0, "status": "empty"})
            if stop_on_empty:
                break
            time.sleep(poll_interval_sec)
            continue

        insert_result = ingest_batch_via_trino(client, settings, rows)
        save_checkpoint(checkpoint)
        total_inserted += len(rows)
        batches.append(
            {
                "batch": batch_index + 1,
                "inserted_rows": len(rows),
                "status": "inserted",
                "trino": {"columns": insert_result["result"].get("columns")},
            }
        )
        if batch_index + 1 < max_batches:
            time.sleep(poll_interval_sec)

    if total_inserted:
        save_checkpoint(checkpoint)

    return {
        "mode": "trino-ingest",
        "qualified_table": settings.qualified_table,
        "topic": topic,
        "checkpoint_path": str(_checkpoint_path()),
        "checkpoint": checkpoint,
        "total_inserted": total_inserted,
        "batches": batches,
        "bootstrap": bootstrap_result,
        "note": (
            "Lakehouse Trino has no kafka catalog; Kafka is read via CLI and inserted with "
            "INSERT INTO ... SELECT * FROM (VALUES ...)."
        ),
    }
