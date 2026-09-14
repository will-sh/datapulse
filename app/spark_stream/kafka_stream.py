from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path

from app.spark_stream.store import STORE


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _config_dir() -> Path:
    return Path(_env("KAFKA_CONFIG_DIR", ".")).expanduser()


def _spark_env_summary() -> str:
    keys = sorted(k for k in os.environ if "SPARK" in k or k.startswith("CDSW_"))
    return ", ".join(keys[:12]) or "no SPARK/CDSW env detected"


def _kafka_options() -> tuple[dict[str, str], str, str]:
    config_dir = _config_dir()
    token_url = _env(
        "KAFKA_TOKEN_URL",
        "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
    )
    kafka_ca = config_dir / "kafka-ca.crt"
    oauth_ca = config_dir / "oauth-ca.crt"
    allowed_urls = f"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={token_url}"
    dist_files = ",".join(str(path) for path in (kafka_ca, oauth_ca) if path.is_file())

    options = {
        "kafka.bootstrap.servers": _env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        ),
        "subscribe": _env("KAFKA_TOPIC", "datapulse-events"),
        "startingOffsets": _env("KAFKA_STARTING_OFFSETS", "earliest"),
        "failOnDataLoss": "false",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.login.callback.handler.class": (
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler"
        ),
        "kafka.sasl.oauthbearer.token.endpoint.url": token_url,
        "kafka.sasl.oauthbearer.client.id": _env("KAFKA_CLIENT_ID"),
        "kafka.sasl.oauthbearer.client.secret": _env("KAFKA_CLIENT_SECRET"),
        "kafka.ssl.truststore.location": str(kafka_ca.resolve()),
        "kafka.ssl.truststore.type": "PEM",
    }

    jaas = (
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
        f'clientId="{_env("KAFKA_CLIENT_ID")}" '
        f'clientSecret="{_env("KAFKA_CLIENT_SECRET")}" '
        f'ssl.truststore.location="{oauth_ca.resolve()}" ssl.truststore.type=PEM;'
    )
    options["kafka.sasl.jaas.config"] = jaas
    return options, allowed_urls, dist_files


def _build_spark_session():
    from pyspark.sql import SparkSession

    _, allowed_urls, dist_files = _kafka_options()
    if not _env("KAFKA_CLIENT_ID"):
        raise RuntimeError("KAFKA_CLIENT_ID is required")

    spark_version = _env("SPARK_VERSION", "3.5.4")
    scala_version = _env("SPARK_SCALA_VERSION", "2.12")
    kafka_package = _env(
        "SPARK_KAFKA_PACKAGE",
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    )

    builder = (
        SparkSession.builder.appName("datapulse-spark-kafka-consumer")
        .master(_env("SPARK_MASTER", "local[1]"))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", allowed_urls)
        .config("spark.executor.extraJavaOptions", allowed_urls)
        .config("spark.jars.packages", kafka_package)
    )
    if dist_files:
        builder = builder.config("spark.files", dist_files)

    remote = _env("SPARK_REMOTE") or _env("SPARK_CONNECT_URL")
    if remote:
        builder = SparkSession.builder.appName("datapulse-spark-kafka-consumer").remote(remote)

    return builder.getOrCreate()


def _create_spark_session():
    timeout = int(_env("SPARK_SESSION_TIMEOUT_SEC", "120"))
    STORE.set_status(f"creating Spark session ({_spark_env_summary()})")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_build_spark_session)
        try:
            return future.result(timeout=timeout), _kafka_options()[0]
        except FuturesTimeoutError as exc:
            raise RuntimeError(f"Spark session creation timed out after {timeout}s") from exc


def _process_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    for row in batch_df.collect():
        raw = row.asDict(recursive=True)
        message = raw.get("message") or raw.get("value") or ""
        timestamp = raw.get("event_timestamp") or raw.get("timestamp")
        STORE.add(str(message), str(timestamp) if timestamp is not None else None)


def start_streaming_worker() -> None:
    try:
        spark, kafka_options = _create_spark_session()
        STORE.set_status(f"Spark {spark.version} ready", active=False)

        source = spark.readStream.format("kafka").options(**kafka_options).load()
        events = source.selectExpr(
            "CAST(value AS STRING) AS message",
            "CAST(timestamp AS STRING) AS event_timestamp",
        )

        checkpoint = str(_config_dir() / ".spark-checkpoints" / "datapulse-kafka")
        Path(checkpoint).mkdir(parents=True, exist_ok=True)
        query = (
            events.writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint)
            .foreachBatch(_process_batch)
            .trigger(processingTime=_env("KAFKA_POLL_INTERVAL", "5 seconds"))
            .start()
        )
        STORE.set_status("streaming", active=True)
        query.awaitTermination()
    except Exception as exc:  # noqa: BLE001 - surface in UI
        STORE.set_error(str(exc))


def launch_streaming_thread() -> threading.Thread:
    thread = threading.Thread(target=start_streaming_worker, name="spark-kafka-stream", daemon=True)
    thread.start()
    return thread
