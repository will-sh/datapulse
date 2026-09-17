from __future__ import annotations

import json
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path

from app.kafka_client_properties import write_external_properties as write_kafka_external_properties
from app.spark_stream.store import STORE


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


DEFAULT_CONFIG_DIR = Path("config/kafka")


def _config_dir() -> Path:
    return Path(_env("KAFKA_CONFIG_DIR", str(DEFAULT_CONFIG_DIR))).expanduser()


def _spark_connect_host() -> str:
    return _env("SPARK_CONNECT_HOST") or _env("CDSW_IP_ADDRESS") or "127.0.0.1"


def _spark_connect_port() -> str:
    engine_id = _env("CDSW_ENGINE_ID").upper()
    if engine_id:
        port = os.getenv(f"DS_RUNTIME_{engine_id}_SERVICE_PORT_SPARK")
        if port:
            return port
    for key, port in os.environ.items():
        if key.startswith("DS_RUNTIME_") and key.endswith("_SERVICE_PORT_SPARK"):
            return port
    return _env("SPARK_CONNECT_PORT", "20049")


def _spark_connect_url() -> str | None:
    explicit = _env("SPARK_REMOTE") or _env("SPARK_CONNECT_URL")
    if explicit:
        return explicit
    if not _env("CDSW_ENGINE_ID") and not any(
        k.startswith("DS_RUNTIME_") and k.endswith("_SERVICE_PORT_SPARK") for k in os.environ
    ):
        return None
    return f"sc://{_spark_connect_host()}:{_spark_connect_port()}"


def _spark_env_summary() -> str:
    return f"connect={_spark_connect_url() or 'missing'}"


def _kafka_options() -> tuple[dict[str, str], str, str]:
    config_dir = _config_dir()
    token_url = _env(
        "KAFKA_TOKEN_URL",
        "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
    )
    kafka_ca = config_dir / "kafka-ca.crt"
    oauth_ca = config_dir / "oauth-ca.crt"
    allowed_urls = f"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={token_url}"
    dist_files = ",".join(str(path.resolve()) for path in (kafka_ca, oauth_ca) if path.is_file())

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


def _write_external_properties(config_dir: Path) -> Path:
    return write_kafka_external_properties(
        config_dir,
        bootstrap_servers=_env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
        ),
        token_url=_env(
            "KAFKA_TOKEN_URL",
            "https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token",
        ),
        client_id=_env("KAFKA_CLIENT_ID"),
        client_secret=_env("KAFKA_CLIENT_SECRET"),
        offset_reset=_env("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    )


def _kafka_cli_home() -> Path | None:
    explicit = _env("KAFKA_HOME")
    if explicit:
        path = Path(explicit)
        if (path / "bin" / "kafka-console-consumer.sh").is_file():
            return path
    cache = Path.home() / ".cache" / "kafka" / "kafka_2.13-3.9.0"
    if (cache / "bin" / "kafka-console-consumer.sh").is_file():
        return cache
    return None


def _build_spark_session():
    from pyspark.sql import SparkSession

    _, allowed_urls, dist_files = _kafka_options()
    if not _env("KAFKA_CLIENT_ID"):
        raise RuntimeError("KAFKA_CLIENT_ID is required")

    connect_url = _spark_connect_url()
    if not connect_url:
        raise RuntimeError("Spark Connect URL not found (missing CDSW_ENGINE_ID or runtime addon)")

    spark_version = _env("SPARK_VERSION", "3.5.4")
    scala_version = _env("SPARK_SCALA_VERSION", "2.12")
    kafka_package = _env(
        "SPARK_KAFKA_PACKAGE",
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    )

    builder = (
        SparkSession.builder.appName("datapulse-spark-kafka-consumer")
        .remote(connect_url)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.extraJavaOptions", allowed_urls)
        .config("spark.executor.extraJavaOptions", allowed_urls)
        .config("spark.jars.packages", kafka_package)
    )
    if dist_files:
        builder = builder.config("spark.files", dist_files)

    return builder.getOrCreate()


def _create_spark_session():
    timeout = int(_env("SPARK_SESSION_TIMEOUT_SEC", "60"))
    STORE.set_status(f"creating Spark session ({_spark_env_summary()})")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_build_spark_session)
        try:
            return future.result(timeout=timeout), _kafka_options()[0]
        except FuturesTimeoutError as exc:
            raise RuntimeError(
                f"Spark Connect session timed out after {timeout}s ({_spark_env_summary()})"
            ) from exc


def _spark_connect_probe_script() -> str:
    return """
import os
from pyspark.sql import SparkSession

host = os.environ.get("SPARK_CONNECT_HOST") or os.environ.get("CDSW_IP_ADDRESS", "127.0.0.1")
engine_id = os.environ.get("CDSW_ENGINE_ID", "").upper()
port = os.environ.get(f"DS_RUNTIME_{engine_id}_SERVICE_PORT_SPARK") if engine_id else None
if not port:
    for key, value in os.environ.items():
        if key.startswith("DS_RUNTIME_") and key.endswith("_SERVICE_PORT_SPARK"):
            port = value
            break
port = port or os.environ.get("SPARK_CONNECT_PORT", "20049")
url = os.environ.get("SPARK_CONNECT_URL") or f"sc://{host}:{port}"
spark = SparkSession.builder.appName("datapulse-connect-probe").remote(url).getOrCreate()
print(spark.version)
spark.stop()
"""


def _try_start_spark_connect_server() -> bool:
    """Best-effort: some CAI runtimes expose Spark Connect on a port but defer JVM startup."""
    if _env("SPARK_CONNECT_AUTOSTART", "true").lower() in {"0", "false", "no"}:
        return False

    candidates = [
        Path("/opt/spark-connect/bin/start-connect-server.sh"),
        Path("/opt/spark-connect/start-connect-server.sh"),
        Path("/opt/spark-connect/sbin/start-connect-server.sh"),
    ]
    for script in candidates:
        if not script.is_file():
            continue
        host = _spark_connect_host()
        port = _spark_connect_port()
        env = os.environ.copy()
        env.setdefault("SPARK_CONNECT_URL", f"sc://{host}:{port}")
        try:
            subprocess.run(
                [str(script), "--host", host, "--port", port],
                capture_output=True,
                text=True,
                timeout=30,
                env=env,
                check=False,
            )
            STORE.set_status(f"attempted Spark Connect start via {script.name}")
            return True
        except (OSError, subprocess.TimeoutExpired):
            continue
    return False


def _spark_connect_available() -> bool:
    connect_url = _spark_connect_url()
    if not connect_url:
        return False

    probe_timeout = int(_env("SPARK_CONNECT_PROBE_SEC", "20"))
    env = os.environ.copy()
    env.setdefault("SPARK_CONNECT_URL", connect_url)
    _try_start_spark_connect_server()
    try:
        result = subprocess.run(
            [sys.executable, "-c", _spark_connect_probe_script()],
            capture_output=True,
            text=True,
            timeout=probe_timeout,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        STORE.set_status(f"Spark Connect probe timed out after {probe_timeout}s")
        return False

    if result.returncode == 0:
        version = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else "unknown"
        STORE.set_status(f"Spark Connect probe ok ({version})")
        return True

    detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
    STORE.set_status(f"Spark Connect probe failed: {detail[:240]}")
    return False


def _process_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    for row in batch_df.collect():
        raw = row.asDict(recursive=True)
        message = raw.get("message") or raw.get("value") or ""
        timestamp = raw.get("event_timestamp") or raw.get("timestamp")
        text = str(message)
        kafka_ts = str(timestamp) if timestamp is not None else None
        STORE.ingest(text, kafka_ts)


def _start_spark_connect_streaming() -> None:
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
    STORE.set_status("streaming (spark connect)", active=True)
    query.awaitTermination()


def _write_cli_status(payload: dict[str, object]) -> None:
    path = _config_dir() / "consumer-cli-status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _consume_kafka_cli_line(line: str) -> None:
    line = line.strip()
    if not line:
        return
    STORE.ingest(line, None)


def _drain_kafka_cli_stderr(proc: subprocess.Popen[str]) -> None:
    if proc.stderr is None:
        return
    for line in proc.stderr:
        detail = line.strip()
        if not detail:
            continue
        if "ERROR" in detail or "Exception" in detail or "Failed" in detail:
            STORE.set_error(detail[:800])


def _start_kafka_cli_consumer() -> None:
    if not _env("KAFKA_CLIENT_ID"):
        raise RuntimeError("KAFKA_CLIENT_ID is required")

    config_dir = _config_dir()
    _write_external_properties(config_dir)
    kafka_home = _kafka_cli_home()
    if kafka_home is None:
        raise RuntimeError("Kafka CLI not found under ~/.cache/kafka (download via startup script)")

    consumer = kafka_home / "bin" / "kafka-console-consumer.sh"
    topic = _env("KAFKA_TOPIC", "datapulse-events")
    bootstrap = _env(
        "KAFKA_BOOTSTRAP_SERVERS",
        "csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443",
    )
    group = _env("KAFKA_CONSUMER_GROUP", "datapulse-spark-consumer")
    token_url = _env("KAFKA_TOKEN_URL")

    env = os.environ.copy()
    env["KAFKA_OPTS"] = f"-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls={token_url}"

    properties_name = (_config_dir() / "external.properties").name

    cmd = [
        str(consumer),
        "--bootstrap-server",
        bootstrap,
        "--consumer.config",
        properties_name,
        "--topic",
        topic,
        "--group",
        group,
    ]
    if _env("KAFKA_CLI_FROM_BEGINNING", "").lower() in {"1", "true", "yes"}:
        cmd.append("--from-beginning")

    STORE.set_status(f"kafka-cli streaming (group={group})", active=True)
    _write_cli_status({"ok": True, "phase": "starting", "group": group, "config_dir": str(config_dir)})
    while True:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(config_dir),
            env=env,
            text=True,
            bufsize=1,
        )
        stderr_thread = threading.Thread(
            target=_drain_kafka_cli_stderr,
            args=(proc,),
            name="kafka-cli-stderr",
            daemon=True,
        )
        stderr_thread.start()
        try:
            if proc.stdout is not None:
                for line in proc.stdout:
                    _consume_kafka_cli_line(line)
                    if STORE.total_received == 1:
                        _write_cli_status(
                            {
                                "ok": True,
                                "phase": "streaming",
                                "group": group,
                                "total_received": STORE.total_received,
                            }
                        )
        finally:
            proc.wait(timeout=30)
            if proc.returncode not in (0,):
                _write_cli_status(
                    {
                        "ok": False,
                        "returncode": proc.returncode,
                        "group": group,
                        "config_dir": str(config_dir),
                    }
                )
                STORE.set_status(
                    f"kafka-cli exited ({proc.returncode}); reconnecting (group={group})",
                    active=True,
                )
        time.sleep(int(_env("KAFKA_CLI_RECONNECT_SEC", "3")))


def start_streaming_worker() -> None:
    mode = _env("KAFKA_CONSUMER_MODE", "auto").lower()
    try:
        if mode in {"cli", "kafka-cli"}:
            _start_kafka_cli_consumer()
            return
        if mode in {"spark", "spark-connect"}:
            _start_spark_connect_streaming()
            return

        if _spark_connect_available():
            try:
                _start_spark_connect_streaming()
                return
            except Exception as spark_exc:
                STORE.set_status(f"Spark Connect stream failed ({spark_exc}); using Kafka CLI")

        _start_kafka_cli_consumer()
    except Exception as exc:  # noqa: BLE001 - surface in UI
        STORE.set_error(str(exc))


def launch_streaming_thread() -> threading.Thread:
    thread = threading.Thread(target=start_streaming_worker, name="spark-kafka-stream", daemon=True)
    thread.start()
    return thread
