# DataPulse

A PostHog-powered user behavior analytics demo. Clicks, page views, form submissions, and other actions on the site appear in the bottom-right event capture panel in real time, can sync to PostHog cloud, and on Cloudera AI can also be written to Kafka and displayed live by the Spark Consumer Application.

## Target architecture

DataPulse on Cloudera: **real-time event pipeline** (implemented) + **lakehouse persistence and analytics** (planned extension). Kafka is the unified event bus; stream-to-lake can follow either **CDE (Spark)** or **CSA (Flink)**.

```mermaid
flowchart LR
    subgraph UserLayer["User"]
        User["User"]
    end

    subgraph CAI["Cloudera AI · CAI Workbench"]
        Producer["datapulse-app<br/>(CAI Application)"]
        Consumer["datapulse-spark-consumer<br/>(CAI Application)"]
    end

    subgraph CSM["Streams Messaging"]
        Kafka[("CSM(Kafka)<br/>datapulse-events")]
    end

    subgraph Ingest["Stream Ingest · Route Choice · Planned"]
        CDE["CDE(Spark)<br/>Structured Streaming"]
        CSA["CSA(Flink)<br/>Flink SQL"]
    end

    subgraph Lakehouse["Cloudera Lakehouse · Planned"]
        Table[("Iceberg Table<br/>datapulse.events")]
        Trino["Trino<br/>SQL Analytics"]
        CDV["CDV(Viz)<br/>Funnels · Trends"]
    end

    subgraph ProductAnalytics["Product Analytics · Optional · Parallel"]
        PostHog["PostHog<br/>Product Analytics"]
    end

    subgraph Observability["Observability · Optional · Parallel"]
        Prometheus["Prometheus<br/>Metrics Collection / Storage"]
        Grafana["Grafana<br/>Dashboard"]
        Datadog["Datadog<br/>APM · Logs · Traces"]
    end

    User -->|"Behavioral Events"| Producer
    Producer -->|"POST /api/events"| Kafka
    Kafka -->|"Real-time Subscribe"| Consumer
    Consumer -->|"Real-time Display"| User

    Kafka -->|"Route A"| CDE
    Kafka -->|"Route B"| CSA
    CDE --> Table
    CSA --> Table
    Table --> Trino
    Table --> CDV

    User -.->|"Browser SDK"| PostHog
    Producer -.->|"Server-side Events"| PostHog

    Producer -.->|"/metrics · Trace"| Prometheus
    Consumer -.->|"/metrics · Trace"| Prometheus
    CDE -.->|"Job Metrics"| Prometheus
    CSA -.->|"Job Metrics"| Prometheus
    Kafka -.->|"Broker Metrics"| Prometheus
    Prometheus --> Grafana

    Producer -.->|"APM · Logs · Trace"| Datadog
    Consumer -.->|"APM · Logs · Trace"| Datadog
    CDE -.->|"Job Monitoring"| Datadog
    CSA -.->|"Job Monitoring"| Datadog
```

### Platform components

| Layer | Component | Blueprint / service | Status |
|------|------|------------------|------|
| App hosting | CAI Workbench | `cai` | ✅ Deployed |
| Event production | datapulse-app (CAI Application) | CAI Application | ✅ Verified |
| Message bus | CSM(Kafka) | `csm` | ✅ Verified |
| Real-time consumption | datapulse-spark-consumer (CAI Application) | CAI Application | ✅ Verified |
| Stream ingest A | CDE(Spark) | `cde-udf` | 📋 Planned |
| Stream ingest B | CSA(Flink) | `csa` | 📋 Planned (choose one of CDE or CSA) |
| Lakehouse storage | Iceberg tables | `cloudera-lakehouse-engine-governed` | 📋 Planned |
| SQL analytics | Trino | Lakehouse Engine | 📋 Planned |
| Visualization | CDV(Viz) | `cdv` | 📋 Planned (optional) |
| Observability | Observability | Prometheus + Grafana / Datadog, etc. | 📋 Planned (optional) |

### CAI Applications

| Application | Start script | Description |
|-------------|----------|------|
| **datapulse-app** | `scripts/cai_start_application.py` | Demo site + event production |
| **datapulse-spark-consumer** | `scripts/cai_spark_kafka_stream.py` | Kafka consumption + real-time dashboard |

Producer / Consumer access CSM(Kafka) via Console Access Key (OAuth). **`cai-base-connected` Blueprint is not required** unless you need on-prem CDP Base data lake integration.

### Four data paths

| Path | Pipeline | Purpose |
|------|------|------|
| **Real-time** | User → datapulse-app → CSM(Kafka) → spark-consumer → User | Demo, sub-second feedback |
| **Analytics** | CSM(Kafka) → CDE(Spark) **or** CSA(Flink) → Lakehouse → Trino / CDV(Viz) | Historical queries, funnels, retention, reports |
| **Product analytics** | User / datapulse-app → PostHog, etc. | Behavioral tracking, conversion analysis — **parallel** to CSM |
| **Observability** | Components at each layer → **Observability** | Platform and app health monitoring (see table below) |

### Observability

The **Observability** module runs **in parallel** with the Cloudera business pipeline, aggregating multiple signals focused on **SLI / SLO** (latency, error rate, throughput, resources). It does **not** carry business event analytics; data does **not** land in Lakehouse business tables.

| Signal | Examples | Integration points | What to observe |
|------|------|--------|----------|
| **Metrics** | Prometheus + Grafana | datapulse-app, spark-consumer, CDE Job, CSA Job, CSM(Kafka) Broker | QPS, P99 latency, consumer lag, job backpressure, CPU / memory |
| **APM / Logs / Traces** | Datadog, Dynatrace, New Relic | Same applications and job layers | Distributed traces, log search, alerts, SLO dashboards |
| **Errors** | Sentry | datapulse-app, spark-consumer | Frontend/backend exception stacks |
| **Alerting** | PagerDuty, Slack | Grafana Alerting / Datadog rules | Ops notifications downstream of Observability |

**Prometheus + Grafana** and **Datadog** are **peers** within Observability: the former suits self-hosted metrics dashboards (e.g. Locust / Kafka lag); the latter suits all-in-one SaaS APM. Use one or combine (metrics via Prometheus, traces/logs via Datadog).

### Product Analytics (optional)

Separated from Observability: focuses on **user behavior and business KPIs**, not service health.

| Type | Products | Integration | Data | Relation to main pipeline |
|------|----------|--------|----------|--------------|
| **Product analytics** | PostHog, Mixpanel, Amplitude | ① User browser (`analytics.js` SDK)<br/>② datapulse-app server | Clicks, views, conversions, etc. | **Parallel** to CSM(Kafka); SaaS only, Kafka only, or dual-write |

**Design principles:**

- **Product Analytics (PostHog, etc.)** → **User / Application layer**, business KPIs, optional dual-write with Kafka.
- **Observability (Prometheus / Grafana / Datadog, etc.)** → **Application, Job, CSM layers**, platform and app health.
- **Cloudera main pipeline** (CSM → Lakehouse → Trino / CDV) → **In-platform business data assets**; all three roles stay separate.

## Features

- **Page view tracking** — automatic capture on route changes
- **Custom events** — button clicks, feature usage, plan selection, etc.
- **Form submission** — simulated subscription conversion events
- **User identify** — PostHog `identify` demo
- **Local event panel** — full demo without PostHog configured

## Quick start (FastAPI)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8080
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080)

## CAI Application deployment

When creating an Application in Cloudera AI Workbench, use the start script:

```bash
scripts/cai-start-application.sh
```

The script reads `CDSW_READONLY_PORT` and starts uvicorn on `127.0.0.1`. PBJ Workbench Python 3.11 runtime is recommended.

Set the Application `script` field to `scripts/cai_start_application.py` or `scripts/start_datapulse.py` (PBJ Python runtime executes the script as Python — not a bash path; use `os.getcwd()` instead of `__file__` in scripts, and do not wrap uvicorn in `raise SystemExit`).

## Day-to-day CAI deployment

After code changes: **upload → delete + recreate**. Do not use `:restart` (it can hang in `APPLICATION_STARTING` and leave orphan engine sessions).

```bash
export CAI_BASE=https://<your-cai-domain>
export CAI_PID=<project-id>
export CAI_KEY=$CDSW_APIV2_KEY

# One-shot: upload files + recreate all Applications
python3 scripts/cai_deploy.py

# Or step by step
python3 scripts/cai_upload_files.py
python3 scripts/cai_recreate_applications.py

# Recreate a single app (e.g. producer)
python3 scripts/cai_recreate_applications.py --apps datapulse-app
```

`cai_recreate_applications.py` finds existing Applications by name, snapshots runtime / env / subdomain config, deletes the old instance, then creates anew (`environment` submitted as an object). If the monitoring subdomain changes, update `GRAFANA_ROOT_URL` / `GRAFANA_DOMAIN` in `datapulse.env`.

## PostHog configuration (optional)

1. Sign up at [PostHog](https://posthog.com) and create a project
2. Copy the Project API Key
3. Set environment variables:

```env
POSTHOG_KEY=phc_your_project_api_key_here
POSTHOG_HOST=https://us.i.posthog.com
```

4. Run `python3 scripts/cai_recreate_applications.py --apps datapulse-app` to apply

The event panel will show “PostHog connected” and data syncs to PostHog Live Events.

## Kafka OAuth configuration (optional)

1. In Surveyor, open Kafka **CLIENT_CONFIGS**, download `kafka-ca.crt` and `oauth-ca.crt` into `config/kafka/`
2. Create an Access Key in Console API Explorer (`POST /api/v0/auth/access-keys/credentials`)
3. Set environment variables:

```env
KAFKA_ENABLED=true
KAFKA_CLIENT_ID=your-client-id
KAFKA_CLIENT_SECRET=your-client-secret
KAFKA_TOPIC=datapulse-events
KAFKA_BOOTSTRAP_SERVERS=csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443
KAFKA_TOKEN_URL=https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token
KAFKA_CONFIG_DIR=config/kafka
```

4. Run `python3 scripts/cai_recreate_applications.py --apps datapulse-app` to apply. Browser events are written to the Kafka topic via `POST /api/events` or `POST /v1/capture`.

The CAI Application start script auto-downloads the Kafka CLI when `KAFKA_ENABLED=true` (`KAFKA_HOME`).

### Spark Consumer Application

Create a second CAI Application with start script `scripts/cai_spark_kafka_stream.py` and the same Kafka OAuth env vars as the Producer. Recommended runtime addon: `sparkconnect354-731-26` (falls back to Kafka CLI when Spark Connect is unavailable).

### Lakehouse: Kafka → Iceberg (standalone CAI Job)

**Do not** put lake ingest logic in the `datapulse-spark-consumer` Application. The consumer only powers the in-memory real-time dashboard; historical analytics go through Lakehouse.

| Component | Role |
|------|------|
| CSM(Kafka) `datapulse-events` | Event bus (Producer already writes here) |
| CAI Job `datapulse-lakehouse-kafka-ingest` | Spark Structured Streaming + `foreachBatch` → Iceberg |
| Lakehouse HMS + Ozone | `iceberg_catalog.datapulse.events` metadata and storage |
| Trino (`lakehouse-bp-trino`) | SQL validation and later funnel/retention analysis |

Recommended path (matches architecture diagram above):

```
Kafka(datapulse-events) → Spark Job(foreachBatch) → Iceberg(datapulse.events) → Trino SQL
```

**Isolated testing** (does not affect existing Applications):

```bash
export CAI_BASE=https://ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work
export CAI_PID=k87h-zej9-dugs-473y
export CAI_KEY=$CDSW_APIV2_KEY

# Upload new scripts
python3 scripts/cai_upload_files.py

# 1) discover — print HMS / warehouse / Kafka config
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode discover

# 2) bootstrap — create Iceberg table (requires sparkconnect addon)
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode bootstrap

# 3) batch — run one micro-batch write
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode batch

# 4) verify — row count and sample rows
python3 scripts/cai_submit_lakehouse_job.py ensure-and-run --mode verify
```

Job entry: `scripts/cai_lakehouse_discover_only.py` → `jobs/kafka_to_iceberg.py`  
Note: do not use `raise SystemExit()` in CAI PBJ Job scripts — the UI will show `ENGINE_FAILED`.

Runtime addons: `discover` only needs `hadoop-cli-7.3.1.709-1`; `bootstrap/batch/stream/verify` also need `sparkconnect354-731-26`

Common Lakehouse env vars (override with `--env KEY=VALUE`):

```env
HIVE_METASTORE_URI=thrift://hivemetastore.cldr-csk-lakehouse.a70735.test.cldr.work:9083
ICEBERG_WAREHOUSE=s3a://s3v/warehouse/datapulse
ICEBERG_CATALOG=iceberg_catalog
ICEBERG_DATABASE=datapulse
ICEBERG_TABLE=events
```

After a successful write, query in Trino Admin UI:

```sql
SELECT event_name, user_id, anonymous_id, event_timestamp
FROM iceberg_catalog.datapulse.events
ORDER BY ingested_at DESC
LIMIT 20;
```

> Note: Iceberg **Structured Streaming sink** is still Technical Preview on some CDE versions; this Job uses **`foreachBatch` + `writeTo(...).append()`**, matching the existing Consumer Spark path for easier step-by-step validation in CAI.

### Monitoring Application (Prometheus + Grafana)

A third CAI Application handles Observability; start script: `scripts/cai_start_monitoring.py`.

**Keep CAI Project in sync with `main`**: see [docs/cai-main-sync.md](docs/cai-main-sync.md). On `main`, run `python3 scripts/cai_project_sync.py upload` (do not upload directly from feature branches).

| Item | Value |
|----|-----|
| Application name | `datapulse-monitoring` |
| Script | `scripts/cai_start_monitoring.py` |
| Public UI | Grafana (subdomain assigned by CAI, e.g. `datapulse-mon-xxxxxx.<domain>`) |

Environment variables (recommended in **Project → Settings → Engine** so all Applications load them at start; or place `datapulse.env` at project root as fallback):

```env
CDSW_APP_POLLING_ENDPOINT=/
PRODUCER_URL=https://datapulse-app.<your-domain>
CONSUMER_URL=https://datapulse-spark-consumer.<your-domain>
GRAFANA_ROOT_URL=https://datapulse-mon-xxxxxx.<your-domain>/
GRAFANA_DOMAIN=datapulse-mon-xxxxxx.<your-domain>
MONITORING_VERIFY_SSL=false
WAIT_TIMEOUT=300
```

`CDSW_APP_POLLING_ENDPOINT=/` is required for CAI to mark the Application RUNNING (see WebSessions Locust Dashboard). Grafana binds directly to `CDSW_READONLY_PORT` — no extra HTTP proxy.

The monitoring Application only needs `MONITORING_VERIFY_SSL=false` locally (other vars inherit from Project). The exporter scrapes Producer / Consumer via in-pod `DS_RUNTIME_*_PORT_8100_TCP_ADDR` (Applications must bind `0.0.0.0:CDSW_READONLY_PORT`).

**Access URL:** In Workbench **Applications**, click **Open** on `datapulse-monitoring` (subdomain like `datapulse-mon-xxxxxx.<domain>`, assigned at create time). If `nslookup` returns NXDOMAIN, internal DNS may not be synced yet — delete and recreate the Application to trigger registration; confirm `datapulse-app.<domain>` resolves to rule out network issues.

In-pod components:

- `DataPulseExporter.py` — aggregates Producer / Consumer `/health` and `/metrics` (relay to Prometheus)
- Prometheus — scrapes `localhost:9191`
- Grafana — prebuilt **DataPulse Overview** dashboard

Producer / Consumer expose `GET /metrics` (Prometheus format). Local debug:

```bash
bash monitoring/download.sh
PRODUCER_URL=http://127.0.0.1:8080 CONSUMER_URL=http://127.0.0.1:8081 bash monitoring/start.sh
```

## Project structure

```
app/
  main.py                  # Producer FastAPI routes
  config.py                # Environment configuration
  kafka_settings.py        # Kafka OAuth configuration
  api/events.py            # POST /api/events
  services/kafka_producer.py
  spark_stream/            # Consumer Application
    kafka_stream.py        # Spark Connect probe + Kafka CLI consumption
    web.py                 # Consumer Web UI
    store.py               # In-memory event store
config/kafka/              # Surveyor certs and client properties
templates/                 # Producer Jinja2 page templates
static/
  css/styles.css
  js/analytics.js          # Event capture and PostHog integration
scripts/
  cai_deploy.py                 # upload + recreate one-shot deploy
  cai_upload_files.py             # Upload project files to Workbench
  cai_recreate_applications.py    # delete + create Application rebuild
  cai_stop_applications.py        # stop only (debug; use recreate for daily deploy)
  cai_start_application.py        # Producer CAI start script
  cai_spark_kafka_stream.py       # Consumer CAI start script
  cai_lakehouse_ingest_job.py     # Lakehouse ingest CAI Job entry (standalone)
  cai_submit_lakehouse_job.py     # Create/run Lakehouse Job (does not touch Applications)
  cai_start_monitoring.py         # Monitoring CAI start script
jobs/
  kafka_to_iceberg.py            # Spark Kafka -> Iceberg logic
requirements-spark.txt     # Consumer dependencies
monitoring/                # Prometheus + Grafana + DataPulse exporter
  DataPulseExporter.py
  prometheus.yml
  start.sh
  download.sh
src/                       # Legacy Next.js implementation (reference)
```

## Tech stack

- FastAPI + Jinja2 (Producer site + Consumer dashboards)
- Kafka OAuth (CSM / Strimzi)
- Spark Connect + Kafka CLI fallback (Consumer)
- Cloudera Lakehouse + Iceberg + Trino + CDV(Viz) (target analytics path, planned)
- Observability: Prometheus + Grafana / Datadog (optional, parallel to main pipeline)
- PostHog (`posthog-js` CDN, optional, Product Analytics)
- Legacy Next.js 16 implementation kept in `src/` for reference
