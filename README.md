# DataPulse

基于 PostHog 的用户行为分析 Demo 应用。在网站上进行点击、浏览、表单提交等操作，事件会实时显示在右下角的事件采集面板中，并可同步到 PostHog 云端；在 Cloudera AI 上还可将事件写入 Kafka，并由 Spark Consumer Application 实时展示。

## 最终架构

DataPulse 在 Cloudera 平台上的目标形态：**实时事件链路**（已落地）+ **湖仓沉淀与分析**（规划扩展）。Kafka 作为统一事件总线；流式入湖可在 **CDE(Spark)** 与 **CSA(Flink)** 两条路线中择一。

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

### 平台组件

| 层级 | 组件 | Blueprint / 服务 | 状态 |
|------|------|------------------|------|
| 应用托管 | CAI Workbench | `cai` | ✅ 已部署 |
| 事件生产 | datapulse-app (CAI Application) | CAI Application | ✅ 已验证 |
| 消息总线 | CSM(Kafka) | `csm` | ✅ 已验证 |
| 实时消费 | datapulse-spark-consumer (CAI Application) | CAI Application | ✅ 已验证 |
| 流式入湖 A | CDE(Spark) | `cde-udf` | 📋 规划 |
| 流式入湖 B | CSA(Flink) | `csa` | 📋 规划（与 CDE 二选一） |
| 湖仓存储 | Iceberg 表 | `cloudera-lakehouse-engine-governed` | 📋 规划 |
| SQL 分析 | Trino | Lakehouse Engine | 📋 规划 |
| 可视化 | CDV(Viz) | `cdv` | 📋 规划（可选） |
| 可观测性 | Observability | Prometheus + Grafana / Datadog 等 | 📋 规划（可选） |

### CAI Applications

| Application | 启动脚本 | 说明 |
|-------------|----------|------|
| **datapulse-app** | `scripts/cai_start_application.py` | 演示站点 + 事件生产 |
| **datapulse-spark-consumer** | `scripts/cai_spark_kafka_stream.py` | Kafka 消费 + 实时看板 |

Producer / Consumer 通过 Console Access Key 访问 CSM(Kafka)（OAuth）。**无需** `cai-base-connected` Blueprint，除非需对接 on-prem CDP Base 数据湖。

### 四条数据路径

| 路径 | 链路 | 用途 |
|------|------|------|
| **实时路径** | 用户 → datapulse-app → CSM(Kafka) → spark-consumer → 用户 | Demo 演示、秒级反馈 |
| **分析路径** | CSM(Kafka) → CDE(Spark) **或** CSA(Flink) → Lakehouse → Trino / CDV(Viz) | 历史查询、漏斗、留存、报表 |
| **产品分析路径** | 用户 / datapulse-app → PostHog 等 | 行为埋点、转化分析，与 CSM **并行** |
| **可观测性路径** | 各层组件 → **Observability** | 平台与应用健康监控，见下表 |

### Observability（可观测性）

**Observability** 模块与 Cloudera 业务主链路**并行**，汇聚多种观测手段，关注 **SLI / SLO**（延迟、错误率、吞吐、资源），**不承载业务事件分析**，数据**不进入** Lakehouse 业务表。

| 手段 | 代表 | 接入点 | 观测内容 |
|------|------|--------|----------|
| **Metrics（指标）** | Prometheus + Grafana | datapulse-app、spark-consumer、CDE Job、CSA Job、CSM(Kafka) Broker | QPS、P99 延迟、Consumer Lag、Job 背压、CPU / 内存 |
| **APM / Logs / Traces** | Datadog、Dynatrace、New Relic | 同上各 Application 与 Job 层 | 分布式 Trace、日志检索、告警、SLO 面板 |
| **Errors（错误追踪）** | Sentry | datapulse-app、spark-consumer | 前后端异常堆栈 |
| **Alerting（告警）** | PagerDuty、Slack | Grafana Alerting / Datadog 规则 | 运维通知，Observability 下游 |

**Prometheus + Grafana** 与 **Datadog** 在 Observability 内**并列**：前者适合平台内自建 Metrics 看板（如 Locust / Kafka Lag 大盘）；后者适合一体化 SaaS APM。可按环境二选一或组合使用（Metrics 走 Prometheus，Trace / Log 走 Datadog）。

### Product Analytics（产品分析 · 可选）

与 Observability 职责分离：关注**用户行为与业务 KPI**，而非服务健康。

| 类型 | 代表产品 | 接入点 | 数据内容 | 与主链路关系 |
|------|----------|--------|----------|--------------|
| **产品分析** | PostHog、Mixpanel、Amplitude | ① 用户浏览器（`analytics.js` SDK）<br/>② datapulse-app 服务端 | 点击、浏览、转化等行为事件 | 与 CSM(Kafka) **并行**；可只开 SaaS、只开 Kafka、或双写 |

**设计原则：**

- **Product Analytics（PostHog 等）** → 挂在 **用户 / Application 层**，管业务 KPI，可与 Kafka 双写。
- **Observability（Prometheus / Grafana / Datadog 等）** → 挂在 **Application、Job、CSM 各层**，管平台与应用健康。
- **Cloudera 主链路**（CSM → Lakehouse → Trino / CDV）→ 管 **平台内业务数据资产**；三者职责分离。

## 功能

- **页面浏览追踪** — 自动采集路由切换
- **自定义事件** — 按钮点击、功能使用、方案选择等
- **表单提交** — 模拟订阅表单转化事件
- **用户识别** — PostHog `identify` 演示
- **本地事件面板** — 未配置 PostHog 时也可完整演示

## 快速开始（FastAPI）

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8080
```

访问 [http://127.0.0.1:8080](http://127.0.0.1:8080)

## CAI Application 部署

在 Cloudera AI Workbench 中创建 Application 时，使用启动脚本：

```bash
scripts/cai-start-application.sh
```

脚本会读取 `CDSW_READONLY_PORT` 环境变量，并在 `127.0.0.1` 上启动 uvicorn。推荐使用 PBJ Workbench Python 3.11 runtime。

Application 的 `script` 字段应设为 `scripts/cai_start_application.py` 或 `scripts/start_datapulse.py`（PBJ Python runtime 会将脚本作为 Python 代码执行，不能使用 bash 脚本路径；脚本内请使用 `os.getcwd()` 而非 `__file__`，且不要用 `raise SystemExit` 包裹 uvicorn）。

## 配置 PostHog（可选）

1. 在 [PostHog](https://posthog.com) 注册并创建项目
2. 复制 Project API Key
3. 设置环境变量：

```env
POSTHOG_KEY=phc_your_project_api_key_here
POSTHOG_HOST=https://us.i.posthog.com
```

4. 重启应用

配置后，事件面板会显示「PostHog 已连接」，数据同步到 PostHog 后台的 Live Events。

## 配置 Kafka OAuth（可选）

1. 在 Surveyor 打开 Kafka **CLIENT_CONFIGS**，下载 `kafka-ca.crt` 与 `oauth-ca.crt` 到 `config/kafka/`
2. 在 Console API Explorer 创建 Access Key（`POST /api/v0/auth/access-keys/credentials`）
3. 设置环境变量：

```env
KAFKA_ENABLED=true
KAFKA_CLIENT_ID=your-client-id
KAFKA_CLIENT_SECRET=your-client-secret
KAFKA_TOPIC=datapulse-events
KAFKA_BOOTSTRAP_SERVERS=csm-bp-kafka.cldr-csk-csm-1.a70735.test.cldr.work:8443
KAFKA_TOKEN_URL=https://console.readygo.a70735.test.cldr.work/api/v0/auth/access-keys/token
KAFKA_CONFIG_DIR=config/kafka
```

4. 重启应用。浏览器事件会通过 `POST /api/events` 写入 Kafka topic。

CAI Application 启动脚本会在 `KAFKA_ENABLED=true` 时自动下载 Kafka CLI（`KAFKA_HOME`）。

### Spark Consumer Application

在 CAI 中创建第二个 Application，启动脚本设为 `scripts/cai_spark_kafka_stream.py`，并配置与 Producer 相同的 Kafka OAuth 环境变量。推荐 Runtime Addon：`sparkconnect354-731-26`（Spark Connect 不可用时自动 fallback 到 Kafka CLI）。

### Monitoring Application（Prometheus + Grafana）

第三个 CAI Application 用于 Observability，启动脚本：`scripts/cai_start_monitoring.py`。

| 项 | 值 |
|----|-----|
| Application 名 | `datapulse-monitoring` |
| Script | `scripts/cai_start_monitoring.py` |
| 对外 UI | Grafana（绑定 `CDSW_READONLY_PORT`） |

环境变量示例：

```env
PRODUCER_URL=https://datapulse-app.<your-domain>
CONSUMER_URL=https://datapulse-spark-consumer.<your-domain>
MONITORING_BEARER_TOKEN=<workbench-api-key>
MONITORING_VERIFY_SSL=false
```

Pod 内组件：

- `DataPulseExporter.py` — 聚合 Producer / Consumer `/health` 与 `/metrics`
- Prometheus — scrape `localhost:9191`
- Grafana — 预置 **DataPulse Overview** dashboard

Producer / Consumer 现已暴露 `GET /metrics`（Prometheus 格式）。本地调试：

```bash
bash monitoring/download.sh
PRODUCER_URL=http://127.0.0.1:8080 CONSUMER_URL=http://127.0.0.1:8081 bash monitoring/start.sh
```

## 项目结构

```
app/
  main.py                  # Producer FastAPI 路由
  config.py                # 环境变量配置
  kafka_settings.py        # Kafka OAuth 配置
  api/events.py            # POST /api/events
  services/kafka_producer.py
  spark_stream/            # Consumer Application
    kafka_stream.py        # Spark Connect 探针 + Kafka CLI 消费
    web.py                 # Consumer Web UI
    store.py               # 内存事件存储
config/kafka/              # Surveyor 证书与 client properties
templates/                 # Producer Jinja2 页面模板
static/
  css/styles.css
  js/analytics.js          # 事件采集与 PostHog 集成
scripts/
  cai_start_application.py       # Producer CAI 启动脚本
  cai_spark_kafka_stream.py      # Consumer CAI 启动脚本
  cai_start_monitoring.py        # Monitoring CAI 启动脚本
  cai-start-application.sh
requirements-spark.txt     # Consumer 依赖
monitoring/                # Prometheus + Grafana + DataPulse exporter
  DataPulseExporter.py
  prometheus.yml
  start.sh
  download.sh
src/                       # 原 Next.js 实现（保留参考）
```

## 技术栈

- FastAPI + Jinja2（Producer 站点 + Consumer 看板）
- Kafka OAuth（CSM / Strimzi）
- Spark Connect + Kafka CLI fallback（Consumer）
- Cloudera Lakehouse + Iceberg + Trino + CDV(Viz)（目标分析路径，规划）
- Observability：Prometheus + Grafana / Datadog（可选，与主链路并行）
- PostHog (`posthog-js` CDN，可选，Product Analytics)
- 原 Next.js 16 实现保留在 `src/` 目录供参考
