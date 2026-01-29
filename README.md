# Gaming Event Processing System

Enterprise-grade real-time data pipeline for gaming events using Apache Kafka and Apache Spark. Implements the Medallion Architecture with Delta Lake for ACID transactions, featuring event ingestion, YAML-driven data quality transformations, and multi-level aggregations for analytics and reporting.

## 🎯 Overview

This system processes real-time events from an 8Ball Pool game server through a complete data lakehouse architecture:

- **Event Streaming**: Kafka-based real-time event ingestion with configurable rates
- **Medallion Architecture**: Bronze → Silver → Gold layers using Delta Lake
- **Data Quality Framework**: Extensible, YAML-driven transformation engine
- **Batch Processing**: Daily aggregations for historical reporting
- **Real-time Analytics**: Minute-level streaming aggregations for live dashboards
- **Schema Validation**: JSON schema enforcement for all event types
- **Design Patterns**: Template Method, Strategy, Factory, Builder, Registry patterns throughout

## 🏗️ High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EVENT GENERATION LAYER                               │
│  ┌──────────────┐                                                            │
│  │   Producer   │──▶ Faker Library generates realistic gaming events        │
│  │  (Python)    │    - Init events (player opens app)                       │
│  └──────────────┘    - Match events (completed games)                       │
│         │             - Purchase events (in-app transactions)                │
└─────────┼─────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      STREAMING INFRASTRUCTURE                                │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Apache Kafka (3 Topics)                                   │            │
│  │   - init_events    : Player app initialization events       │            │
│  │   - match_events   : Completed game match results           │            │
│  │   - purchase_events: In-app purchase transactions           │            │
│  └─────────────────────────────────────────────────────────────┘            │
└─────────┬───────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER - RAW DATA INGESTION                         │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Spark Structured Streaming (3 Jobs)                       │            │
│  │   - Multiplexed ingestion from all Kafka topics             │            │
│  │   - Preserves raw Kafka metadata (topic, partition, offset) │            │
│  │   - ACID writes to single Delta Lake table                  │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Delta Lake: storage/bronze/events/                        │            │
│  │   - All events in single multiplex table                    │            │
│  │   - Checkpoint: storage/checkpoints/bronze_ingestion_*/     │            │
│  └─────────────────────────────────────────────────────────────┘            │
└─────────┬───────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 SILVER LAYER - DATA QUALITY & ENRICHMENT                     │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Data Quality Pipeline (Spark Streaming)                   │            │
│  │   - YAML-driven transformation rules (extensible)           │            │
│  │   - Uppercase transformations (e.g., platform → PLATFORM)   │            │
│  │   - ID-to-name mapping (e.g., country_id → country_name)    │            │
│  │   - Schema validation and enforcement                       │            │
│  │   - Dual output: Delta Lake + Kafka (optional)              │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Delta Lake: storage/silver/{init,match,purchase}/         │            │
│  │   - Event-specific tables with clean, enriched data         │            │
│  │   - Checkpoints: storage/checkpoints/silver_*/              │            │
│  └─────────────────────────────────────────────────────────────┘            │
└─────────┬───────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER - AGGREGATED ANALYTICS                         │
│  ┌──────────────────────────┐   ┌───────────────────────────┐              │
│  │   Batch Aggregations     │   │  Real-time Aggregations   │              │
│  │   - Daily distinct users │   │  - Minute-level purchases │              │
│  │   - By country/platform  │   │  - Minute-level matches   │              │
│  │   - Historical reports   │   │  - By country granularity │              │
│  └──────────────────────────┘   │  - Live dashboards        │              │
│               │                  └───────────────────────────┘              │
│               ▼                              ▼                               │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Delta Lake: storage/gold/                                 │            │
│  │   - daily_users/            : Batch aggregated metrics      │            │
│  │   - minute_purchases/       : Real-time purchase stats      │            │
│  │   - minute_matches/         : Real-time match stats         │            │
│  │   - minute_*_by_country/    : Country-segmented metrics     │            │
│  └─────────────────────────────────────────────────────────────┘            │
└─────────┬───────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VISUALIZATION & REPORTING                            │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │   Data Visualizer (Python CLI)                              │            │
│  │   - Query all Delta tables across layers                    │            │
│  │   - Display metrics and distributions                       │            │
│  │   - Sample data inspection                                  │            │
│  └─────────────────────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

```bash
# One-command setup - fully automated!
git clone <repository-url>
cd gaming-event-processing
./scripts/quickstart.sh
```

**Duration**: ~3-4 minutes. The script automatically:
- ✅ Validates prerequisites (Docker & Docker Compose)
- ✅ Creates directory structure with proper permissions
- ✅ Builds custom Docker images (Spark + Producer)
- ✅ Starts services in dependency order
- ✅ Initializes Kafka topics and Delta tables
- ✅ Begins event generation and processing

**Access Points**:
- Spark Master UI: http://localhost:9090
- Spark History Server: http://localhost:18080

## 📋 Services Overview

| Service | Component | Purpose | Technology |
|---------|-----------|---------|------------|
| `zookeeper` | Infrastructure | Kafka coordination | Apache Zookeeper |
| `kafka` | Infrastructure | Event streaming broker | Apache Kafka 7.6.0 |
| `spark-master` | Processing | Spark cluster coordinator | Spark 3.5.0 |
| `spark-worker-*` | Processing | Distributed compute (3 workers) | Spark 3.5.0 |
| `event-producer` | Generation | Synthetic event creation | Python + Faker |
| `bronze-ingestion-*` | Bronze Layer | Kafka → Delta ingestion (3 jobs) | Spark Streaming |
| `data-quality` | Silver Layer | Transformation pipeline | Spark Streaming |

## 📊 Event Types

### 1. Init Event
Triggered when a player opens the @8ballpool app (first event of session).

**Schema**: [schemas/init.json](schemas/init.json)

**Fields**: `user-id`, `timestamp`, `platform`, `country`, `device`, `version`

### 2. Match Event
Triggered when two players complete a match.

**Schema**: [schemas/match.json](schemas/match.json)

**Fields**: `user-a`, `user-b`, `winner`, `timestamp`, `game-tier`, `user_a_platform`, `user_b_platform`, `user_a_country`, `user_b_country`

### 3. In-App Purchase Event
Triggered when a player makes a purchase.

**Schema**: [schemas/in-app-purchase.json](schemas/in-app-purchase.json)

**Fields**: `user-id`, `timestamp`, `product-id`, `purchase_value`, `currency`, `country`

## 🛠️ Technology Stack

| Layer | Technology | Version | Purpose |
|-------|------------|---------|---------|
| **Runtime** | Python | 3.11 | Application development |
| **Processing** | Apache Spark | 3.5.0 | Stream & batch processing |
| **Storage** | Delta Lake | 3.2.0 | ACID lakehouse tables |
| **Streaming** | Apache Kafka | 7.6.0 (Confluent) | Event messaging |
| **Orchestration** | Docker Compose | v2+ | Container management |
| **Configuration** | Pydantic | 2.x | Settings & validation |
| **Data Generation** | Faker | 24.x | Realistic test data |
| **Schemas** | JSON Schema | Draft 7 | Event validation |

## 📊 Event Types

## 🔄 Data Transformations

The system implements an extensible data quality framework with YAML-driven transformations:

### Transformation Types

| Type | Purpose | Example | Configuration |
|------|---------|---------|---------------|
| **Uppercase** | Normalize text fields | `android` → `ANDROID` | [uppercase.yml](src/data_quality/config/rules/uppercase.yml) |
| **Mapping** | ID-to-name translation | `US` → `United States` | [mapping.yml](src/data_quality/config/rules/mapping.yml) |

### Extensibility

Add new transformations by:
1. Creating a new transformation class implementing the `Transformation` ABC
2. Registering it in the `TransformationRegistry`
3. Adding YAML configuration under `config/rules/`

See [src/data_quality/README.md](src/data_quality/README.md) for implementation details.

## 📝 Daily Operations

```bash
# View system status
cd build && docker compose ps

# View logs for specific service
docker compose logs -f event-producer
docker compose logs -f data-quality

# Run daily aggregation
./scripts/run-daily-aggregation.sh

# Visualize all data layers
./scripts/visualize-data.sh

# Visualize specific table
./scripts/visualize-data.sh --table bronze
./scripts/visualize-data.sh --table silver-init
./scripts/visualize-data.sh --table gold-daily-users

# Stop all services
./scripts/shutdown.sh
```

## 📚 Documentation Structure

```
/
├── README.md (this file)           # System overview and quick start
├── src/
│   ├── producer/README.md          # Event generation architecture
│   ├── ingestion/README.md         # Bronze layer ingestion details
│   ├── data_quality/README.md      # Silver layer transformations
│   ├── aggregation/
│   │   ├── batch/README.md         # Daily batch aggregations
│   │   └── realtime/README.md      # Minute streaming aggregations
│   ├── visualizer/README.md        # Data visualization tool
│   └── common/README.md            # Shared utilities
└── scripts/README.md               # Operational scripts reference
```

### Component Documentation

- **[Producer](src/producer/README.md)** - Event generation with Faker, Kafka publishing, schema validation
- **[Ingestion](src/ingestion/README.md)** - Bronze layer: Kafka to Delta Lake streaming ingestion
- **[Data Quality](src/data_quality/README.md)** - Silver layer: YAML-driven transformation pipeline
- **[Batch Aggregation](src/aggregation/batch/README.md)** - Gold layer: Daily user metrics
- **[Real-time Aggregation](src/aggregation/realtime/README.md)** - Gold layer: Minute-level analytics
- **[Visualizer](src/visualizer/README.md)** - Query and display Delta tables
- **[Common](src/common/README.md)** - Shared utilities: logging, Spark session factory, config
- **[Scripts](scripts/README.md)** - Operational scripts and workflows

## 📂 Project Structure

```
gaming-event-processing/
├── src/                              # Application source code
│   ├── common/                       # Shared utilities
│   │   ├── config.py                 # Common configuration utilities
│   │   ├── logger.py                 # Structured logging
│   │   └── spark_utils.py            # SparkSession factory
│   ├── producer/                     # Event generation module
│   │   ├── core/                     # Core producer logic
│   │   ├── generation/               # Event generators (Faker-based)
│   │   ├── kafka/                    # Kafka publisher
│   │   └── validation/               # Schema validation
│   ├── ingestion/                    # Bronze layer ingestion
│   │   ├── core/                     # Base classes and config
│   │   ├── builders/                 # Job builder (Factory pattern)
│   │   └── storage/                  # Delta writer
│   ├── data_quality/                 # Silver layer transformations
│   │   ├── core/                     # Pipeline orchestration
│   │   ├── processors/               # Event processors
│   │   ├── transformations/          # Transformation engine
│   │   │   ├── base.py               # Transformation ABC
│   │   │   ├── registry.py           # Registry pattern
│   │   │   ├── loader.py             # YAML rule loader
│   │   │   ├── uppercase.py          # Uppercase transformation
│   │   │   └── mapping.py            # ID-to-name mapping
│   │   ├── writers/                  # Delta and Kafka writers
│   │   └── config/                   # YAML transformation rules
│   │       ├── event_configs.yml     # Event type configurations
│   │       └── rules/                # Transformation rule files
│   ├── aggregation/                  # Gold layer aggregations
│   │   ├── batch/                    # Daily batch processing
│   │   │   ├── aggregator.py         # Batch aggregation logic
│   │   │   ├── config.py             # Batch configuration
│   │   │   └── main.py               # Entry point
│   │   └── realtime/                 # Minute streaming processing
│   │       ├── aggregator.py         # Streaming aggregation logic
│   │       ├── config.py             # Streaming configuration
│   │       └── main.py               # Entry point
│   └── visualizer/                   # Data visualization tool
│       ├── main.py                   # CLI visualization tool
│       └── config.py                 # Visualizer configuration
├── schemas/                          # JSON schema definitions
│   ├── init.json                     # Init event schema
│   ├── match.json                    # Match event schema
│   └── in-app-purchase.json          # Purchase event schema
├── storage/                          # Delta Lake storage (gitignored)
│   ├── bronze/events/                # Raw multiplex table
│   ├── silver/                       # Cleaned event tables
│   │   ├── init/
│   │   ├── match/
│   │   └── purchase/
│   ├── gold/                         # Aggregated metrics
│   │   ├── daily_users/
│   │   ├── minute_purchases/
│   │   ├── minute_matches/
│   │   ├── minute_purchases_by_country/
│   │   └── minute_matches_by_country/
│   └── checkpoints/                  # Streaming checkpoints
├── build/                            # Docker configuration
│   ├── docker-compose.yml            # Service orchestration
│   ├── Dockerfile.spark              # Custom Spark image
│   ├── Dockerfile.producer           # Event producer image
│   ├── .env.example                  # Environment variables template
│   └── config/                       # Spark configuration files
├── scripts/                          # Operational scripts
│   ├── quickstart.sh                 # Automated setup
│   ├── run-daily-aggregation.sh      # Batch job runner
│   ├── visualize-data.sh             # Data viewer
│   ├── shutdown.sh                   # System shutdown
│   └── README.md                     # Scripts documentation
└── README.md                         # This file
```

## ⚙️ Configuration

### Environment Variables

All services are configured through environment variables. See [build/.env.example](build/.env.example) for a complete list.

**Key Configuration Areas**:

| Category | Variables | Default | Description |
|----------|-----------|---------|-------------|
| **Kafka** | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Broker connection string |
| | `INIT_TOPIC` | `init_events` | Init event topic |
| | `MATCH_TOPIC` | `match_events` | Match event topic |
| | `PURCHASE_TOPIC` | `purchase_events` | Purchase event topic |
| **Producer** | `EVENT_RATE_PER_SECOND` | `100` | Events generated per second |
| | `USERS_POOL_SIZE` | `1000` | Unique user pool size |
| | `INIT_PROBABILITY` | `0.3` | Init event weight |
| | `MATCH_PROBABILITY` | `0.5` | Match event weight |
| | `PURCHASE_PROBABILITY` | `0.2` | Purchase event weight |
| **Streaming** | `STREAMING_TRIGGER_INTERVAL` | `10 seconds` | Micro-batch interval |
| | `STREAMING_WATERMARK_DELAY` | `10 minutes` | Late data tolerance |
| | `WINDOW_DURATION` | `1 minute` | Aggregation window size |
| **Spark** | `SPARK_LOG_LEVEL` | `WARN` | Spark logging verbosity |
| **Application** | `LOG_LEVEL` | `INFO` | Python logging level |

### Storage Paths

All Delta Lake paths follow the pattern: `/opt/bitnami/spark/storage/{layer}/{table}/`

**Checkpoint Locations**: `/opt/bitnami/spark/storage/checkpoints/{job_name}/`

## 🎨 Design Patterns

The codebase implements several software engineering design patterns:

| Pattern | Location | Purpose |
|---------|----------|---------|
| **Template Method** | [ingestion/core/base.py](src/ingestion/core/base.py) | Bronze ingestion workflow |
| **Template Method** | [data_quality/core/pipeline.py](src/data_quality/core/pipeline.py) | Silver transformation pipeline |
| **Strategy** | [data_quality/transformations/](src/data_quality/transformations/) | Pluggable transformations |
| **Registry** | [data_quality/transformations/registry.py](src/data_quality/transformations/registry.py) | Transformation registration |
| **Factory** | [ingestion/builders/job_builder.py](src/ingestion/builders/job_builder.py) | Job creation |
| **Builder** | [ingestion/builders/job_builder.py](src/ingestion/builders/job_builder.py) | Complex object construction |
| **Dependency Injection** | Throughout | Configuration and service injection |

## 🧪 Data Quality

The data quality framework provides:

1. **Extensibility**: Add new transformations without modifying core logic
2. **Configuration-Driven**: YAML files define transformation rules
3. **Type Safety**: Pydantic models for configuration validation
4. **Testability**: Clean separation of concerns

**Supported Transformations**:
- Text normalization (uppercase, lowercase, trim)
- ID-to-name mapping (countries, products, etc.)
- Custom transformations via plugin architecture

## 📈 Monitoring & Observability

### Spark UIs

- **Master**: http://localhost:9090 - Cluster status and running jobs
- **History Server**: http://localhost:18080 - Completed job history

### Logs

```bash
# View real-time logs for any service
docker compose -f build/docker-compose.yml logs -f <service-name>

# Examples
docker compose -f build/docker-compose.yml logs -f event-producer
docker compose -f build/docker-compose.yml logs -f data-quality
docker compose -f build/docker-compose.yml logs -f bronze-ingestion-init
```

### Data Validation

```bash
# Query any layer to verify data flow
./scripts/visualize-data.sh --table bronze
./scripts/visualize-data.sh --table silver-init
./scripts/visualize-data.sh --table gold-daily-users
```

## 🔧 Troubleshooting

### Common Issues

**Services won't start**:
```bash
# Check Docker daemon
docker ps

# Verify Docker Compose version
docker compose version  # Should be v2+

# Clean restart
./scripts/shutdown.sh
./scripts/quickstart.sh
```

**No data in Bronze layer**:
```bash
# Check producer logs
docker compose -f build/docker-compose.yml logs event-producer

# Verify Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Spark jobs failing**:
```bash
# Check Spark Master UI
open http://localhost:9090

# View detailed logs
docker compose -f build/docker-compose.yml logs spark-master
```

## 🚀 Performance Tuning

### Event Generation Rate

Adjust in [build/.env.example](build/.env.example):
```bash
EVENT_RATE_PER_SECOND=100  # Increase for higher throughput
```

### Spark Cluster Resources

Modify [build/docker-compose.yml](build/docker-compose.yml):
```yaml
spark-worker-1:
  environment:
    - SPARK_WORKER_CORES=2      # CPU cores per worker
    - SPARK_WORKER_MEMORY=2G    # Memory per worker
```

### Streaming Micro-batch Size

Configure trigger interval:
```bash
STREAMING_TRIGGER_INTERVAL="5 seconds"  # Smaller = lower latency, higher overhead
```

## 🤝 Contributing

This implementation demonstrates data engineering best practices:

- ✅ Clean architecture with separation of concerns
- ✅ Design patterns for extensibility and maintainability
- ✅ Comprehensive documentation with diagrams
- ✅ Type hints and Pydantic validation
- ✅ Structured logging with context
- ✅ Configuration management via environment variables
- ✅ ACID transactions with Delta Lake
- ✅ Schema validation for all events

**For Production**:
- Add monitoring and alerting (Prometheus, Grafana)
- Implement schema evolution with Delta Lake
- Add data quality metrics and SLAs
- Implement retry logic and dead letter queues
- Scale Spark cluster based on workload
- Add authentication and authorization
- Implement data lineage tracking

## 📄 License

This project is a technical implementation demonstrating data engineering best practices for real-time event processing.

---

**Need Help?**
- 📖 Read component-specific documentation in `src/<component>/README.md`
- 🔍 Check [scripts/README.md](scripts/README.md) for operational workflows
- 🐛 Review logs: `docker compose -f build/docker-compose.yml logs -f <service>`
