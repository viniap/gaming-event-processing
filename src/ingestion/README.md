# Bronze Layer Ingestion

Real-time streaming ingestion from Kafka topics to Bronze Delta Lake layer using Spark Structured Streaming. Implements the Template Method pattern for extensible ingestion workflows.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                BRONZE LAYER INGESTION ARCHITECTURE               │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                      Kafka Topics (Input)                         │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐     │
│  │init_events  │   │match_events  │   │purchase_events   │     │
│  └─────────────┘   └──────────────┘   └──────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│              Spark Structured Streaming (3 Jobs)                 │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  BronzeIngestionJob (Template Method Pattern)              │ │
│  │                                                             │ │
│  │  1. read_from_kafka()        ──▶ Read stream from topic   │ │
│  │  2. transform_to_bronze()    ──▶ Add ingestion metadata   │ │
│  │  3. write_to_bronze()        ──▶ Write to Delta Lake      │ │
│  │                                                             │ │
│  │  Template workflow ensures consistent behavior             │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                 Bronze Delta Lake Table (Output)                  │
│                                                                   │
│  storage/bronze/events/  (Multiplexed)                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Schema:                                                    │ │
│  │  - key             : binary   (Kafka key)                  │ │
│  │  - value           : binary   (Kafka message, JSON)        │ │
│  │  - topic           : string   (Source topic name)          │ │
│  │  - partition       : int      (Kafka partition)            │ │
│  │  - offset          : long     (Kafka offset)               │ │
│  │  - timestamp       : timestamp (Kafka timestamp)           │ │
│  │  - timestampType   : int      (0=CreateTime, 1=LogAppend)  │ │
│  │  - kafka_timestamp : timestamp (Original timestamp)        │ │
│  │  - ingestion_timestamp: timestamp (When ingested)          │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  Benefits of single multiplex table:                             │
│  ✓ Simplified architecture                                       │
│  ✓ Single source of truth for raw data                           │
│  ✓ Easy replay and reprocessing                                  │
│  ✓ Topic filtering in downstream processing                      │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                  Checkpoints (Per Topic)                          │
│  storage/checkpoints/bronze_ingestion_init-events/               │
│  storage/checkpoints/bronze_ingestion_match-events/              │
│  storage/checkpoints/bronze_ingestion_purchase-events/           │
│                                                                   │
│  Enables exactly-once semantics and failure recovery             │
└──────────────────────────────────────────────────────────────────┘
```

## Design Patterns

### Template Method Pattern

**Base Class**: `BronzeIngestionJob` (abstract)

Defines the ingestion workflow skeleton:

```python
def run(self) -> None:
    """Template method defining the workflow."""
    self._setup_spark()
    df = self.read_from_kafka()
    df = self.transform_to_bronze(df)
    query = self.write_to_bronze(df)
    self._wait_for_termination(query)
```

**Concrete Class**: `ConfigurableBronzeIngestion`

Implements specific behavior for each step while the workflow remains fixed.

### Factory Pattern

**Builder**: `BronzeIngestionJobBuilder`

Creates job instances with proper configuration:

```python
builder = BronzeIngestionJobBuilder()
job = builder.with_config(config).build()
```

### Dependency Injection

Configuration injected via constructor, enabling testability and flexibility:

```python
job = BronzeEventIngestion(
    config=config,
    spark_factory=SparkSessionFactory,
    logger=logger
)
```

## Components

### 1. Configuration (`core/config.py`)

**Class**: `BronzeIngestionConfig`

Pydantic-based configuration with environment variable support.

| Setting | Default | Description |
|---------|---------|-------------|
| `kafka_bootstrap_servers` | `kafka:9092` | Kafka broker addresses |
| `kafka_topic` | **Required** | Topic to consume (set via env) |
| `kafka_starting_offsets` | `earliest` | Where to start reading |
| `storage_base_path` | `/opt/bitnami/spark/storage` | Base storage path |
| `storage_bronze_path` | `{base}/bronze/events` | Bronze table location |
| `checkpoint_bronze` | `{base}/checkpoints/bronze_ingestion_{topic}` | Checkpoint directory |
| `streaming_trigger_interval` | `10 seconds` | Micro-batch interval |
| `streaming_max_offsets_per_trigger` | `10000` | Max records per batch |
| `spark_app_name` | `bronze-ingestion-{topic}` | Spark application name |
| `spark_log_level` | `WARN` | Spark logging level |
| `log_level` | `INFO` | Application logging level |

### 2. Base Ingestion Job (`core/base.py`)

**Abstract Class**: `BronzeIngestionJob`

Defines the contract for all bronze ingestion jobs:

**Abstract Methods**:
- `read_from_kafka() -> DataFrame`: Read streaming data from Kafka
- `transform_to_bronze(df) -> DataFrame`: Add bronze layer metadata
- `write_to_bronze(df) -> StreamingQuery`: Write to Delta Lake

**Template Method**:
- `run()`: Orchestrates the complete workflow

**Concrete Class**: `ConfigurableBronzeIngestion`

Standard implementation reading from configured Kafka topic and writing to bronze Delta table.

### 3. Job Builder (`builders/job_builder.py`)

**Class**: `BronzeIngestionJobBuilder`

Factory for creating configured ingestion jobs:

```python
builder = BronzeIngestionJobBuilder()
job = (builder
    .with_config(config)
    .with_spark_factory(SparkSessionFactory)
    .with_logger(logger)
    .build())
```

**Methods**:
- `with_config(config)`: Set configuration
- `with_spark_factory(factory)`: Set Spark session factory
- `with_logger(logger)`: Set logger instance
- `build()`: Create job instance

### 4. Bronze Writer (`storage/bronze_writer.py`)

**Class**: `BronzeEventIngestion`

Implements the ingestion logic:

**Key Methods**:

`read_from_kafka()`: 
- Creates streaming DataFrame from Kafka
- Subscribes to single topic
- Reads from configured starting offset
- Returns raw Kafka schema

`transform_to_bronze()`:
- Renames `timestamp` to `kafka_timestamp` (avoids conflict)
- Adds `ingestion_timestamp` with current timestamp
- Preserves all Kafka metadata (key, value, topic, partition, offset)
- Returns transformed DataFrame

`write_to_bronze()`:
- Writes to Delta Lake format
- Uses append mode (multiplexed table)
- Enables checkpointing for fault tolerance
- Configures trigger interval for micro-batches
- Returns StreamingQuery for monitoring

### 5. Main Entry Point (`main.py`)

**Function**: `main()`

Application entry point that:
1. Loads configuration from environment
2. Validates required settings (KAFKA_TOPIC)
3. Initializes structured logger
4. Creates job via builder
5. Executes ingestion workflow
6. Handles graceful shutdown

## Running Ingestion Jobs

### Using Quickstart (Recommended)

All three ingestion jobs start automatically:

```bash
./scripts/quickstart.sh
```

This starts:
- `bronze-ingestion-init` - Ingests from init_events
- `bronze-ingestion-match` - Ingests from match_events  
- `bronze-ingestion-purchase` - Ingests from purchase_events

### Manual Execution via Docker

```bash
# Init events
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  -e KAFKA_TOPIC=init_events \
  spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
    --conf "spark.jars.ivy=/tmp/ivy-user" \
    /opt/bitnami/spark/jobs/src/ingestion/main.py

# Match events
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  -e KAFKA_TOPIC=match_events \
  spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
    --conf "spark.jars.ivy=/tmp/ivy-user" \
    /opt/bitnami/spark/jobs/src/ingestion/main.py

# Purchase events  
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  -e KAFKA_TOPIC=purchase_events \
  spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
    --conf "spark.jars.ivy=/tmp/ivy-user" \
    /opt/bitnami/spark/jobs/src/ingestion/main.py
```

### Local Development

```bash
# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092
export KAFKA_TOPIC=init_events
export STORAGE_BASE_PATH=./storage
export LOG_LEVEL=INFO

# Run with spark-submit
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0 \
  src/ingestion/main.py
```

## Configuration

### Environment Variables

```bash
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=init_events              # Required: which topic to ingest
KAFKA_STARTING_OFFSETS=earliest      # earliest|latest|json

# Storage paths  
STORAGE_BASE_PATH=/opt/bitnami/spark/storage
STORAGE_BRONZE_PATH=/opt/bitnami/spark/storage/bronze/events
CHECKPOINT_BRONZE=/opt/bitnami/spark/storage/checkpoints/bronze_ingestion_init-events

# Streaming settings
STREAMING_TRIGGER_INTERVAL=10 seconds
STREAMING_MAX_OFFSETS_PER_TRIGGER=10000

# Spark settings
SPARK_APP_NAME=bronze-ingestion-init-events
SPARK_LOG_LEVEL=WARN

# Logging
LOG_LEVEL=INFO
```

### Configuration Priority

1. Environment variables (highest)
2. `.env` file
3. Default values in `config.py` (lowest)

## Data Flow

```
Kafka Topic                 Structured Streaming              Bronze Delta Table
┌─────────────┐            ┌──────────────────┐             ┌─────────────────┐
│init_events  │────read───▶│  Spark Stream    │────write───▶│ storage/bronze/ │
│             │            │                  │             │     events/     │
│  - key      │            │  Transformations:│             │                 │
│  - value    │            │  - rename columns│             │  Single table   │
│  - metadata │            │  - add timestamp │             │  for all topics │
└─────────────┘            └──────────────────┘             └─────────────────┘
                                    │                                │
                                    ▼                                ▼
                           ┌──────────────────┐             ┌─────────────────┐
                           │   Checkpoint     │             │  Delta Log      │
                           │   (per topic)    │             │  _delta_log/    │
                           └──────────────────┘             └─────────────────┘
                           
Exactly-once semantics via checkpointing + idempotent writes
```

## Bronze Table Schema

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| `key` | binary | Kafka message key | Kafka |
| `value` | binary | JSON message payload | Kafka |
| `topic` | string | Source topic name | Kafka |
| `partition` | int | Kafka partition number | Kafka |
| `offset` | long | Message offset in partition | Kafka |
| `kafka_timestamp` | timestamp | Original Kafka timestamp | Kafka (renamed) |
| `timestampType` | int | 0=CreateTime, 1=LogAppendTime | Kafka |
| `ingestion_timestamp` | timestamp | When record was ingested | Added |

**Key Design Decisions**:

1. **Multiplex Table**: Single table for all events
   - Simplifies architecture
   - Topic field enables filtering
   - Easier replay and auditing

2. **Binary Value**: Preserves raw JSON
   - No schema evolution issues
   - Downstream can parse differently
   - Supports schema validation at source

3. **Kafka Metadata**: Full Kafka context
   - Enables exactly-once processing
   - Supports lineage tracking
   - Facilitates debugging

4. **Ingestion Timestamp**: Processing time
   - Distinguishes event time from processing time
   - Useful for latency monitoring
   - Enables data quality checks

## Monitoring

### Streaming Query Metrics

```python
# Access from StreamingQuery object
query.lastProgress  # Latest batch progress
query.status        # Current status
query.recentProgress  # Recent batches
```

### Spark UI

View running jobs at http://localhost:9090
- Batch durations
- Input rates
- Processing rates  
- Trigger details

### Logs

```bash
# View ingestion logs
docker compose -f build/docker-compose.yml logs -f bronze-ingestion-init
docker compose -f build/docker-compose.yml logs -f bronze-ingestion-match
docker compose -f build/docker-compose.yml logs -f bronze-ingestion-purchase

# Sample log output
INFO: Bronze ingestion job initialized
INFO: Kafka topic: init_events
INFO: Bronze path: /opt/bitnami/spark/storage/bronze/events
INFO: Checkpoint: /opt/bitnami/spark/storage/checkpoints/bronze_ingestion_init-events
INFO: Reading from Kafka topic: init_events
INFO: Writing to bronze Delta table
INFO: Streaming query started successfully
```

### Data Validation

```bash
# Check bronze data
./scripts/visualize-data.sh --table bronze

# Or query directly
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  -e "SELECT topic, COUNT(*) FROM delta.\`/opt/bitnami/spark/storage/bronze/events\` GROUP BY topic"
```

## Checkpointing

### Purpose

Checkpoints provide:
- **Exactly-once semantics**: Each Kafka offset processed exactly once
- **Fault tolerance**: Resume from last committed offset on failure
- **State management**: Track processing progress

### Checkpoint Structure

```
storage/checkpoints/bronze_ingestion_init-events/
├── commits/                    # Committed batches
│   ├── 0
│   ├── 1
│   └── 2
├── offsets/                    # Kafka offsets
│   ├── 0
│   ├── 1
│   └── 2
├── sources/                    # Source info
│   └── 0/
│       └── 0
└── metadata                    # Query metadata
```

### Checkpoint Management

```bash
# List checkpoints
ls -la storage/checkpoints/

# Clear checkpoint to restart from beginning
rm -rf storage/checkpoints/bronze_ingestion_init-events/
# Job will restart from KAFKA_STARTING_OFFSETS

# View checkpoint metadata
cat storage/checkpoints/bronze_ingestion_init-events/metadata
```

**⚠️ Warning**: Deleting checkpoints causes data reprocessing. Bronze table should use append mode to avoid duplicates (downstream deduplication recommended).

## Error Handling

### Kafka Connection Failures

If Kafka is unavailable:
1. Spark retries connection automatically
2. Job fails after timeout
3. Container restarts (Docker Compose)
4. Job resumes from checkpoint

### Schema Evolution

Bronze layer stores raw binary data, so schema changes don't break ingestion. Validation happens downstream.

### Backpressure

If Delta writes can't keep up:
1. Configured via `maxOffsetsPerTrigger`
2. Spark automatically throttles reads
3. Monitor via batch durations in Spark UI

## Performance Tuning

### Increase Throughput

```bash
# Process more records per batch
STREAMING_MAX_OFFSETS_PER_TRIGGER=50000

# Reduce micro-batch interval
STREAMING_TRIGGER_INTERVAL=5 seconds

# Scale Spark cluster
docker-compose scale spark-worker=5
```

### Reduce Latency

```bash
# Continuous processing
STREAMING_TRIGGER_INTERVAL=0 seconds  # Continuous mode

# Smaller batches
STREAMING_MAX_OFFSETS_PER_TRIGGER=1000
```

### Optimize Delta Writes

```bash
# Delta table properties
ALTER TABLE delta.`/opt/bitnami/spark/storage/bronze/events`
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

## Troubleshooting

### No Data Ingested

**Check producer is running**:
```bash
docker compose -f build/docker-compose.yml ps event-producer
```

**Verify Kafka has data**:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic init_events \
  --max-messages 5
```

**Check ingestion logs**:
```bash
docker compose -f build/docker-compose.yml logs bronze-ingestion-init
```

### Checkpoint Corruption

**Symptoms**: Job fails to start, checkpoint errors in logs

**Solution**:
```bash
# Back up checkpoint
mv storage/checkpoints/bronze_ingestion_init-events \
   storage/checkpoints/bronze_ingestion_init-events.backup

# Job will create new checkpoint and restart from configured offset
```

### High Latency

**Check batch durations** in Spark UI (http://localhost:9090)

**Common causes**:
- Delta Lake compaction running
- Insufficient Spark resources
- Network latency to Kafka
- Large batches (`maxOffsetsPerTrigger` too high)

**Solutions**:
```bash
# Add more workers
docker-compose scale spark-worker=5

# Tune batch size
STREAMING_MAX_OFFSETS_PER_TRIGGER=5000

# Increase trigger interval
STREAMING_TRIGGER_INTERVAL=30 seconds
```

## Advanced Topics

### Custom Transformations

Extend `BronzeIngestionJob` for custom logic:

```python
class CustomBronzeIngestion(BronzeIngestionJob):
    def transform_to_bronze(self, df: DataFrame) -> DataFrame:
        # Add custom transformations
        return (df
            .withColumn("kafka_timestamp", col("timestamp"))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("custom_field", lit("value")))
```

### Multi-topic Ingestion

Subscribe to multiple topics in one job:

```python
df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
    .option("subscribe", "init_events,match_events,purchase_events")
    .load())
```

### Schema Enforcement

Add validation before writing:

```python
def transform_to_bronze(self, df: DataFrame) -> DataFrame:
    # Validate value is valid JSON
    from pyspark.sql.functions import from_json, schema_of_json
    
    # Parse JSON to validate
    parsed = df.withColumn("parsed", from_json(col("value").cast("string"), schema))
    
    # Filter invalid records
    valid = parsed.filter(col("parsed").isNotNull())
    
    return valid.select("key", "value", "topic", ...)
```

## Testing

### Unit Tests

```python
def test_transform_adds_timestamp():
    """Test that transform adds ingestion timestamp."""
    config = BronzeIngestionConfig(kafka_topic="test")
    job = BronzeEventIngestion(config)
    
    # Create test DataFrame
    data = [("key", "value", "test_topic", 0, 1, datetime.now(), 0)]
    df = spark.createDataFrame(data, ["key", "value", "topic", ...])
    
    # Transform
    result = job.transform_to_bronze(df)
    
    # Assert
    assert "ingestion_timestamp" in result.columns
    assert "kafka_timestamp" in result.columns
```

### Integration Tests

```python
def test_end_to_end_ingestion(kafka_producer, spark_session):
    """Test complete ingestion flow."""
    # Produce test event
    kafka_producer.send("test_topic", value=b'{"test": "data"}')
    kafka_producer.flush()
    
    # Run ingestion
    config = BronzeIngestionConfig(
        kafka_topic="test_topic",
        storage_bronze_path="/tmp/test_bronze"
    )
    job = BronzeEventIngestion(config)
    
    # Let it run for a few batches
    query = job.run()
    time.sleep(30)
    query.stop()
    
    # Verify data
    result = spark_session.read.format("delta").load("/tmp/test_bronze")
    assert result.count() > 0
    assert result.filter(col("topic") == "test_topic").count() > 0
```

## Best Practices

1. **One job per topic**: Simplifies monitoring and checkpoint management
2. **Multiplex at bronze**: Single source of truth for all raw events
3. **Preserve metadata**: Keep all Kafka context for debugging
4. **Binary storage**: Store raw bytes to avoid schema coupling
5. **Checkpointing**: Always enable for production workloads
6. **Monitoring**: Track batch durations and processing rates
7. **Resource allocation**: Size Spark cluster based on peak load
8. **Backpressure**: Configure `maxOffsetsPerTrigger` to prevent overload

## Future Enhancements

- [ ] Add schema registry integration
- [ ] Implement data quality metrics
- [ ] Add alerting on processing delays
- [ ] Support schema evolution tracking
- [ ] Implement data retention policies
- [ ] Add metrics export to Prometheus
- [ ] Support multiple storage backends (S3, ADLS)
- [ ] Implement automated checkpoint cleanup
docker exec spark-master ls -lah /opt/bitnami/spark/storage/checkpoints/bronze_ingestion_init-events/
```

### Spark UI

Access the Spark UI at: http://localhost:9090

## Troubleshooting

### ModuleNotFoundError: No module named 'src'

Make sure `PYTHONPATH=/opt/bitnami/spark/jobs` is set when running spark-submit.

### Failed to find data source: kafka

The Kafka connector package is required. Make sure you include:
```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0
```

### Permission denied on Ivy cache

Use a writable Ivy cache directory:
```bash
--conf "spark.jars.ivy=/tmp/ivy-user"
```

Create the directory with:
```bash
docker exec spark-master bash -c "mkdir -p /tmp/ivy-user && chmod 777 /tmp/ivy-user"
```

## Design Patterns

### Template Method Pattern

The `BronzeIngestionJob` base class defines the skeleton of the ingestion algorithm:

```python
def run(self):
    self.spark = self._create_spark_session()      # Step 1
    kafka_stream = self._read_kafka_stream()        # Step 2
    parsed_stream = self._parse_kafka_messages()    # Step 3
    transformed_stream = self._transform_data()     # Step 4
    query = self._write_to_bronze()                 # Step 5
    query.awaitTermination()                        # Step 6
```

Subclasses can override specific steps without changing the overall algorithm.

### Configurable Implementation

The `ConfigurableBronzeIngestion` class uses dependency injection to accept all configuration through the constructor, making it highly testable and reusable.

## Examples

### Running All Three Topics Simultaneously

```bash
# Terminal 1
./scripts/run-ingestion.sh init_events

# Terminal 2
./scripts/run-ingestion.sh match_events

# Terminal 3
./scripts/run-ingestion.sh purchase_events
```

Each job will:
1. Connect to Kafka and start reading from the specified topic
2. Write raw messages to the bronze Delta Lake table
3. Maintain checkpoints for fault tolerance
4. Run continuously until stopped (Ctrl+C)
