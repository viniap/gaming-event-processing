# Data Quality (Silver Layer)

YAML-driven data quality and transformation pipeline that reads from Bronze Delta tables, applies configurable transformations, and writes to Silver Delta tables with dual output to Kafka. Implements extensible transformation framework using Strategy and Registry patterns.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                  DATA QUALITY PIPELINE ARCHITECTURE                       │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                     Bronze Delta Table (Input)                            │
│  storage/bronze/events/  (Multiplexed raw events)                        │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  topic | value (JSON) | kafka_timestamp | ingestion_timestamp      │ │
│  │  ------|--------------|-----------------|-------------------------  │ │
│  │  init_events  | {...}  | 2024-01-29...   | 2024-01-29...          │ │
│  │  match_events | {...}  | 2024-01-29...   | 2024-01-29...          │ │
│  │  purchase_... | {...}  | 2024-01-29...   | 2024-01-29...          │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               Silver Data Quality Pipeline (Template Method)              │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  SilverDataQualityPipeline (Orchestrator)                          │  │
│  │                                                                     │  │
│  │  1. Load transformation rules from YAML                            │  │
│  │  2. For each event type (init, match, purchase):                   │  │
│  │     a. Filter bronze by topic                                      │  │
│  │     b. Parse JSON value to struct                                  │  │
│  │     c. Apply transformations (Strategy pattern)                    │  │
│  │     d. Write to Silver Delta + Kafka                               │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌──────────────────────────┐    ┌────────────────────────────┐
│  Transformation Engine    │    │  Event Processor           │
│  (Strategy Pattern)       │    │  - Schema parsing          │
│                           │    │  - Column selection        │
│  ┌────────────────────┐  │    │  - Timestamp handling      │
│  │ TransformationLdr │  │    └────────────────────────────┘
│  │ - Load YAML rules │  │
│  │ - Build pipeline  │  │
│  └────────────────────┘  │
│                           │
│  ┌────────────────────┐  │
│  │ Registry Pattern   │  │
│  │ - Uppercase        │  │
│  │ - Mapping          │  │
│  │ - (Extensible)     │  │
│  └────────────────────┘  │
│                           │
│  Each transformation:     │
│  - Implements ABC         │
│  - Registered in registry │
│  - Configured via YAML    │
└──────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Transformed DataFrames                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌───────────────────┐           │
│  │  Init Events │   │ Match Events │   │ Purchase Events   │           │
│  │  + country_  │   │ + user_a_    │   │ + product_name    │           │
│  │    name      │   │   platform   │   │   (mapped)        │           │
│  │  + PLATFORM  │   │   (UPPER)    │   │                   │           │
│  │    (UPPER)   │   │              │   │                   │           │
│  └──────────────┘   └──────────────┘   └───────────────────┘           │
└──────────────────────────────────────────────────────────────────────────┘
              │
    ┌─────────┴─────────┐
    ▼                   ▼
┌────────────────┐  ┌────────────────┐
│  Delta Writer  │  │  Kafka Writer  │
│  (ACID writes) │  │  (Optional)    │
└────────────────┘  └────────────────┘
    │                   │
    ▼                   ▼
┌────────────────┐  ┌────────────────┐
│ Silver Tables  │  │ silver_events  │
│ storage/silver/│  │     topic      │
│ - init/        │  └────────────────┘
│ - match/       │
│ - purchase/    │
└────────────────┘
```

## Design Patterns

### Template Method Pattern

**Class**: `SilverDataQualityPipeline`

Defines workflow skeleton in `run()` method:

```python
def run(self):
    """Template method orchestrating the pipeline."""
    self._load_transformation_rules()
    
    for event_type in ["init", "match", "purchase"]:
        self._process_event_stream(
            event_type=event_type,
            topic_name=f"{event_type}_events",
            output_path=self.config.get_silver_path(event_type),
            checkpoint_path=self.config.get_checkpoint_path(event_type)
        )
    
    self._wait_for_termination()
```

### Strategy Pattern

**Abstract Base**: `Transformation`

```python
class Transformation(ABC):
    @abstractmethod
    def apply(self, df: DataFrame, field: str, config: Dict) -> DataFrame:
        """Apply transformation to DataFrame."""
        pass
```

**Concrete Implementations**:
- `UppercaseTransformation`: Convert text to uppercase
- `MappingTransformation`: Map IDs to names using dictionaries

### Registry Pattern

**Class**: `TransformationRegistry`

Central registry for all transformation types:

```python
registry = TransformationRegistry()
registry.register("uppercase", UppercaseTransformation())
registry.register("mapping", MappingTransformation())

# Get transformation by name
transformer = registry.get("uppercase")
```

Enables dynamic transformation loading from YAML configuration.

## Components

### 1. Configuration (`core/config.py`)

**Class**: `DataQualityConfig`

Pydantic-based configuration management:

| Setting | Default | Description |
|---------|---------|-------------|
| `storage_bronze_path` | `/opt/bitnami/spark/storage/bronze/events` | Bronze input path |
| `storage_silver_init_path` | `.../silver/init` | Silver init output |
| `storage_silver_match_path` | `.../silver/match` | Silver match output |
| `storage_silver_purchase_path` | `.../silver/purchase` | Silver purchase output |
| `checkpoint_silver_init` | `.../checkpoints/silver_init` | Init checkpoint |
| `checkpoint_silver_match` | `.../checkpoints/silver_match` | Match checkpoint |
| `checkpoint_silver_purchase` | `.../checkpoints/silver_purchase` | Purchase checkpoint |
| `kafka_bootstrap_servers` | `kafka:9092` | Kafka broker connection |
| `kafka_silver_topic` | `silver_events` | Output Kafka topic |
| `enable_kafka_output` | `true` | Enable Kafka publishing |
| `rules_dir` | `.../config/rules` | Transformation rules directory |
| `event_configs_path` | `.../config/event_configs.yml` | Event type configurations |
| `streaming_trigger_interval` | `10 seconds` | Micro-batch interval |

### 2. Pipeline Orchestrator (`core/pipeline.py`)

**Class**: `SilverDataQualityPipeline`

Main orchestrator implementing Template Method pattern.

**Key Methods**:

`run()`: Main workflow
- Loads transformation rules
- Processes each event type (init, match, purchase)
- Manages streaming queries
- Waits for termination

`_process_event_stream()`: Per-event-type processing
- Reads from bronze Delta table
- Filters by topic
- Parses JSON to struct
- Applies transformations via `TransformationLoader`
- Writes to Silver Delta table
- Optionally publishes to Kafka

`_create_silver_stream()`: Creates base stream
- Reads from bronze as stream
- Filters by topic name
- Returns streaming DataFrame

### 3. Event Processor (`processors/event_processor.py`)

**Class**: `EventProcessor`

Handles event-specific parsing and schema application.

**Methods**:

`parse_and_transform()`:
- Parses JSON `value` column to struct
- Flattens nested fields
- Selects relevant columns per event type
- Adds processing timestamp
- Returns transformed DataFrame

`_get_schema()`:
- Loads JSON schema for event type
- Converts to Spark struct schema
- Uses `EventSchemaProvider` for schema inference

### 4. Transformation Loader (`transformations/loader.py`)

**Class**: `TransformationLoader`

Loads and applies YAML-defined transformation rules.

**Methods**:

`load_transformations(event_type)`:
- Reads YAML files from `config/rules/`
- Parses transformation definitions
- Returns list of transformation steps

`apply_transformations(df, event_type)`:
- Iterates through transformation rules
- Gets transformer from registry
- Applies each transformation sequentially
- Returns fully transformed DataFrame

**YAML Rule Format**:

```yaml
# uppercase.yml
transformations:
  - type: uppercase
    apply_to:
      init:
        - platform
      match:
        - user_a_platform
        - user_b_platform
```

```yaml
# mapping.yml
transformations:
  - type: mapping
    apply_to:
      init:
        - field: country
          new_field: country_name
          mapping:
            US: "United States"
            UK: "United Kingdom"
            BR: "Brazil"
            # ... more mappings
      purchase:
        - field: product-id
          new_field: product_name
          mapping:
            coins_100: "100 Coins Pack"
            coins_500: "500 Coins Pack"
            # ... more mappings
```

### 5. Transformation Implementations

#### UppercaseTransformation (`transformations/uppercase.py`)

Converts string column values to uppercase.

**Usage**:
```python
transformer = UppercaseTransformation()
df = transformer.apply(df, field="platform", config={})
# "android" → "ANDROID", "ios" → "IOS"
```

**Implementation**:
```python
def apply(self, df: DataFrame, field: str, config: Dict) -> DataFrame:
    return df.withColumn(field, upper(col(field)))
```

#### MappingTransformation (`transformations/mapping.py`)

Maps values from one domain to another using dictionaries.

**Usage**:
```python
config = {
    "new_field": "country_name",
    "mapping": {"US": "United States", "UK": "United Kingdom"}
}
transformer = MappingTransformation()
df = transformer.apply(df, field="country", config=config)
# Adds column "country_name" with mapped values
```

**Implementation**:
```python
def apply(self, df: DataFrame, field: str, config: Dict) -> DataFrame:
    mapping_dict = config["mapping"]
    new_field = config.get("new_field", f"{field}_mapped")
    
    # Create mapping expression
    mapping_expr = create_map(
        [lit(x) for pair in mapping_dict.items() for x in pair]
    )
    
    return df.withColumn(new_field, mapping_expr[col(field)])
```

### 6. Writers

#### DeltaWriter (`writers/delta_writer.py`)

Writes transformed data to Silver Delta tables.

**Features**:
- Streaming writes in append mode
- Checkpoint-based fault tolerance
- Configurable trigger interval
- ACID guarantees via Delta Lake

**Method**:
```python
def write_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str
) -> StreamingQuery:
    return (df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start(output_path))
```

#### KafkaWriter (`writers/kafka_writer.py`)

Publishes transformed events to Kafka topic (optional).

**Features**:
- JSON serialization to string
- Adds metadata fields (event_type, processing_timestamp)
- Key-based partitioning for ordering
- Separate checkpoint from Delta writer

**Method**:
```python
def write_stream(
    df: DataFrame,
    kafka_servers: str,
    topic: str,
    checkpoint_path: str,
    trigger_interval: str
) -> StreamingQuery:
    kafka_df = (df
        .select(
            col("user-id").cast("string").alias("key"),
            to_json(struct("*")).alias("value")
        ))
    
    return (kafka_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start())
```

## Configuration Files

### Event Configurations (`config/event_configs.yml`)

Defines event types and their topics:

```yaml
events:
  init:
    topic: "init_events"
    schema: "schemas/init.json"
  match:
    topic: "match_events"
    schema: "schemas/match.json"
  purchase:
    topic: "purchase_events"
    schema: "schemas/in-app-purchase.json"
```

### Transformation Rules

#### Uppercase Rules (`config/rules/uppercase.yml`)

```yaml
transformations:
  - type: uppercase
    apply_to:
      init:
        - platform
      match:
        - user_a_platform
        - user_b_platform
```

#### Mapping Rules (`config/rules/mapping.yml`)

```yaml
transformations:
  - type: mapping
    apply_to:
      init:
        - field: country
          new_field: country_name
          mapping:
            US: "United States"
            UK: "United Kingdom"
            BR: "Brazil"
            DE: "Germany"
            FR: "France"
            ES: "Spain"
            IT: "Italy"
            JP: "Japan"
            CN: "China"
            IN: "India"
            
      purchase:
        - field: product-id
          new_field: product_name
          mapping:
            coins_100: "100 Coins Pack"
            coins_500: "500 Coins Pack"
            coins_1000: "1000 Coins Pack"
            premium_cue: "Premium Cue Stick"
            vip_pass: "VIP Membership Pass"
```

## Running the Pipeline

### Using Quickstart (Recommended)

The data quality pipeline starts automatically:

```bash
./scripts/quickstart.sh
```

### Manual Execution

```bash
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf "spark.jars.ivy=/tmp/ivy-user" \
    /opt/bitnami/spark/jobs/src/data_quality/main.py
```

### Local Development

```bash
# Set environment
export STORAGE_BRONZE_PATH=./storage/bronze/events
export STORAGE_SILVER_INIT_PATH=./storage/silver/init
export RULES_DIR=./src/data_quality/config/rules
export LOG_LEVEL=INFO

# Run with spark-submit
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  src/data_quality/main.py
```

## Monitoring

### Logs

```bash
# View pipeline logs
docker compose -f build/docker-compose.yml logs -f data-quality

# Sample output
INFO: Silver data quality pipeline initialized
INFO: Loading transformation rules from /opt/bitnami/spark/jobs/src/data_quality/config/rules
INFO: Loaded 2 transformation rules
INFO: Processing init events
INFO: Applying transformations: uppercase, mapping
INFO: Writing to /opt/bitnami/spark/storage/silver/init
INFO: Streaming query started for init events
INFO: Processing match events
...
```

### Data Validation

```bash
# Check silver data
./scripts/visualize-data.sh --table silver-init
./scripts/visualize-data.sh --table silver-match
./scripts/visualize-data.sh --table silver-purchase

# Verify transformations applied
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  -e "SELECT platform, country_name, COUNT(*) 
      FROM delta.\`/opt/bitnami/spark/storage/silver/init\`
      GROUP BY platform, country_name
      LIMIT 10"
```

### Spark UI

Monitor streaming jobs at http://localhost:9090
- Batch processing times
- Transformation latency
- Output rates to Delta and Kafka

## Extending with New Transformations

### 1. Create Transformation Class

```python
# src/data_quality/transformations/lowercase.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower, col
from src.data_quality.transformations.base import Transformation

class LowercaseTransformation(Transformation):
    """Convert text field to lowercase."""
    
    def apply(self, df: DataFrame, field: str, config: Dict) -> DataFrame:
        """
        Apply lowercase transformation to specified field.
        
        Args:
            df: Input DataFrame
            field: Column name to transform
            config: Transformation configuration (unused for lowercase)
            
        Returns:
            DataFrame with lowercased field.
        """
        return df.withColumn(field, lower(col(field)))
```

### 2. Register in Registry

```python
# src/data_quality/transformations/registry.py
def _register_default_transformations(self):
    """Register built-in transformations."""
    self.register("uppercase", UppercaseTransformation())
    self.register("mapping", MappingTransformation())
    self.register("lowercase", LowercaseTransformation())  # Add this
```

### 3. Create YAML Rule

```yaml
# src/data_quality/config/rules/lowercase.yml
transformations:
  - type: lowercase
    apply_to:
      init:
        - device
      match:
        - game-tier
```

### 4. Use in Pipeline

The transformation is automatically picked up by the loader!

```python
loader = TransformationLoader(config)
df = loader.apply_transformations(df, event_type="init")
# lowercase transformation applied automatically
```

## Performance Tuning

### Optimize Transformation Order

```yaml
# Apply cheaper transformations first
transformations:
  - type: uppercase      # Fast: simple upper()
    apply_to: ...
  - type: mapping        # Slower: dictionary lookup
    apply_to: ...
```

### Increase Throughput

```bash
# Larger micro-batches
STREAMING_TRIGGER_INTERVAL=30 seconds

# Add more Spark workers
docker-compose scale spark-worker=5
```

### Reduce Latency

```bash
# Smaller micro-batches
STREAMING_TRIGGER_INTERVAL=5 seconds

# Continuous processing (zero latency)
STREAMING_TRIGGER_INTERVAL=0 seconds
```

### Disable Kafka Output

If Kafka output not needed:

```bash
ENABLE_KAFKA_OUTPUT=false
```

Reduces overhead of dual writes.

## Troubleshooting

### Transformation Not Applied

**Check rule file exists**:
```bash
ls -la src/data_quality/config/rules/
```

**Verify YAML syntax**:
```bash
python -c "import yaml; print(yaml.safe_load(open('src/data_quality/config/rules/uppercase.yml')))"
```

**Check logs for parsing errors**:
```bash
docker compose -f build/docker-compose.yml logs data-quality | grep ERROR
```

### Checkpoint Corruption

```bash
# Backup and remove checkpoint
mv storage/checkpoints/silver_init storage/checkpoints/silver_init.backup

# Job will create new checkpoint
# Note: May cause data reprocessing
```

### Schema Mismatch

If JSON schema doesn't match actual data:

```bash
# Check bronze data
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  -e "SELECT value FROM delta.\`/opt/bitnami/spark/storage/bronze/events\`
      WHERE topic = 'init_events' LIMIT 1"

# Compare with schema file
cat schemas/init.json
```

## Testing

### Unit Tests

```python
def test_uppercase_transformation():
    """Test uppercase transformation."""
    transformer = UppercaseTransformation()
    
    data = [("android",), ("ios",), ("web",)]
    df = spark.createDataFrame(data, ["platform"])
    
    result = transformer.apply(df, "platform", {})
    
    values = [row.platform for row in result.collect()]
    assert values == ["ANDROID", "IOS", "WEB"]

def test_mapping_transformation():
    """Test mapping transformation."""
    transformer = MappingTransformation()
    config = {
        "new_field": "country_name",
        "mapping": {"US": "United States", "UK": "United Kingdom"}
    }
    
    data = [("US",), ("UK",), ("BR",)]
    df = spark.createDataFrame(data, ["country"])
    
    result = transformer.apply(df, "country", config)
    
    assert "country_name" in result.columns
    us_name = result.filter(col("country") == "US").first().country_name
    assert us_name == "United States"
```

### Integration Tests

```python
def test_end_to_end_pipeline(spark_session):
    """Test complete pipeline from bronze to silver."""
    # Create test bronze data
    bronze_data = [(
        b'key',
        b'{"user-id": "test123", "platform": "android", "country": "US"}',
        "init_events",
        0, 0,
        datetime.now(),
        0,
        datetime.now(),
        datetime.now()
    )]
    
    # Write to bronze
    df = spark_session.createDataFrame(bronze_data, bronze_schema)
    df.write.format("delta").mode("append").save("/tmp/test_bronze")
    
    # Run pipeline
    config = DataQualityConfig(
        storage_bronze_path="/tmp/test_bronze",
        storage_silver_init_path="/tmp/test_silver_init"
    )
    pipeline = SilverDataQualityPipeline(config)
    
    # Let it process
    query = pipeline.run()
    time.sleep(30)
    query.stop()
    
    # Verify silver data
    silver = spark_session.read.format("delta").load("/tmp/test_silver_init")
    
    assert silver.count() > 0
    first = silver.first()
    assert first.platform == "ANDROID"  # Uppercase applied
    assert first.country_name == "United States"  # Mapping applied
```

## Best Practices

1. **YAML for Configuration**: Keep transformation logic in code, rules in YAML
2. **Single Responsibility**: Each transformation does one thing
3. **Immutable Operations**: Transformations return new DataFrames
4. **Schema Evolution**: Use `mergeSchema` option for compatible changes
5. **Monitoring**: Track transformation latency and error rates
6. **Testing**: Unit test each transformation independently
7. **Documentation**: Document expected input/output for each transformation
8. **Versioning**: Version transformation rules with schema versions

## Future Enhancements

- [ ] Add data quality metrics (completeness, accuracy)
- [ ] Implement validation rules (e.g., email format, phone numbers)
- [ ] Support complex transformations (regex, calculations)
- [ ] Add transformation chaining and dependencies
- [ ] Implement data masking for PII
- [ ] Support user-defined functions (UDFs)
- [ ] Add transformation performance profiling
- [ ] Implement schema evolution handling
- [ ] Add data quality dashboard
- [ ] Support incremental transformation updates
