# Real-time Aggregation (Gold Layer)

Minute-level streaming aggregations using Spark Structured Streaming that compute real-time metrics from Silver layer data. Provides live analytics for dashboards and monitoring.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│             REAL-TIME AGGREGATION ARCHITECTURE                            │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                   Silver Delta Tables (Input)                             │
│  storage/silver/{init, match, purchase}/                                 │
│  Streaming read from cleaned events                                      │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              Real-time Aggregator (4 Parallel Streams)                    │
│                                                                           │
│  ┌─────────────────────────┐   ┌────────────────────────────────────┐  │
│  │  Global Aggregations    │   │  Country Aggregations              │  │
│  │                          │   │                                     │  │
│  │  • Purchases             │   │  • Purchases by Country            │  │
│  │    - Count               │   │    - Revenue per country           │  │
│  │    - Revenue sum         │   │    - Purchase count per country    │  │
│  │    - Distinct users      │   │                                     │  │
│  │                          │   │  • Matches by Country              │  │
│  │  • Matches               │   │    - Match count per country       │  │
│  │    - Count               │   │                                     │  │
│  │    - Distinct users      │   │                                     │  │
│  └─────────────────────────┘   └────────────────────────────────────┘  │
│                                                                           │
│  All using:                                                               │
│  - 1-minute tumbling windows                                              │
│  - 10-minute watermark for late data                                      │
│  - Append output mode (immutable windows)                                │
└──────────────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌──────────────────────────┐    ┌────────────────────────────┐
│  Global Aggregations     │    │  Country Aggregations      │
│  storage/gold/           │    │  storage/gold/             │
│  - minute_purchases/     │    │  - minute_purchases_by_... │
│  - minute_matches/       │    │  - minute_matches_by_...   │
└──────────────────────────┘    └────────────────────────────┘
```

## Metrics Computed

### 1. Global Purchase Metrics
**Output**: `storage/gold/minute_purchases/`

| Column | Type | Description |
|--------|------|-------------|
| `window_start` | timestamp | Window start time |
| `window_end` | timestamp | Window end time |
| `purchase_count` | long | Total purchases in window |
| `total_revenue` | double | Sum of purchase values |
| `distinct_users` | long | Unique users who purchased |

### 2. Global Match Metrics
**Output**: `storage/gold/minute_matches/`

| Column | Type | Description |
|--------|------|-------------|
| `window_start` | timestamp | Window start time |
| `window_end` | timestamp | Window end time |
| `match_count` | long | Total matches in window |
| `distinct_users` | long | Unique users who played |

### 3. Purchase Metrics by Country
**Output**: `storage/gold/minute_purchases_by_country/`

| Column | Type | Description |
|--------|------|-------------|
| `window_start` | timestamp | Window start time |
| `window_end` | timestamp | Window end time |
| `country_name` | string | Country name |
| `purchase_count` | long | Purchases from this country |
| `country_revenue` | double | Revenue from this country |

### 4. Match Metrics by Country
**Output**: `storage/gold/minute_matches_by_country/`

| Column | Type | Description |
|--------|------|-------------|
| `window_start` | timestamp | Window start time |
| `window_end` | timestamp | Window end time |
| `country_name` | string | Country name |
| `match_count` | long | Matches from this country |

## Components

### 1. Configuration (`config.py`)

**Class**: `RealtimeAggregationConfig`

| Setting | Default | Description |
|---------|---------|-------------|
| `storage_silver_init_path` | `.../silver/init` | Silver init input |
| `storage_silver_match_path` | `.../silver/match` | Silver match input |
| `storage_silver_purchase_path` | `.../silver/purchase` | Silver purchase input |
| `storage_gold_minute_purchases_path` | `.../gold/minute_purchases` | Global purchase output |
| `storage_gold_minute_matches_path` | `.../gold/minute_matches` | Global match output |
| `storage_gold_minute_purchases_by_country_path` | `.../gold/minute_purchases_by_country` | Country purchase output |
| `storage_gold_minute_matches_by_country_path` | `.../gold/minute_matches_by_country` | Country match output |
| `checkpoint_gold_purchases` | `.../checkpoints/gold_purchases` | Purchase checkpoint |
| `checkpoint_gold_matches` | `.../checkpoints/gold_matches` | Match checkpoint |
| `checkpoint_gold_purchases_by_country` | `.../checkpoints/gold_purchases_by_country` | Purchase by country checkpoint |
| `checkpoint_gold_matches_by_country` | `.../checkpoints/gold_matches_by_country` | Match by country checkpoint |
| `streaming_trigger_interval` | `1 minute` | Micro-batch interval |
| `streaming_watermark_delay` | `10 minutes` | Late data tolerance |
| `window_duration` | `1 minute` | Aggregation window size |
| `spark_app_name` | `gold-realtime-aggregation` | Spark application name |

### 2. Aggregator (`aggregator.py`)

**Class**: `RealtimeAggregator`

Main aggregation logic with four methods:

#### `aggregate_purchases() -> StreamingQuery`
Computes global purchase metrics per minute.

**Logic**:
```python
1. Read purchase stream from Silver Delta
2. Add watermark on processing_timestamp (10 minutes)
3. Window by 1-minute tumbling windows
4. Aggregate:
   - COUNT(*) as purchase_count
   - SUM(purchase_value) as total_revenue
   - COUNT(DISTINCT user-id) as distinct_users
5. Select window bounds + aggregates
6. Write to Gold Delta (append mode)
7. Return StreamingQuery
```

#### `aggregate_matches() -> StreamingQuery`
Computes global match metrics per minute.

**Logic**:
```python
1. Read match stream from Silver Delta
2. Add watermark on processing_timestamp
3. Window by 1-minute tumbling windows
4. Aggregate:
   - COUNT(*) as match_count
   - COUNT(DISTINCT user-a) as distinct_users_a
   - COUNT(DISTINCT user-b) as distinct_users_b
   - Calculate total distinct users (a + b)
5. Write to Gold Delta
```

#### `aggregate_purchases_by_country() -> StreamingQuery`
Computes purchase metrics segmented by country.

**Logic**:
```python
1. Read purchase stream
2. Add watermark
3. Window by 1-minute + group by country_name
4. Aggregate per (window, country):
   - COUNT(*) as purchase_count
   - SUM(purchase_value) as country_revenue
5. Write to Gold Delta
```

#### `aggregate_matches_by_country() -> StreamingQuery`
Computes match metrics segmented by country.

**Logic**:
```python
1. Read match stream
2. Add watermark
3. Extract player countries (user_a_country, user_b_country)
4. Union both countries (count each player's match once)
5. Window by 1-minute + group by country_name
6. Aggregate per (window, country):
   - COUNT(*) as match_count
7. Write to Gold Delta
```

### 3. Main Entry Point (`main.py`)

**Function**: `main()`

Starts all four streaming queries in parallel:

```python
1. Load configuration
2. Initialize logger
3. Create Spark session (streaming mode)
4. Create aggregator
5. Start all streams:
   - Global purchases
   - Global matches
   - Purchases by country
   - Matches by country
6. Wait for termination (Ctrl+C to stop)
```

## Running the Job

### Using Quickstart (Optional)

Realtime aggregation is commented out by default in quickstart. To enable:

1. Uncomment in `docker-compose.yml`
2. Run quickstart:
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
    --packages io.delta:delta-spark_2.12:3.2.0 \
    /opt/bitnami/spark/jobs/src/aggregation/realtime/main.py
```

### Local Development

```bash
export STORAGE_SILVER_PURCHASE_PATH=./storage/silver/purchase
export STORAGE_GOLD_MINUTE_PURCHASES_PATH=./storage/gold/minute_purchases
export LOG_LEVEL=INFO

spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  src/aggregation/realtime/main.py
```

## Watermarking & Late Data

### Watermark Configuration

**Watermark delay**: 10 minutes (configurable)

Events arriving more than 10 minutes late are dropped.

**Example**:
```
Current processing time: 2024-01-29 10:15:00
Watermark: 2024-01-29 10:05:00
Event timestamp: 2024-01-29 10:04:00  ❌ Dropped (too late)
Event timestamp: 2024-01-29 10:06:00  ✅ Accepted
```

### Tuning Watermark

**Increase for more completeness** (but higher latency):
```bash
STREAMING_WATERMARK_DELAY=30 minutes
```

**Decrease for lower latency** (but more dropped late data):
```bash
STREAMING_WATERMARK_DELAY=2 minutes
```

## Windowing

### Tumbling Windows

1-minute non-overlapping windows:

```
10:00:00 - 10:01:00  │  Window 1
10:01:00 - 10:02:00  │  Window 2
10:02:00 - 10:03:00  │  Window 3
```

Each event belongs to exactly one window based on its timestamp.

### Window Lifecycle

1. **Open**: Window is accumulating data
2. **Closed**: Window end time reached, no more data accepted (except within watermark)
3. **Finalized**: Watermark passes window end time
4. **Written**: Results written to Delta table (append mode)

## Monitoring

### Streaming Query Status

```python
# Access from StreamingQuery objects
query.lastProgress      # Latest batch metrics
query.status            # Current status
query.recentProgress    # Recent batches
query.isActive          # Is query running?
```

### Spark UI

Monitor at http://localhost:9090
- Input rate per stream
- Processing time per batch
- Watermark lag
- Number of state rows

### Logs

```bash
docker compose -f build/docker-compose.yml logs -f realtime-aggregation

# Sample output
INFO: Starting real-time aggregation application
INFO: Starting global purchase aggregation
INFO: Starting global match aggregation  
INFO: Starting purchase by country aggregation
INFO: Starting match by country aggregation
INFO: All streaming queries started
INFO: Waiting for termination...
```

### Data Validation

```bash
# View minute aggregations
./scripts/visualize-data.sh --table gold-minute-purchases
./scripts/visualize-data.sh --table gold-minute-matches
./scripts/visualize-data.sh --table gold-minute-purchases-by-country
./scripts/visualize-data.sh --table gold-minute-matches-by-country

# Query specific time range
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  -e "SELECT * FROM delta.\`/opt/bitnami/spark/storage/gold/minute_purchases\`
      WHERE window_start >= '2024-01-29 10:00:00'
      ORDER BY window_start DESC
      LIMIT 10"
```

## Performance Tuning

### Increase Throughput

```bash
# Process more frequently
STREAMING_TRIGGER_INTERVAL=30 seconds

# Add more Spark workers
docker-compose scale spark-worker=5
```

### Reduce Latency

```bash
# Continuous processing
STREAMING_TRIGGER_INTERVAL=0 seconds

# Shorter watermark
STREAMING_WATERMARK_DELAY=1 minute
```

### State Management

Stateful aggregations (with distinct counts) require memory for state:

```bash
# Increase executor memory
--executor-memory 4G

# Enable RocksDB state store for large state
--conf spark.sql.streaming.stateStore.providerClass=\
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

## Troubleshooting

### High Latency

**Symptoms**: Windows processed minutes after they close

**Solutions**:
```bash
# Add more workers
docker-compose scale spark-worker=5

# Increase trigger interval
STREAMING_TRIGGER_INTERVAL=2 minutes

# Check Spark UI for bottlenecks
```

### State Store Errors

**Symptoms**: "State store provider class not found" errors

**Solution**: Ensure Delta package is loaded:
```bash
--packages io.delta:delta-spark_2.12:3.2.0
```

### Missing Data

**Check watermark hasn't passed**:
```bash
# View watermark in Spark UI or logs
# If watermark > event timestamp, data is dropped
```

**Solution**: Increase watermark delay:
```bash
STREAMING_WATERMARK_DELAY=30 minutes
```

## Advanced Topics

### Custom Aggregations

Add new metrics by creating additional aggregation methods:

```python
def aggregate_revenue_per_product(self) -> StreamingQuery:
    """Aggregate revenue by product per minute."""
    df = (self.spark
        .readStream
        .format("delta")
        .load(self.config.storage_silver_purchase_path))
    
    df = df.withWatermark("processing_timestamp", "10 minutes")
    
    result = (df
        .groupBy(
            window(col("processing_timestamp"), "1 minute"),
            col("product_name")
        )
        .agg(
            count("*").alias("purchase_count"),
            spark_sum(col("purchase_value")).alias("product_revenue")
        ))
    
    result = result.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_name"),
        col("purchase_count"),
        col("product_revenue")
    )
    
    query = (result
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/path/to/checkpoint")
        .trigger(processingTime="1 minute")
        .start("/path/to/output"))
    
    return query
```

### Sliding Windows

For overlapping windows (e.g., 5-minute windows every 1 minute):

```python
.groupBy(
    window(col("processing_timestamp"), "5 minutes", "1 minute"),
    col("country_name")
)
```

### Multiple Output Modes

- **Append**: Immutable windows (current approach)
- **Update**: Update existing windows (requires complete mode)
- **Complete**: Output entire result table (requires aggregations)

## Best Practices

1. **Watermarking**: Always use watermarks for stateful aggregations
2. **Windowing**: Choose window size based on business requirements
3. **State Management**: Monitor state size and use RocksDB for large state
4. **Checkpointing**: Never delete checkpoints unless reprocessing is acceptable
5. **Testing**: Test with delayed events to verify watermark behavior
6. **Monitoring**: Track watermark lag and processing times
7. **Resource Planning**: Size cluster for peak load, not average

## Future Enhancements

- [ ] Add session windows for user activity
- [ ] Implement sliding windows for moving averages
- [ ] Add complex event processing (CEP) patterns
- [ ] Support multiple aggregation windows (1min, 5min, 15min)
- [ ] Implement late data metrics and monitoring
- [ ] Add state pruning for old windows
- [ ] Support dynamic window durations
- [ ] Add aggregation quality metrics
- [ ] Implement automated alerting on anomalies
- [ ] Add support for out-of-order event handling
