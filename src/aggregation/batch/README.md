# Batch Aggregation (Gold Layer)

Daily batch aggregation job that computes distinct user metrics by country and platform from Silver layer data. Uses Spark batch processing to generate historical analytics in the Gold layer.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│              BATCH AGGREGATION ARCHITECTURE                       │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                    Silver Delta Table (Input)                     │
│  storage/silver/init/  (Cleaned init events)                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  user-id | timestamp | platform | country | country_name  │  │
│  │  --------|-----------|----------|---------|--------------- │  │
│  │  user123 | 2024...   | ANDROID  | US      | United States │  │
│  │  user456 | 2024...   | IOS      | UK      | United Kingdom│  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Batch Aggregator                               │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  aggregate_daily_users()                                   │  │
│  │                                                             │  │
│  │  1. Read from Silver init table                            │  │
│  │  2. Parse timestamp to date                                │  │
│  │  3. Group by (date, country_name, platform)                │  │
│  │  4. Count distinct users per group                         │  │
│  │  5. Write to Gold Delta table (overwrite per partition)    │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                 Gold Delta Table (Output)                         │
│  storage/gold/daily_users/                                       │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  event_date | country_name   | platform | distinct_users │  │
│  │  -----------|----------------|----------|---------------- │  │
│  │  2024-01-29| United States  | ANDROID  | 342            │  │
│  │  2024-01-29| United States  | IOS      | 198            │  │
│  │  2024-01-29| United Kingdom | ANDROID  | 156            │  │
│  │  2024-01-29| United Kingdom | IOS      | 124            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                   │
│  Partitioned by: event_date                                      │
│  Optimized for: Historical reporting, trend analysis             │
└──────────────────────────────────────────────────────────────────┘
```

## Purpose

Provides daily user engagement metrics for:
- **Historical analysis**: Track user growth over time
- **Geographic insights**: Compare activity by country
- **Platform comparison**: Analyze cross-platform adoption
- **Business reporting**: Daily/weekly/monthly aggregates

## Components

### 1. Configuration (`config.py`)

**Class**: `BatchAggregationConfig`

| Setting | Default | Description |
|---------|---------|-------------|
| `storage_silver_init_path` | `.../silver/init` | Silver init events input |
| `storage_gold_daily_users_path` | `.../gold/daily_users` | Gold output path |
| `spark_app_name` | `batch-daily-aggregation` | Spark application name |
| `spark_log_level` | `WARN` | Spark logging level |
| `log_level` | `INFO` | Application logging level |

### 2. Aggregator (`aggregator.py`)

**Class**: `BatchAggregator`

Main aggregation logic.

**Method**: `aggregate_daily_users(date: Optional[str] = None) -> None`

Computes daily distinct users by country and platform.

**Parameters**:
- `date`: Optional date string (YYYY-MM-DD). Defaults to today.

**Logic**:
```python
1. Read init events from Silver Delta table
2. Filter by specified date (if provided)
3. Parse timestamp to date
4. Group by: (event_date, country_name, platform)
5. Aggregate: COUNT(DISTINCT user-id) as distinct_users
6. Write to Gold Delta table
   - Mode: overwrite (idempotent for reprocessing)
   - Partition: by event_date (efficient date-based queries)
```

**SQL Equivalent**:
```sql
SELECT 
    DATE(timestamp) as event_date,
    country_name,
    platform,
    COUNT(DISTINCT `user-id`) as distinct_users
FROM silver.init
WHERE DATE(timestamp) = '2024-01-29'  -- Optional filter
GROUP BY 
    DATE(timestamp),
    country_name,
    platform
```

### 3. Main Entry Point (`main.py`)

**Function**: `main(date: Optional[str] = None)`

Entry point for batch job.

**Workflow**:
1. Load configuration
2. Initialize logger
3. Create Spark session (batch mode)
4. Create aggregator instance
5. Run aggregation for specified date
6. Stop Spark session

**Usage**:
```python
# Run for today
main()

# Run for specific date
main(date="2024-01-15")
```

## Running the Job

### Using Script (Recommended)

```bash
# Run for today
./scripts/run-daily-aggregation.sh

# Run for specific date
./scripts/run-daily-aggregation.sh 2024-01-15
```

### Manual Execution

```bash
# Today's data
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    /opt/bitnami/spark/jobs/src/aggregation/batch/main.py

# Specific date
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  spark-master \
  /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    /opt/bitnami/spark/jobs/src/aggregation/batch/main.py \
    --date 2024-01-15
```

### Local Development

```bash
export STORAGE_SILVER_INIT_PATH=./storage/silver/init
export STORAGE_GOLD_DAILY_USERS_PATH=./storage/gold/daily_users
export LOG_LEVEL=INFO

spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  src/aggregation/batch/main.py \
  --date 2024-01-29
```

## Output Schema

**Gold Table**: `storage/gold/daily_users/`

| Column | Type | Description |
|--------|------|-------------|
| `event_date` | date | Date of activity (YYYY-MM-DD) |
| `country_name` | string | Country name (e.g., "United States") |
| `platform` | string | Platform (ANDROID, IOS, WEB) |
| `distinct_users` | long | Count of unique users |

**Partitioning**: By `event_date` for efficient date-range queries

**Sample Data**:
```
event_date  | country_name   | platform | distinct_users
------------|----------------|----------|---------------
2024-01-29  | United States  | ANDROID  | 342
2024-01-29  | United States  | IOS      | 198
2024-01-29  | United States  | WEB      | 87
2024-01-29  | United Kingdom | ANDROID  | 156
2024-01-29  | United Kingdom | IOS      | 124
```

## Scheduling

### Daily Cron Job

```bash
# Run every day at 2 AM
0 2 * * * /path/to/scripts/run-daily-aggregation.sh >> /var/log/daily-agg.log 2>&1
```

### Airflow DAG

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_user_aggregation',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=True  # Backfill missing dates
)

aggregate_task = BashOperator(
    task_id='aggregate_daily_users',
    bash_command='/path/to/scripts/run-daily-aggregation.sh {{ ds }}',
    dag=dag
)
```

## Monitoring

### Logs

```bash
# View aggregation logs
docker compose -f build/docker-compose.yml logs -f | grep batch-daily

# Check Spark UI
open http://localhost:9090

# View history
open http://localhost:18080
```

### Data Validation

```bash
# Check output
./scripts/visualize-data.sh --table gold-daily-users

# Query specific date
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  -e "SELECT * FROM delta.\`/opt/bitnami/spark/storage/gold/daily_users\`
      WHERE event_date = '2024-01-29'
      ORDER BY distinct_users DESC"

# Count records per date
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  -e "SELECT event_date, COUNT(*) as records
      FROM delta.\`/opt/bitnami/spark/storage/gold/daily_users\`
      GROUP BY event_date
      ORDER BY event_date DESC"
```

## Performance Considerations

### Data Volume

Typical metrics (assuming 100 events/sec):
- **Daily events**: ~8.6M events
- **Silver init**: ~30% = 2.6M records
- **Gold output**: ~30-50 rows (countries × platforms)

Aggregation reduces data by ~50,000x!

### Optimization Strategies

#### 1. Partition Pruning

Job only reads relevant date partition:
```python
df = spark.read.format("delta").load(silver_path)
df = df.filter(col("event_date") == date)  # Partition pruning
```

#### 2. Predicate Pushdown

Delta Lake pushes filters to parquet readers, skipping irrelevant files.

#### 3. Caching (for large date ranges)

```python
df = spark.read.format("delta").load(silver_path)
df = df.filter((col("event_date") >= start_date) & (col("event_date") <= end_date))
df.cache()  # Cache for multiple aggregations
```

#### 4. Repartitioning

For very large aggregations:
```python
df = df.repartition(200, "country_name", "platform")
```

### Execution Time

Typical execution times:
- **1 day**: ~10-30 seconds
- **1 week**: ~1-2 minutes
- **1 month**: ~5-10 minutes
- **1 year**: ~30-60 minutes (with caching)

## Backfilling

### Backfill Date Range

```bash
# Bash script to backfill
for date in $(seq -f "2024-01-%02g" 1 31); do
    echo "Processing $date"
    ./scripts/run-daily-aggregation.sh $date
    sleep 10
done
```

### Parallel Backfill

```bash
# Using GNU parallel
seq -f "2024-01-%02g" 1 31 | \
  parallel -j 4 ./scripts/run-daily-aggregation.sh {}
```

### Airflow Backfill

```bash
# Backfill with Airflow
airflow dags backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  daily_user_aggregation
```

## Troubleshooting

### No Data in Output

**Check Silver has data**:
```bash
./scripts/visualize-data.sh --table silver-init
```

**Verify date filter**:
```bash
docker exec spark-master spark-sql \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  -e "SELECT DATE(timestamp) as date, COUNT(*)
      FROM delta.\`/opt/bitnami/spark/storage/silver/init\`
      GROUP BY DATE(timestamp)
      ORDER BY date DESC
      LIMIT 10"
```

### Duplicate Records

If seeing duplicates, check write mode:
```python
# Should be overwrite for idempotency
df.write.mode("overwrite").save(gold_path)
```

### Performance Issues

**Check Spark resources**:
```bash
# Add more workers
docker-compose scale spark-worker=5

# Increase executor memory
--executor-memory 4G
```

**Enable Delta optimizations**:
```sql
ALTER TABLE delta.`/opt/bitnami/spark/storage/gold/daily_users`
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

## Advanced Usage

### Multiple Aggregations

Compute additional metrics in one job:

```python
def aggregate_daily_users(self, date: Optional[str] = None) -> None:
    """Compute multiple daily metrics."""
    df = self.spark.read.format("delta").load(self.config.storage_silver_init_path)
    
    if date:
        df = df.filter(to_date(col("timestamp")) == date)
    
    # Metric 1: Distinct users by country and platform
    users_by_country_platform = (df
        .groupBy(
            to_date(col("timestamp")).alias("event_date"),
            col("country_name"),
            col("platform")
        )
        .agg(countDistinct(col("user-id")).alias("distinct_users")))
    
    users_by_country_platform.write\
        .mode("overwrite")\
        .partitionBy("event_date")\
        .format("delta")\
        .save(self.config.storage_gold_daily_users_path)
    
    # Metric 2: Total users by country
    users_by_country = (df
        .groupBy(
            to_date(col("timestamp")).alias("event_date"),
            col("country_name")
        )
        .agg(countDistinct(col("user-id")).alias("distinct_users")))
    
    users_by_country.write\
        .mode("overwrite")\
        .partitionBy("event_date")\
        .format("delta")\
        .save(f"{base_path}/daily_users_by_country")
```

### Custom Date Ranges

```python
def aggregate_date_range(
    self,
    start_date: str,
    end_date: str
) -> None:
    """Aggregate over date range."""
    df = self.spark.read.format("delta").load(self.config.storage_silver_init_path)
    
    df = df.filter(
        (to_date(col("timestamp")) >= start_date) &
        (to_date(col("timestamp")) <= end_date)
    )
    
    result = (df
        .groupBy(
            to_date(col("timestamp")).alias("event_date"),
            col("country_name"),
            col("platform")
        )
        .agg(countDistinct(col("user-id")).alias("distinct_users")))
    
    result.write\
        .mode("append")\
        .partitionBy("event_date")\
        .format("delta")\
        .save(self.config.storage_gold_daily_users_path)
```

## Testing

### Unit Tests

```python
def test_daily_aggregation(spark_session):
    """Test daily user aggregation logic."""
    # Create test data
    data = [
        ("user1", "2024-01-29T10:00:00", "ANDROID", "US", "United States"),
        ("user1", "2024-01-29T11:00:00", "ANDROID", "US", "United States"),  # Duplicate
        ("user2", "2024-01-29T10:30:00", "IOS", "US", "United States"),
        ("user3", "2024-01-29T12:00:00", "ANDROID", "UK", "United Kingdom"),
    ]
    
    df = spark_session.createDataFrame(data, ["user-id", "timestamp", "platform", "country", "country_name"])
    df.write.format("delta").mode("overwrite").save("/tmp/test_silver_init")
    
    # Run aggregation
    config = BatchAggregationConfig(
        storage_silver_init_path="/tmp/test_silver_init",
        storage_gold_daily_users_path="/tmp/test_gold_daily"
    )
    aggregator = BatchAggregator(spark_session, config)
    aggregator.aggregate_daily_users(date="2024-01-29")
    
    # Verify results
    result = spark_session.read.format("delta").load("/tmp/test_gold_daily")
    
    assert result.count() == 3  # 3 unique combinations
    
    us_android = result.filter(
        (col("country_name") == "United States") &
        (col("platform") == "ANDROID")
    ).first()
    
    assert us_android.distinct_users == 1  # user1 counted once despite 2 events
```

## Best Practices

1. **Idempotent Writes**: Use overwrite mode for reprocessability
2. **Partition by Date**: Enables efficient date-range queries
3. **Distinct Counts**: Use `countDistinct` for accurate user counts
4. **Error Handling**: Log failures and implement retries
5. **Data Validation**: Verify output row counts and aggregates
6. **Monitoring**: Track execution time and data volumes
7. **Documentation**: Document aggregation logic and business rules

## Future Enhancements

- [ ] Add more metrics (retention, cohort analysis)
- [ ] Implement weekly/monthly rollups
- [ ] Add data quality checks
- [ ] Support custom time zones
- [ ] Implement incremental aggregations
- [ ] Add metric validation and alerts
- [ ] Support multiple event types in one job
- [ ] Add aggregation history tracking
