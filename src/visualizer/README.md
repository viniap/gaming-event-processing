# Data Visualizer

Command-line tool for querying and displaying data from all Delta Lake tables across the medallion architecture. Provides quick data inspection and validation capabilities.

## Overview

The visualizer is a CLI application that:
- Queries Delta tables using Spark SQL
- Displays formatted summaries with statistics
- Supports viewing all layers (Bronze, Silver, Gold)
- Enables targeted table selection via command-line arguments

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Data Visualizer                            │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  main()                                              │   │
│  │  1. Parse command-line arguments                     │   │
│  │  2. Load configuration                               │   │
│  │  3. Create Spark session (batch mode)               │   │
│  │  4. Query selected tables                            │   │
│  │  5. Display formatted results                        │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  Query Functions (9 total):                                  │
│  • query_bronze_table()                                      │
│  • query_silver_init_table()                                 │
│  • query_silver_match_table()                                │
│  • query_silver_purchase_table()                             │
│  • query_gold_daily_users_table()                            │
│  • query_gold_minute_purchases_table()                       │
│  • query_gold_minute_matches_table()                         │
│  • query_gold_minute_purchases_by_country_table()            │
│  • query_gold_minute_matches_by_country_table()              │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│              Delta Lake Tables (All Layers)                   │
│  • storage/bronze/events/                                    │
│  • storage/silver/init/, match/, purchase/                   │
│  • storage/gold/daily_users/                                 │
│  • storage/gold/minute_purchases/, minute_matches/, ...      │
└──────────────────────────────────────────────────────────────┘
```

## Features

### Bronze Layer Visualization
- Total record count
- Event distribution by topic
- Sample records with timestamps

### Silver Layer Visualization
- Total event count by type
- Platform distribution (shows UPPERCASE transformations)
- Country distribution (shows mapped country names)
- Revenue analysis (for purchases)
- Sample records showing transformations

### Gold Layer Visualization
- Daily user metrics by country and platform
- Minute-level purchase/match aggregations
- Top countries by revenue/activity
- Recent aggregation windows

## Usage

### View All Tables

```bash
./scripts/visualize-data.sh
# or
./scripts/visualize-data.sh --all
```

### View Specific Table

```bash
# Bronze layer
./scripts/visualize-data.sh --table bronze

# Silver layer
./scripts/visualize-data.sh --table silver-init
./scripts/visualize-data.sh --table silver-match
./scripts/visualize-data.sh --table silver-purchase

# Gold layer
./scripts/visualize-data.sh --table gold-daily-users
./scripts/visualize-data.sh --table gold-minute-purchases
./scripts/visualize-data.sh --table gold-minute-matches
./scripts/visualize-data.sh --table gold-minute-purchases-by-country
./scripts/visualize-data.sh --table gold-minute-matches-by-country
```

### View Multiple Tables

```bash
./scripts/visualize-data.sh --table bronze --table silver-init --table gold-daily-users
```

### Direct Python Execution

```bash
cd build
docker exec -it --user 1000:1000 \
  -e PYTHONPATH=/opt/bitnami/spark/jobs \
  spark-master \
  python /opt/bitnami/spark/jobs/src/visualizer/main.py --table bronze
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_BRONZE_PATH` | `/opt/bitnami/spark/storage/bronze/events` | Bronze table location |
| `STORAGE_SILVER_INIT_PATH` | `.../silver/init` | Silver init location |
| `STORAGE_SILVER_MATCH_PATH` | `.../silver/match` | Silver match location |
| `STORAGE_SILVER_PURCHASE_PATH` | `.../silver/purchase` | Silver purchase location |
| `STORAGE_GOLD_DAILY_USERS_PATH` | `.../gold/daily_users` | Gold daily users location |
| `STORAGE_GOLD_MINUTE_PURCHASES_PATH` | `.../gold/minute_purchases` | Gold minute purchases |
| `STORAGE_GOLD_MINUTE_MATCHES_PATH` | `.../gold/minute_matches` | Gold minute matches |
| `STORAGE_GOLD_MINUTE_PURCHASES_BY_COUNTRY_PATH` | `.../gold/minute_purchases_by_country` | Gold purchases by country |
| `STORAGE_GOLD_MINUTE_MATCHES_BY_COUNTRY_PATH` | `.../gold/minute_matches_by_country` | Gold matches by country |
| `NUM_ROWS` | `10` | Number of sample rows to display |
| `SPARK_APP_NAME` | `data-visualizer` | Spark application name |
| `SPARK_LOG_LEVEL` | `WARN` | Spark logging level |
| `LOG_LEVEL` | `INFO` | Application logging level |

## Sample Output

### Bronze Layer

```
================================================================================
  BRONZE LAYER - Raw Events
================================================================================

📊 Total Records: 1,234,567

📈 Events by Topic:
+----------------+-------+
|topic           |count  |
+----------------+-------+
|init_events     |370,370|
|match_events    |617,283|
|purchase_events |246,914|
+----------------+-------+

🔍 Sample Records (showing 10):
+----------------+-------------------+---------------------+
|topic           |kafka_timestamp    |ingestion_timestamp  |
+----------------+-------------------+---------------------+
|init_events     |2024-01-29 10:...  |2024-01-29 10:...   |
|match_events    |2024-01-29 10:...  |2024-01-29 10:...   |
+----------------+-------------------+---------------------+
```

### Silver Layer (Init Events)

```
================================================================================
  SILVER LAYER - Init Events
================================================================================

📊 Total Init Events: 370,370

📈 Events by Platform (after transformation):
+---------+------+
|platform |count |
+---------+------+
|ANDROID  |185,185|
|IOS      |123,457|
|WEB      |61,728 |
+---------+------+

🌍 Events by Country Name (after mapping):
+---------------+------+
|country_name   |count |
+---------------+------+
|United States  |92,592|
|United Kingdom |46,296|
|Brazil         |37,037|
|Germany        |27,778|
+---------------+------+

🔍 Sample Records (showing 10):
+-------+---------+---------+--------------+---------------------+
|user-id|platform |country  |country_name  |processing_timestamp |
+-------+---------+---------+--------------+---------------------+
|user123|ANDROID  |US       |United States |2024-01-29 10:00:... |
+-------+---------+---------+--------------+---------------------+
```

### Gold Layer (Daily Users)

```
================================================================================
  GOLD LAYER - Daily Aggregated Users
================================================================================

📊 Total Daily Records: 42

📈 Recent Daily User Counts:
+------------+---------------+---------+---------------+
|event_date  |country_name   |platform |distinct_users |
+------------+---------------+---------+---------------+
|2024-01-29  |United States  |ANDROID  |342            |
|2024-01-29  |United States  |IOS      |198            |
|2024-01-29  |United States  |WEB      |87             |
|2024-01-29  |United Kingdom |ANDROID  |156            |
+------------+---------------+---------+---------------+
```

## Components

### 1. Configuration (`config.py`)

**Class**: `VisualizerConfig`

Pydantic-based configuration with all Delta table paths and display settings.

### 2. Query Functions (`main.py`)

Each function follows the same pattern:
1. Check if table exists
2. Read from Delta format
3. Compute summary statistics
4. Display formatted results
5. Handle errors gracefully

**Functions**:
- `query_bronze_table()`: Raw events from Kafka
- `query_silver_init_table()`: Cleaned init events
- `query_silver_match_table()`: Cleaned match events
- `query_silver_purchase_table()`: Cleaned purchase events
- `query_gold_daily_users_table()`: Daily aggregated users
- `query_gold_minute_purchases_table()`: Minute purchase metrics
- `query_gold_minute_matches_table()`: Minute match metrics
- `query_gold_minute_purchases_by_country_table()`: Country purchase metrics
- `query_gold_minute_matches_by_country_table()`: Country match metrics

### 3. Argument Parser (`parse_arguments()`)

Handles command-line argument parsing with support for:
- `--all`: Show all tables (default)
- `--table <name>`: Show specific table (repeatable)

### 4. Main Orchestrator (`main()`)

Coordinates the visualization workflow:
1. Parse arguments
2. Load configuration
3. Initialize logger
4. Create Spark session
5. Execute selected queries
6. Stop Spark session

## Verification Use Cases

### 1. Pipeline Health Check

```bash
# Quick check all layers
./scripts/visualize-data.sh
```

Verifies:
- Data flowing through all layers
- Transformations applied correctly
- Aggregations running

### 2. Transformation Validation

```bash
# Check uppercase transformation
./scripts/visualize-data.sh --table silver-init

# Verify platforms are UPPERCASE (ANDROID, IOS, WEB)
```

### 3. Mapping Validation

```bash
# Check country mapping
./scripts/visualize-data.sh --table silver-init

# Verify country_name populated from country codes
```

### 4. Aggregation Validation

```bash
# Check daily aggregation
./scripts/visualize-data.sh --table gold-daily-users

# Verify distinct_users counts make sense
```

### 5. Data Freshness

```bash
# Check latest minute aggregations
./scripts/visualize-data.sh --table gold-minute-purchases

# Verify recent window_start timestamps
```

## Troubleshooting

### Table Not Found

**Error**: `❌ Bronze table not found at: ...`

**Solutions**:
```bash
# Check if data exists
docker exec spark-master ls -la /opt/bitnami/spark/storage/bronze/events/

# Verify ingestion is running
docker compose -f build/docker-compose.yml ps bronze-ingestion-init

# Check ingestion logs
docker compose -f build/docker-compose.yml logs bronze-ingestion-init
```

### Spark Errors

**Error**: `org.apache.spark.sql.AnalysisException`

**Solutions**:
```bash
# Verify Spark is running
docker compose -f build/docker-compose.yml ps spark-master

# Check Spark logs
docker compose -f build/docker-compose.yml logs spark-master

# Restart Spark if needed
docker compose -f build/docker-compose.yml restart spark-master
```

### Empty Results

If tables exist but show 0 records:

```bash
# Check producer is running
docker compose -f build/docker-compose.yml ps event-producer

# Verify data generation
docker compose -f build/docker-compose.yml logs event-producer | tail -20

# Check Kafka has data
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic init_events \
  --max-messages 5
```

## Extending the Visualizer

### Add New Table

1. Add path to `config.py`:
```python
storage_new_table_path: str = Field(
    default="/opt/bitnami/spark/storage/new/table",
    description="New table path"
)
```

2. Create query function in `main.py`:
```python
def query_new_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display new table."""
    print_separator("NEW TABLE - Description")
    
    try:
        if not os.path.exists(config.storage_new_table_path.replace(...)):
            print(f"❌ New table not found")
            return
        
        df = spark.read.format("delta").load(config.storage_new_table_path)
        
        print(f"\n📊 Total Records: {df.count():,}")
        
        # Add custom visualizations
        
    except Exception as e:
        print(f"❌ Error: {e}")
```

3. Register in `parse_arguments()`:
```python
choices=[
    "bronze",
    # ... existing choices
    "new-table"  # Add this
]
```

4. Add to table functions dict in `main()`:
```python
table_functions = {
    # ... existing functions
    "new-table": query_new_table
}
```

### Custom Visualizations

Add custom analysis to query functions:

```python
# Top users by activity
print("\n👥 Top Users by Activity:")
top_users = (df
    .groupBy("user-id")
    .count()
    .orderBy(col("count").desc())
    .limit(10))
top_users.show(truncate=False)

# Time series
print("\n📅 Activity Over Time:")
time_series = (df
    .groupBy(window(col("timestamp"), "1 hour"))
    .count()
    .orderBy("window"))
time_series.show(20, truncate=False)
```

## Best Practices

1. **Read-only**: Visualizer never writes data, only reads
2. **Error Handling**: Gracefully handle missing tables
3. **Performance**: Use `.show()` with limits to avoid large scans
4. **Formatting**: Use emojis and formatting for readability
5. **Context**: Show both aggregates and samples
6. **Documentation**: Document each visualization's purpose

## Alternative Visualization Tools

For production use, consider:

### Jupyter Notebooks
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("analysis").getOrCreate()
df = spark.read.format("delta").load("/path/to/table")
df.toPandas().plot()  # Use pandas/matplotlib
```

### BI Tools
- **Tableau**: Connect to Delta Lake via Spark JDBC
- **Power BI**: Use Delta Lake connector
- **Superset**: Direct Spark SQL queries

### Dashboards
- **Grafana**: Visualize metrics over time
- **Streamlit**: Build custom Python dashboards
- **Plotly Dash**: Interactive visualizations

## Future Enhancements

- [ ] Add export to CSV/JSON
- [ ] Support time range filtering
- [ ] Add aggregation comparisons
- [ ] Implement data quality checks
- [ ] Add chart generation (matplotlib)
- [ ] Support custom SQL queries
- [ ] Add data profiling statistics
- [ ] Implement caching for large tables
- [ ] Add interactive mode (REPL)
- [ ] Support output formatting (JSON, table, etc.)
