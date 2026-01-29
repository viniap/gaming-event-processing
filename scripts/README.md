# Scripts Directory

This directory contains operational scripts for managing the Gaming Event Processing system.

## Available Scripts

### 🚀 quickstart.sh
**Purpose**: Complete system setup and startup automation

**Usage**:
```bash
./scripts/quickstart.sh
```

**What it does**:
1. Checks Docker availability
2. Creates all required directories with proper permissions
3. Builds Docker images (Spark, Producer)
4. Starts services in correct order:
   - Infrastructure (Kafka, Zookeeper, Spark cluster)
   - Event Producer (generates game events)
   - Bronze Ingestion (3 jobs for init, match, purchase events)
   - Data Quality (Silver layer transformations)
5. Provides status updates and next steps

**Duration**: ~3-4 minutes (includes wait times for service initialization)

**Requirements**: Docker and Docker Compose must be installed and running

---

### 📊 run-daily-aggregation.sh
**Purpose**: Execute batch aggregation job for daily user metrics

**Usage**:
```bash
# Run for today's date
./scripts/run-daily-aggregation.sh

# Run for a specific date
./scripts/run-daily-aggregation.sh 2024-01-15
```

**What it does**:
- Verifies Spark cluster is running
- Runs the batch aggregation job to compute daily distinct users by country and platform
- Saves results to `storage/gold/daily_users/`

**Output**: Gold table with daily aggregated user metrics

---

### 🔍 visualize-data.sh
**Purpose**: Query and display data from Delta tables

**Usage**:
```bash
# Show all tables
./scripts/visualize-data.sh

# Show specific table
./scripts/visualize-data.sh --table bronze
./scripts/visualize-data.sh --table silver-init
./scripts/visualize-data.sh --table gold-daily-users

# Show multiple tables
./scripts/visualize-data.sh --table bronze --table silver-init
```

**Available tables**:
- `bronze` - Raw events from Kafka
- `silver-init` - Cleaned init events
- `silver-match` - Cleaned match events
- `silver-purchase` - Cleaned purchase events
- `gold-daily-users` - Daily aggregated user metrics

**Requirements**: Spark cluster must be running

---

### 🛑 shutdown.sh
**Purpose**: Stop all services and clean up

**Usage**:
```bash
./scripts/shutdown.sh
```

**What it does**:
- Stops all Docker containers
- Removes containers (preserves data in `storage/` directory)
- Optionally can clean up volumes

---

## Typical Workflow

### First-time Setup
```bash
# 1. Clone the repository
git clone <repository-url>
cd gaming-event-processing

# 2. Run quickstart (lift and shift - no manual steps needed!)
./scripts/quickstart.sh

# 3. Wait for services to initialize (~3-4 minutes)
# The script handles everything automatically

# 4. Verify data is flowing
./scripts/visualize-data.sh --table bronze

# 5. Run daily aggregation
./scripts/run-daily-aggregation.sh

# 6. View results
./scripts/visualize-data.sh --table gold-daily-users
```

### Daily Operations
```bash
# Check service status
cd build && docker compose ps

# View logs for a specific service
cd build && docker compose logs -f event-producer
cd build && docker compose logs -f data-quality

# Run daily aggregation
./scripts/run-daily-aggregation.sh

# Visualize results
./scripts/visualize-data.sh
```

### Troubleshooting
```bash
# Restart a specific service
cd build && docker compose restart event-producer

# Stop all services
./scripts/shutdown.sh

# Clean start (remove all data)
./scripts/shutdown.sh
rm -rf storage/bronze/* storage/silver/* storage/gold/*
rm -rf storage/checkpoints/*
./scripts/quickstart.sh
```

## Service Architecture

The quickstart script starts services in this order:

```
1. Infrastructure Layer (0s)
   ├── Zookeeper
   ├── Kafka
   └── Spark (Master + 3 Workers)
   
   [Wait 30s for Kafka readiness]

2. Event Generation (30s)
   └── Event Producer
   
   [Wait 60s for event generation]

3. Bronze Layer (90s)
   ├── Bronze Ingestion - Init Events
   ├── Bronze Ingestion - Match Events
   └── Bronze Ingestion - Purchase Events
   
   [Wait 45s for bronze population]

4. Silver Layer (135s)
   └── Data Quality Transformation
   
   [Wait 45s for silver population]

5. Gold Layer (180s)
   └── Real-time Aggregation (optional, commented out)
```

## Data Flow

```
Kafka Topics → Bronze (raw) → Silver (cleaned) → Gold (aggregated)
     ↓              ↓              ↓                    ↓
init_events    events table   init table        daily_users
match_events                  match table
purchase_events              purchase table
```

## Environment Variables

All configuration is managed through the `.env` file or Docker Compose defaults:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string (default: kafka:9092)
- `EVENT_RATE_PER_SECOND`: Event generation rate (default: 100)
- `LOG_LEVEL`: Logging verbosity (default: INFO)

## Notes

- **Permissions**: The quickstart script automatically handles directory permissions
- **Data Persistence**: All data is stored in `storage/` directory and persists between restarts
- **Checkpoints**: Streaming checkpoints are stored in `storage/checkpoints/`
- **Clean Slate**: To start fresh, delete contents of `storage/` before running quickstart
- **Real-time Aggregation**: Commented out by default; uncomment in docker-compose.yml to enable

## Support

For issues or questions:
1. Check service logs: `cd build && docker compose logs -f [service-name]`
2. Verify service status: `cd build && docker compose ps`
3. Review IMPLEMENTATION.md for detailed architecture documentation
