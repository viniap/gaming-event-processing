# Event Producer

Real-time gaming event generator that creates synthetic events using the Faker library and publishes them to Kafka topics. Implements schema validation and configurable event distribution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    EVENT PRODUCER ARCHITECTURE                   │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐
│  Configuration   │
│   (Pydantic)     │──▶ Validates settings, loads schema files
└──────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Event Generators                             │
│  ┌────────────┐   ┌────────────┐   ┌──────────────────┐         │
│  │   Init     │   │   Match    │   │   In-App         │         │
│  │ Generator  │   │ Generator  │   │   Purchase       │         │
│  │            │   │            │   │   Generator      │         │
│  └────────────┘   └────────────┘   └──────────────────┘         │
│        │                 │                    │                   │
│        ▼                 ▼                    ▼                   │
│   Faker Library generates realistic data                         │
│   - User IDs, timestamps, platforms, countries                   │
│   - Match details, winners, tiers                                │
│   - Product IDs, purchase values, currencies                     │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Schema Validator                               │
│  - Loads JSON schemas from schemas/ directory                    │
│  - Validates each event against its schema                       │
│  - Ensures data quality at source                                │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Event Distributor                              │
│  - Weighted random selection based on probabilities              │
│  - Maintains event type balance (init, match, purchase)          │
│  - Configurable distribution via environment variables           │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Kafka Publisher                                │
│  ┌──────────────────┐   ┌──────────────────┐                    │
│  │  JSON Serializer │   │  Delivery Report │                    │
│  │  - UTF-8 encode  │   │  - Ack tracking  │                    │
│  │  - Compact JSON  │   │  - Error logging │                    │
│  └──────────────────┘   └──────────────────┘                    │
└──────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Kafka Topics                               │
│  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐     │
│  │init_events  │   │match_events  │   │purchase_events   │     │
│  └─────────────┘   └──────────────┘   └──────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Core Configuration (`core/config.py`)

**Class**: `ProducerConfig`

Manages all producer settings using Pydantic Settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `kafka_bootstrap_servers` | `localhost:29092` | Kafka broker connection |
| `init_topic` | `init_events` | Topic for init events |
| `match_topic` | `match_events` | Topic for match events |
| `purchase_topic` | `purchase_events` | Topic for purchase events |
| `event_rate_per_second` | `100` | Target event generation rate |
| `batch_size` | `10` | Events per batch |
| `init_probability` | `0.3` | Relative weight for init events |
| `match_probability` | `0.5` | Relative weight for match events |
| `purchase_probability` | `0.2` | Relative weight for purchase events |
| `users_pool_size` | `1000` | Unique user pool size |
| `schema_dir` | `schemas` | JSON schema directory |

### 2. Event Generators (`generation/`)

#### InitEventGenerator

Generates player app initialization events.

**Fields**:
- `user-id`: Random user from pool
- `timestamp`: Current UTC time (ISO 8601)
- `platform`: Random from [android, ios, web]
- `country`: Random country code (US, UK, BR, etc.)
- `device`: Random device model
- `version`: App version (e.g., 5.1.0)

**Schema**: [schemas/init.json](../../schemas/init.json)

#### MatchEventGenerator

Generates completed match events between two players.

**Fields**:
- `user-a`, `user-b`: Two different users from pool
- `winner`: Either user-a or user-b
- `timestamp`: Current UTC time (ISO 8601)
- `game-tier`: Random from [beginner, intermediate, advanced, expert]
- `user_a_platform`, `user_b_platform`: Platform for each player
- `user_a_country`, `user_b_country`: Country for each player

**Schema**: [schemas/match.json](../../schemas/match.json)

#### InAppPurchaseEventGenerator

Generates in-app purchase events.

**Fields**:
- `user-id`: Random user from pool
- `timestamp`: Current UTC time (ISO 8601)
- `product-id`: Random from [coins_100, coins_500, coins_1000, premium_cue, vip_pass]
- `purchase_value`: Realistic price for product ($0.99 - $99.99)
- `currency`: Random from [USD, EUR, GBP, BRL]
- `country`: Random country code

**Schema**: [schemas/in-app-purchase.json](../../schemas/in-app-purchase.json)

### 3. Schema Validation (`validation/schema_validator.py`)

**Class**: `SchemaValidator`

- Loads JSON schemas from `schemas/` directory
- Validates events against JSON Schema Draft 7
- Raises `jsonschema.ValidationError` for invalid events
- Ensures data quality at source

**Methods**:
- `validate_init(event)`: Validate init event
- `validate_match(event)`: Validate match event
- `validate_purchase(event)`: Validate purchase event

### 4. Kafka Publisher (`kafka/producer.py`)

**Class**: `EventProducer`

Publishes events to Kafka with delivery guarantees.

**Features**:
- JSON serialization to UTF-8 bytes
- Configurable acknowledgment mode (acks=all)
- Automatic retries on failure
- Delivery callbacks for monitoring
- Per-topic routing

**Methods**:
- `publish_init(event)`: Publish to init_events topic
- `publish_match(event)`: Publish to match_events topic
- `publish_purchase(event)`: Publish to purchase_events topic
- `flush()`: Wait for all pending messages

### 5. Main Orchestrator (`main.py`)

**Function**: `main()`

Coordinates the event generation loop:

1. Load configuration
2. Initialize logger
3. Create Kafka producer
4. Initialize event generators
5. Initialize schema validator
6. Run generation loop:
   - Select event type based on probabilities
   - Generate event with Faker
   - Validate against schema
   - Publish to Kafka
   - Sleep to maintain target rate

## Data Generation Strategy

### User Pool

A fixed pool of user IDs is maintained to create realistic patterns:
- Default size: 1,000 users
- IDs generated using Faker's UUID library
- Consistent across all event types
- Enables realistic user behavior analysis

### Event Distribution

Events are generated with weighted probabilities:

```python
# Example configuration
INIT_PROBABILITY=0.3      # 30% init events
MATCH_PROBABILITY=0.5     # 50% match events  
PURCHASE_PROBABILITY=0.2  # 20% purchase events
```

Probabilities are normalized automatically, so they represent relative weights.

### Rate Control

Target rate: `EVENT_RATE_PER_SECOND` (default: 100)

Actual implementation:
```python
batch_size = 10
events_per_batch = 10
sleep_time = batch_size / event_rate_per_second  # 0.1 seconds
```

Generates events in batches with sleep intervals to maintain consistent rate.

## Running the Producer

### Using Docker Compose (Recommended)

The producer runs automatically when started via quickstart:

```bash
./scripts/quickstart.sh
```

Check logs:
```bash
docker compose -f build/docker-compose.yml logs -f event-producer
```

### Manual Execution

```bash
# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092
export EVENT_RATE_PER_SECOND=100
export USERS_POOL_SIZE=1000
export INIT_PROBABILITY=0.3
export MATCH_PROBABILITY=0.5
export PURCHASE_PROBABILITY=0.2

# Run producer
python -m src.producer.main
```

## Configuration

### Environment Variables

All settings can be overridden via environment variables:

```bash
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
INIT_TOPIC=init_events
MATCH_TOPIC=match_events
PURCHASE_TOPIC=purchase_events
KAFKA_ACKS=all
KAFKA_RETRIES=3
KAFKA_COMPRESSION_TYPE=gzip

# Generation settings
EVENT_RATE_PER_SECOND=100
BATCH_SIZE=10

# Event probabilities (relative weights)
INIT_PROBABILITY=0.3
MATCH_PROBABILITY=0.5
PURCHASE_PROBABILITY=0.2

# Simulation settings
USERS_POOL_SIZE=1000

# Paths
SCHEMA_DIR=schemas

# Logging
LOG_LEVEL=INFO
```

### Configuration File

Create a `.env` file in the project root:

```bash
cp build/.env.example .env
# Edit .env with your settings
```

## Monitoring

### Logs

The producer logs key events:

```
INFO: Loaded schema for init events from schemas/init.json
INFO: Loaded schema for match events from schemas/match.json
INFO: Loaded schema for purchase events from schemas/in-app-purchase.json
INFO: User pool initialized with 1000 users
INFO: Starting event generation at 100 events/second
INFO: Published init event for user 123e4567-e89b-12d3-a456-426614174000 to init_events
INFO: Published match event between user-a and user-b to match_events
INFO: Published purchase event for user abc123 ($4.99) to purchase_events
```

### Metrics

Key metrics to monitor:

- **Generation rate**: Events per second
- **Event distribution**: Percentage by type
- **Validation failures**: Schema validation errors
- **Kafka errors**: Failed deliveries, retries
- **Latency**: Time from generation to Kafka ack

### Verification

Check that events are reaching Kafka:

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume from topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic init_events \
  --from-beginning \
  --max-messages 10
```

## Error Handling

### Schema Validation Failures

If an event fails validation:
1. Error is logged with details
2. Event is skipped (not published)
3. Counter incremented for monitoring
4. Generation continues

### Kafka Publishing Failures

If Kafka delivery fails:
1. Producer retries automatically (configurable)
2. Delivery callback logs failure
3. After max retries, error is logged
4. Generation continues

### Connection Issues

If Kafka is unavailable:
1. Producer raises exception
2. Container health check fails
3. Docker Compose can restart service
4. Exponential backoff recommended for production

## Performance Tuning

### Increase Throughput

```bash
EVENT_RATE_PER_SECOND=1000  # 10x increase
BATCH_SIZE=100              # Larger batches
```

### Reduce Latency

```bash
KAFKA_ACKS=1               # Only wait for leader
BATCH_SIZE=1               # Immediate send
```

### Optimize Kafka

```bash
KAFKA_COMPRESSION_TYPE=lz4  # Fast compression
KAFKA_RETRIES=0             # No retries (at-most-once)
```

## Design Patterns

### Strategy Pattern

Event generators implement a common interface:
```python
class EventGenerator(ABC):
    @abstractmethod
    def generate(self) -> Dict[str, Any]:
        pass
```

### Dependency Injection

Configuration and dependencies injected via constructor:
```python
producer = EventProducer(config)
generator = InitEventGenerator(config, faker, user_pool)
```

### Factory Pattern

Generators created based on event type:
```python
def create_generator(event_type: str) -> EventGenerator:
    if event_type == "init":
        return InitEventGenerator(...)
    # ...
```

## Testing

### Unit Tests

Test individual components:
```python
# Test event generation
generator = InitEventGenerator(config, faker, users)
event = generator.generate()
assert "user-id" in event
assert event["platform"] in ["android", "ios", "web"]

# Test schema validation
validator = SchemaValidator("schemas")
validator.validate_init(event)  # Should not raise
```

### Integration Tests

Test end-to-end flow:
```python
# Generate and publish event
event = generator.generate()
validator.validate_init(event)
producer.publish_init(event)
producer.flush()

# Verify in Kafka (using kafka-python)
consumer = KafkaConsumer('init_events')
message = next(consumer)
assert json.loads(message.value) == event
```

## Troubleshooting

### No Events Generated

Check logs:
```bash
docker compose -f build/docker-compose.yml logs event-producer
```

Common issues:
- Kafka not reachable: Check `KAFKA_BOOTSTRAP_SERVERS`
- Schema files not found: Verify `SCHEMA_DIR` path
- Permission errors: Ensure read access to schemas/

### Events Not Reaching Kafka

Verify Kafka connectivity:
```bash
# Check Kafka is running
docker compose -f build/docker-compose.yml ps kafka

# Test connection
docker exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

### Low Event Rate

Check CPU and network:
```bash
# Monitor container resources
docker stats event-producer

# Increase batch size
# Reduce sleep time between batches
```

## Future Enhancements

- [ ] Add metrics export (Prometheus)
- [ ] Implement event replay from file
- [ ] Add burst mode for stress testing
- [ ] Support custom event schemas
- [ ] Implement event correlation (e.g., purchase after match)
- [ ] Add realistic user behavior patterns
- [ ] Support multiple concurrent sessions per user
- [ ] Implement dead letter queue for failed events
