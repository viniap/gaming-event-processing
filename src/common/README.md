# Common Utilities

Shared utilities and helper modules used across all components of the gaming event processing system. Provides consistent logging, Spark session management, and configuration patterns.

## Overview

The common module provides:
- **Structured Logging**: Consistent logging with context and formatting
- **Spark Session Factory**: Centralized Spark session creation
- **Configuration Utilities**: Base configuration patterns

## Components

### 1. Structured Logger (`logger.py`)

Provides context-rich logging with consistent formatting across all components.

#### Features

- **Structured Format**: JSON-like log entries with timestamps, levels, and context
- **Context Management**: Automatic component/module identification
- **Multiple Levels**: DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Exception Handling**: Automatic exception logging with stack traces
- **Performance**: Minimal overhead, lazy evaluation

#### Usage

```python
from src.common.logger import StructuredLogger

# Get logger for current module
logger = StructuredLogger.get_logger(__name__)

# Basic logging
logger.info("Processing started")
logger.warning("High memory usage detected")
logger.error("Failed to connect to Kafka")

# With context
logger.info("Event processed", extra={
    "event_type": "init",
    "user_id": "user123",
    "processing_time_ms": 45
})

# Exception logging
try:
    process_event(data)
except Exception as e:
    logger.exception("Event processing failed", exc_info=e)
```

#### Log Format

```
2024-01-29 10:15:30,123 - INFO - src.producer.main - Event generated - {"event_type": "init", "user_id": "user123"}
```

Components:
- `2024-01-29 10:15:30,123`: Timestamp with milliseconds
- `INFO`: Log level
- `src.producer.main`: Module/component name
- `Event generated`: Log message
- `{"event_type": "init", ...}`: Optional context (if provided)

#### Configuration

Set log level via environment variable:

```bash
# In .env or environment
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

Per-component configuration:

```python
# Override log level for specific logger
logger = StructuredLogger.get_logger(__name__, level="DEBUG")
```

#### Best Practices

**DO**:
```python
# ✅ Use appropriate log levels
logger.debug("Detailed debugging information")
logger.info("Normal operation event")
logger.warning("Something unexpected but handled")
logger.error("Error occurred, operation failed")
logger.critical("System-level failure")

# ✅ Add context
logger.info("User registered", extra={
    "user_id": user_id,
    "country": country,
    "platform": platform
})

# ✅ Use exception logging
try:
    risky_operation()
except ValueError as e:
    logger.exception("Validation failed", exc_info=e)
```

**DON'T**:
```python
# ❌ String formatting in log call (use lazy evaluation)
logger.info(f"Processing user {user_id}")  # Bad

# ✅ Let logger handle formatting
logger.info("Processing user %s", user_id)  # Good

# ❌ Logging sensitive data
logger.info(f"User password: {password}")  # NEVER!

# ❌ Excessive logging in hot paths
for event in millions_of_events:
    logger.debug(f"Processing {event}")  # Too much!
```

### 2. Spark Session Factory (`spark_utils.py`)

Centralized factory for creating Spark sessions with consistent configuration.

#### Features

- **Batch Sessions**: For batch processing jobs
- **Streaming Sessions**: For Structured Streaming jobs
- **Delta Lake Integration**: Automatic Delta Lake configuration
- **Consistent Settings**: Default configurations for all jobs

#### Usage

##### Batch Processing

```python
from src.common.spark_utils import SparkSessionFactory

# Create batch session
spark = SparkSessionFactory.create_batch_session(
    app_name="my-batch-job"
)

# Use for batch operations
df = spark.read.format("delta").load("/path/to/table")
df.write.format("delta").mode("overwrite").save("/path/to/output")

# Clean up
spark.stop()
```

##### Streaming Processing

```python
from src.common.spark_utils import SparkSessionFactory

# Create streaming session
spark = SparkSessionFactory.create_streaming_session(
    app_name="my-streaming-job"
)

# Use for streaming operations
df = spark.readStream.format("delta").load("/path/to/table")

query = (df
    .writeStream
    .format("delta")
    .outputMode("append")
    .start("/path/to/output"))

query.awaitTermination()
```

#### Configuration

Default configurations applied:

**Delta Lake**:
```
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**Streaming** (additional for streaming sessions):
```
spark.sql.streaming.schemaInference=true
spark.sql.streaming.checkpointLocation.minBatchesToRetain=2
```

**Adaptive Query Execution** (for batch):
```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

#### Customization

Override defaults:

```python
spark = (SparkSessionFactory
    .create_batch_session(app_name="custom-job")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.executor.memory", "4g"))
```

### 3. Configuration Utilities (`config.py`)

Base patterns for configuration management (currently minimal, can be extended).

#### Purpose

Provides common configuration utilities:
- Environment variable handling
- Configuration validation
- Default value management

#### Usage with Pydantic

All components use Pydantic for configuration:

```python
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class MyConfig(BaseSettings):
    """Component configuration."""
    
    kafka_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers"
    )
    
    topic_name: str = Field(
        default="events",
        description="Kafka topic name"
    )
    
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False  # KAFKA_SERVERS or kafka_servers both work
    )

# Usage
config = MyConfig()
print(config.kafka_servers)  # From environment or default
```

#### Configuration Hierarchy

1. **Environment variables** (highest priority)
2. **`.env` file** values
3. **Default values** in code (lowest priority)

Example:

```bash
# .env file
KAFKA_SERVERS=localhost:29092
LOG_LEVEL=DEBUG

# Python
config = MyConfig()
print(config.kafka_servers)  # "localhost:29092" from .env
print(config.topic_name)     # "events" from default
```

## Design Patterns

### Singleton Pattern (Logger)

Logger instances are cached per module name:

```python
# Both calls return same logger instance
logger1 = StructuredLogger.get_logger("my_module")
logger2 = StructuredLogger.get_logger("my_module")
assert logger1 is logger2  # True
```

### Factory Pattern (Spark Session)

Factory methods create appropriate Spark sessions:

```python
# Different factory methods for different use cases
batch_spark = SparkSessionFactory.create_batch_session(...)
streaming_spark = SparkSessionFactory.create_streaming_session(...)
```

### Builder Pattern (Configuration)

Pydantic settings follow builder pattern:

```python
config = (MyConfig()
    .copy(update={"kafka_servers": "new:9092"})
    .copy(update={"log_level": "DEBUG"}))
```

## Testing

### Logger Testing

```python
import logging
from src.common.logger import StructuredLogger

def test_logger_creation():
    """Test logger creation."""
    logger = StructuredLogger.get_logger("test_module")
    assert logger.name == "test_module"
    assert logger.level == logging.INFO

def test_logger_with_context(caplog):
    """Test logging with context."""
    logger = StructuredLogger.get_logger("test")
    
    with caplog.at_level(logging.INFO):
        logger.info("Test message", extra={"key": "value"})
    
    assert "Test message" in caplog.text
    assert "key" in caplog.text
```

### Spark Session Testing

```python
from src.common.spark_utils import SparkSessionFactory

def test_batch_session_creation():
    """Test batch session creation."""
    spark = SparkSessionFactory.create_batch_session("test-batch")
    
    assert spark.sparkContext.appName == "test-batch"
    assert spark.conf.get("spark.sql.extensions") == "io.delta.sql.DeltaSparkSessionExtension"
    
    spark.stop()

def test_streaming_session_creation():
    """Test streaming session creation."""
    spark = SparkSessionFactory.create_streaming_session("test-streaming")
    
    assert spark.sparkContext.appName == "test-streaming"
    assert spark.conf.get("spark.sql.streaming.schemaInference") == "true"
    
    spark.stop()
```

### Configuration Testing

```python
import os
from pydantic import Field
from pydantic_settings import BaseSettings

def test_config_from_environment():
    """Test configuration from environment."""
    os.environ["KAFKA_SERVERS"] = "test:9092"
    
    class TestConfig(BaseSettings):
        kafka_servers: str = Field(default="default:9092")
    
    config = TestConfig()
    assert config.kafka_servers == "test:9092"
    
    del os.environ["KAFKA_SERVERS"]

def test_config_defaults():
    """Test configuration defaults."""
    class TestConfig(BaseSettings):
        topic: str = Field(default="default_topic")
    
    config = TestConfig()
    assert config.topic == "default_topic"
```

## Best Practices

### Logging

1. **Use appropriate log levels**:
   - DEBUG: Detailed diagnostic information
   - INFO: General informational messages
   - WARNING: Something unexpected but handled
   - ERROR: Error occurred, operation failed
   - CRITICAL: System-level failure

2. **Add context to logs**:
```python
# Good - includes context
logger.info("Event processed", extra={
    "event_type": "purchase",
    "user_id": user_id,
    "amount": amount
})

# Bad - no context
logger.info("Event processed")
```

3. **Don't log sensitive data**:
```python
# Bad - logs password
logger.info(f"User login: {username}:{password}")

# Good - no sensitive data
logger.info("User login", extra={"username": username})
```

4. **Use exception logging**:
```python
# Good - logs exception with stack trace
try:
    process()
except Exception as e:
    logger.exception("Processing failed", exc_info=e)

# Bad - loses stack trace
except Exception as e:
    logger.error(f"Processing failed: {e}")
```

### Spark Sessions

1. **Always stop sessions**:
```python
spark = SparkSessionFactory.create_batch_session(app_name)
try:
    # Do work
    pass
finally:
    spark.stop()  # Always clean up
```

2. **Use appropriate session type**:
```python
# For batch jobs
spark = SparkSessionFactory.create_batch_session(...)

# For streaming jobs
spark = SparkSessionFactory.create_streaming_session(...)
```

3. **Set meaningful app names**:
```python
# Good - descriptive
spark = SparkSessionFactory.create_batch_session("daily-user-aggregation")

# Bad - generic
spark = SparkSessionFactory.create_batch_session("job1")
```

### Configuration

1. **Use Pydantic for validation**:
```python
class Config(BaseSettings):
    port: int = Field(ge=1, le=65535)  # Validate port range
    timeout: float = Field(gt=0)  # Must be positive
```

2. **Document all settings**:
```python
kafka_servers: str = Field(
    default="kafka:9092",
    description="Comma-separated list of Kafka broker addresses"
)
```

3. **Provide sensible defaults**:
```python
# Good - useful default
log_level: str = Field(default="INFO")

# Bad - no default for required setting
kafka_topic: str = Field()  # Missing default
```

## Integration Examples

### Complete Component Example

```python
"""My custom component."""

from src.common.logger import StructuredLogger
from src.common.spark_utils import SparkSessionFactory
from pydantic import Field
from pydantic_settings import BaseSettings

class MyComponentConfig(BaseSettings):
    """Configuration for my component."""
    input_path: str = Field(default="/path/to/input")
    output_path: str = Field(default="/path/to/output")
    log_level: str = Field(default="INFO")

def main():
    """Main entry point."""
    # Load configuration
    config = MyComponentConfig()
    
    # Initialize logger
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    logger.info("Component starting", extra={"config": config.dict()})
    
    # Create Spark session
    spark = SparkSessionFactory.create_batch_session("my-component")
    
    try:
        # Do work
        df = spark.read.format("delta").load(config.input_path)
        logger.info("Data loaded", extra={"rows": df.count()})
        
        # Process...
        
        df.write.format("delta").mode("overwrite").save(config.output_path)
        logger.info("Data written", extra={"path": config.output_path})
        
    except Exception as e:
        logger.exception("Component failed", exc_info=e)
        raise
    finally:
        spark.stop()
        logger.info("Component stopped")

if __name__ == "__main__":
    main()
```

## Future Enhancements

- [ ] Add metrics collection utilities
- [ ] Implement retry decorators
- [ ] Add connection pool managers
- [ ] Implement circuit breaker pattern
- [ ] Add distributed tracing support
- [ ] Implement configuration hot-reload
- [ ] Add health check utilities
- [ ] Implement graceful shutdown handlers
- [ ] Add performance profiling decorators
- [ ] Implement feature flags framework
