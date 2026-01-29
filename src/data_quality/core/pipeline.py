"""
Silver Layer Data Quality Pipeline.

Main orchestrator for the silver layer data quality pipeline.
Reads from bronze, applies transformations, and writes to silver layer
and Kafka using the Template Method pattern.

Design Pattern: Template Method Pattern
- Eliminates code duplication across event type processing
- Single bronze read with stream branching for efficiency
- Extensible configuration-based approach
"""

import os
from typing import Callable, Dict, List, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from src.common.logger import StructuredLogger, log_exception
from src.data_quality.core.config import DataQualityConfig
from src.data_quality.processors.base import EventTypeConfig
from src.data_quality.processors.event_processor import EventProcessor
from src.data_quality.transformations.loader import TransformationLoader
from src.data_quality.writers.delta_writer import DeltaWriter
from src.data_quality.writers.kafka_writer import KafkaWriter

logger = StructuredLogger.get_logger(__name__)


class SilverDataQualityPipeline:
    """Silver layer data quality pipeline orchestrator.
    
    Orchestrates the complete data quality transformation pipeline for the silver layer,
    implementing the Template Method design pattern to eliminate code duplication across
    event types. The pipeline:
    
    - Reads from bronze layer once and branches streams for efficiency
    - Applies event-specific transformations based on YAML rules
    - Writes cleaned data to silver Delta tables
    - Optionally publishes to Kafka for downstream consumers
    
    The pipeline is extensible through YAML configuration files, allowing new event types
    and transformations to be added without code changes.
    
    Attributes:
        spark: Active SparkSession instance with streaming capabilities.
        config: Configuration object containing paths and settings.
        transformation_loader: Loads and applies YAML-defined transformation rules.
        event_processor: Handles JSON parsing, filtering, and preprocessing.
        delta_writer: Writes streaming data to Delta Lake tables.
        kafka_writer: Publishes streaming data to Kafka topics.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[DataQualityConfig] = None
    ):
        """Initialize silver pipeline.
        
        Args:
            spark: SparkSession instance configured for structured streaming.
            config: Optional configuration instance. If None, uses default values.
        """
        self.spark = spark
        self.config = config or DataQualityConfig()
        
        self.transformation_loader = TransformationLoader(self.config.rules_dir)
        self.event_processor = EventProcessor(spark, self.transformation_loader)
        self.delta_writer = DeltaWriter(self.config.streaming_trigger_interval)
        self.kafka_writer = KafkaWriter(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topic=self.config.kafka_silver_topic,
            trigger_interval=self.config.streaming_trigger_interval,
            enabled=self.config.enable_kafka_output
        )
        
        self._event_configs = self._initialize_event_configs()
        
        logger.info(
            "Silver data quality pipeline initialized",
            extra={
                "bronze_path": self.config.storage_bronze_path,
                "rules_dir": self.config.rules_dir,
                "event_types": [ec.event_type for ec in self._event_configs]
            }
        )
    
    def _get_preprocessing_function(self, name: Optional[str]) -> Optional[Callable]:
        """Get preprocessing function by name from registry.
        
        Maps preprocessing function names from YAML configuration to actual Python functions.
        This registry pattern makes it easy to add new preprocessing functions by updating
        both this registry and adding the corresponding method.
        
        Args:
            name: Name of the preprocessing function as specified in YAML config.
            
        Returns:
            Callable preprocessing function or None if name is None or not found.
            
        Example:
            To add a new preprocessing function:
            1. Add the function to preprocessing_registry dict
            2. Implement the function method in EventProcessor class
            3. Reference it in event_configs.yml
        """
        if not name:
            return None
        
        preprocessing_registry: Dict[str, Callable] = {
            "flatten_match_fields": EventProcessor.flatten_match_fields,
        }
        
        if name not in preprocessing_registry:
            logger.warning(
                "Preprocessing function not found: %s",
                name,
                extra={"available_functions": list(preprocessing_registry.keys())}
            )
            return None
        
        return preprocessing_registry[name]
    
    def _initialize_event_configs(self) -> List[EventTypeConfig]:
        """Load event type configurations from YAML file.
        
        Reads event type configurations from an external YAML file, enabling configuration-driven
        development. New event types can be added by:
        1. Adding an entry to event_configs.yml
        2. Creating corresponding transformation rules YAML file
        3. Adding storage paths to DataQualityConfig if needed
        
        The method validates required fields, builds paths from configuration, and optionally
        resolves preprocessing functions from the registry.
        
        Returns:
            List of EventTypeConfig instances, one for each configured event type.
            
        Raises:
            FileNotFoundError: If event_configs.yml file is not found.
            ValueError: If YAML is malformed or missing required fields.
        """
        config_path = self.config.event_configs_path
        
        if not os.path.exists(config_path):
            logger.error("Event configs file not found: %s", config_path)
            raise FileNotFoundError(f"Event configs file not found: {config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            if not config_data or 'events' not in config_data:
                raise ValueError("Event configs YAML must contain 'events' key")
            
            event_configs = []
            for event_cfg in config_data['events']:
                required_fields = ['event_type', 'event_filter', 'silver_table', 'transform_key']
                for field in required_fields:
                    if field not in event_cfg:
                        raise ValueError(f"Missing required field '{field}' in event config: {event_cfg}")
                
                silver_table = event_cfg['silver_table']
                silver_path = getattr(self.config, f"storage_silver_{silver_table}_path")
                checkpoint_path = getattr(self.config, f"checkpoint_silver_{silver_table}")
                
                preprocess_fn = self._get_preprocessing_function(
                    event_cfg.get('preprocessing')
                )
                
                event_config = EventTypeConfig(
                    event_type=event_cfg['event_type'],
                    event_filter=event_cfg['event_filter'],
                    silver_path=silver_path,
                    checkpoint_path=checkpoint_path,
                    transform_key=event_cfg['transform_key'],
                    preprocess_fn=preprocess_fn
                )
                event_configs.append(event_config)
                
                logger.info(
                    "Loaded event config: %s",
                    event_cfg['event_type'],
                    extra={
                        "filter": event_cfg['event_filter'],
                        "has_preprocessing": preprocess_fn is not None
                    }
                )
            
            return event_configs
            
        except yaml.YAMLError as e:
            logger.error("Failed to parse event configs YAML: %s", e)
            raise ValueError(f"Invalid YAML in event configs file: {e}") from e
        except AttributeError as e:
            logger.error("Configuration attribute not found: %s", e)
            raise ValueError(f"Event config references undefined configuration: {e}") from e
    
    def _process_event_stream(
        self,
        bronze_df: DataFrame,
        event_config: EventTypeConfig
    ) -> List[StreamingQuery]:
        """Process a single event type stream using template method pattern.
        
        Defines the standard processing workflow that applies to all event types,
        eliminating code duplication. The workflow consists of:
        
        1. Parse JSON events and filter by event type
        2. Apply optional preprocessing (e.g., flatten nested structures)
        3. Apply data quality transformations from YAML rules
        4. Write to Delta Lake silver table (always)
        5. Write to Kafka topic (optional, based on configuration)
        
        Args:
            bronze_df: Streaming DataFrame from bronze layer multiplex table.
            event_config: Configuration specifying event type, paths, and transformations.
            
        Returns:
            List of StreamingQuery handles (Delta write + optional Kafka write).
        """
        logger.info("Processing %s events", event_config.event_type)
        
        parsed_df = self.event_processor.parse_and_filter(bronze_df, event_config)
        
        silver_df = self.event_processor.apply_transformations(parsed_df, event_config)
        
        queries = []
        queries.append(self.delta_writer.write(silver_df, event_config))
        
        kafka_query = self.kafka_writer.write(silver_df, event_config)
        if kafka_query:
            queries.append(kafka_query)
        
        return queries
    
    def run(self):
        """Run all silver layer processing streams.
        
        Executes the complete data quality pipeline with an important optimization:
        reads the bronze table only once and branches the stream to process multiple
        event types. This is significantly more efficient than creating separate
        read queries for each event type.
        
        Spark Structured Streaming handles stream branching efficiently, sharing
        the bronze table read across all event type processing branches.
        
        The method blocks until one of the streaming queries terminates (normally
        runs indefinitely). In production, consider implementing more sophisticated
        query monitoring and failure handling.
        
        Raises:
            Exception: Re-raises any exception after logging for upstream handling.
        """
        try:
            logger.info("Starting silver layer data quality pipeline")
            
            bronze_df = self.event_processor.read_bronze(self.config.storage_bronze_path)
            
            all_queries = []
            for event_config in self._event_configs:
                queries = self._process_event_stream(bronze_df, event_config)
                all_queries.extend(queries)
            
            logger.info(
                "All silver streams started: %d queries",
                len(all_queries),
                extra={
                    "query_ids": [q.id for q in all_queries],
                    "event_types": [ec.event_type for ec in self._event_configs]
                }
            )
            
            all_queries[0].awaitTermination()
            
        except Exception as e:
            log_exception(
                logger,
                e,
                context={"component": "silver_data_quality"}
            )
            raise
