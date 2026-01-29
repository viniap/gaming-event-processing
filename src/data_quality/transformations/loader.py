"""
Transformation Rule Loader.

Loads and applies YAML-defined transformation rules to DataFrames
using the registered transformation implementations.
"""

import os
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame

from src.common.logger import StructuredLogger, log_exception
from src.data_quality.transformations.registry import TransformationRegistry

logger = StructuredLogger.get_logger(__name__)


class TransformationLoader:
    """Loads and applies YAML-defined transformation rules to DataFrames.
    
    Orchestrates the transformation process by:
    - Loading transformation rules from YAML files for each event type
    - Looking up transformation implementations from TransformationRegistry
    - Applying transformations in sequence as defined in the YAML
    - Providing graceful error handling for individual transformation failures
    
    The loader enables configuration-driven data quality, allowing transformations
    to be modified through YAML changes without code modifications.
    
    Attributes:
        rules_dir: Directory containing transformation rule YAML files.
        rules: Dictionary mapping event types to their transformation rule definitions.
    """
    
    def __init__(self, rules_dir: str):
        """Initialize transformation loader with rules directory.
        
        Args:
            rules_dir: Directory path containing YAML rule files (e.g., init_rules.yml).
        """
        self.rules_dir = rules_dir
        self.rules: Dict[str, Dict[str, Any]] = {}
        
        TransformationRegistry.initialize_defaults()
        
        self._load_rules()
    
    def _load_rules(self):
        """Load transformation rules from YAML files."""
        rule_files = {
            "init": "init_rules.yml",
            "match": "match_rules.yml",
            "in-app-purchase": "purchase_rules.yml"
        }
        
        for event_type, filename in rule_files.items():
            rule_path = os.path.join(self.rules_dir, filename)
            try:
                with open(rule_path, 'r', encoding='utf-8') as f:
                    rules = yaml.safe_load(f)
                    self.rules[event_type] = rules
                    logger.info(
                        "Loaded transformation rules for: %s",
                        event_type,
                        extra={
                            "file": filename,
                            "num_transformations": len(rules.get('transformations', []))
                        }
                    )
            except (OSError, yaml.YAMLError) as e:
                log_exception(
                    logger,
                    e,
                    context={"event_type": event_type, "file": filename}
                )
                self.rules[event_type] = {"transformations": []}
    
    def apply_transformations(
        self,
        df: DataFrame,
        event_type: str
    ) -> DataFrame:
        """Apply all configured transformations for a specific event type.
        
        Reads transformation rules from the loaded YAML configuration and applies each
        transformation in sequence. Transformations are applied independently, so a
        failure in one transformation doesn't prevent subsequent ones from executing.
        
        Args:
            df: Input DataFrame to transform.
            event_type: Event type identifier (e.g., 'init', 'match', 'in-app-purchase')
                used to look up the appropriate transformation rules.
            
        Returns:
            Transformed DataFrame with all configured transformations applied. If no
            rules exist for the event type, returns the input DataFrame unchanged.
        """
        if event_type not in self.rules:
            logger.warning(
                "No rules found for event type: %s",
                event_type,
                extra={"available_types": list(self.rules.keys())}
            )
            return df
        
        event_rules = self.rules[event_type]
        transformations_list = event_rules.get('transformations', [])
        
        if not transformations_list:
            logger.info("No transformations defined for: %s", event_type)
            return df
        
        logger.info(
            "Applying %d transformations for: %s",
            len(transformations_list),
            event_type
        )
        
        transformed_df = df
        
        for rule in transformations_list:
            try:
                field = rule['field']
                transformation_type = rule['type']
                params = rule.get('params', {})
                
                transformation = TransformationRegistry.get(transformation_type)
                
                transformed_df = transformation.apply(transformed_df, field, params)
                
                logger.debug(
                    "Applied %s to field: %s",
                    transformation_type,
                    field,
                    extra={"event_type": event_type, "params": params}
                )
            
            except (ValueError, KeyError, AttributeError) as e:
                log_exception(
                    logger,
                    e,
                    context={
                        "event_type": event_type,
                        "rule": rule
                    }
                )
                continue
        
        return transformed_df
    
    def get_event_types(self) -> list:
        """Get list of event types with defined transformation rules.
        
        Returns:
            List of event type identifiers that have loaded transformation rules.
        """
        return list(self.rules.keys())
