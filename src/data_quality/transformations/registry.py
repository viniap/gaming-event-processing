"""
Transformation Registry.

Factory and registry for managing transformation implementations.
Uses the Registry design pattern for extensible transformation lookup.
"""

from typing import Dict

from src.data_quality.transformations.base import Transformation
from src.data_quality.transformations.uppercase import UppercaseTransformation
from src.data_quality.transformations.mapping import MappingTransformation
from src.common.logger import StructuredLogger


logger = StructuredLogger.get_logger(__name__)


class TransformationRegistry:
    """Registry for transformation implementations using Factory pattern.
    
    Provides centralized registration and lookup of transformation implementations by name.
    The registry enables:
    - Runtime transformation lookup from YAML configuration
    - Easy extension with new transformations without modifying loader code
    - Singleton pattern for global transformation availability
    
    New transformations can be registered by:
    1. Creating a class that extends Transformation base class
    2. Calling TransformationRegistry.register() with an instance
    3. Referencing the transformation name in YAML rules
    
    Attributes:
        _transformations: Class-level dictionary of registered transformation instances.
        _initialized: Flag to prevent duplicate initialization.
    """
    
    _transformations: Dict[str, Transformation] = {}
    _initialized: bool = False
    
    @classmethod
    def register(cls, transformation: Transformation):
        """Register a transformation instance in the registry.
        
        Args:
            transformation: Transformation instance to register. The instance's get_name()
                method determines the registration key.
        """
        name = transformation.get_name()
        cls._transformations[name] = transformation
        logger.info("Registered transformation: %s", name)
    
    @classmethod
    def get(cls, name: str) -> Transformation:
        """Get transformation instance by name.
        
        Automatically initializes the registry with default transformations on first access.
        
        Args:
            name: Transformation name as defined by get_name() method.
            
        Returns:
            Transformation instance ready to use.
            
        Raises:
            ValueError: If transformation name is not registered. Error message includes
                list of available transformation names.
        """
        if not cls._initialized:
            cls.initialize_defaults()
        
        if name not in cls._transformations:
            raise ValueError(
                f"Unknown transformation: {name}. "
                f"Available: {list(cls._transformations.keys())}"
            )
        
        return cls._transformations[name]
    
    @classmethod
    def initialize_defaults(cls):
        """Register default built-in transformations.
        
        Initializes the registry with standard transformations included with the system.
        This method is idempotent and safe to call multiple times.
        
        Registered transformations:
        - UppercaseTransformation: Convert fields to uppercase
        - MappingTransformation: Map values using dictionaries
        
        Can be extended to load custom transformations from plugins or configuration.
        """
        if cls._initialized:
            return
        
        logger.info("Initializing transformation registry")
        
        cls.register(UppercaseTransformation())
        cls.register(MappingTransformation())
        
        cls._initialized = True
        logger.info(
            "Transformation registry initialized with %d transformations",
            len(cls._transformations)
        )
    
    @classmethod
    def list_transformations(cls) -> list:
        """List all registered transformation names.
        
        Automatically initializes the registry on first access.
        
        Returns:
            List of transformation name strings available for use in YAML rules.
        """
        if not cls._initialized:
            cls.initialize_defaults()
        
        return list(cls._transformations.keys())
    
    @classmethod
    def reset(cls):
        """Reset the registry (mainly for testing)."""
        cls._transformations.clear()
        cls._initialized = False
