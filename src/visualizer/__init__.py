"""Data Visualizer Module.

Provides tools for querying and displaying summaries of all Delta Lake tables
across the medallion architecture (bronze, silver, and gold layers). This module
enables data validation and exploration through console-based visualizations.

Classes:
    VisualizerConfig: Configuration management for visualization settings.

Functions:
    query_bronze_table: Query and display bronze layer data.
    query_silver_*_table: Query and display silver layer data by event type.
    query_gold_*_table: Query and display gold layer aggregations.
    main: Entry point for the data visualizer application.
"""
