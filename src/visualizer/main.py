"""Data Visualizer Main Entry Point.

Provides the main application logic for querying and visualizing Delta Lake tables
across the medallion architecture. Supports both full table exploration and targeted
visualization of specific layers or event types through command-line arguments.

Functions:
    print_separator: Format and print section separators.
    query_bronze_table: Query and display bronze layer raw events.
    query_silver_init_table: Query and display transformed init events.
    query_silver_match_table: Query and display transformed match events.
    query_silver_purchase_table: Query and display transformed purchase events.
    query_gold_daily_users_table: Query and display daily user aggregations.
    query_gold_minute_purchases_table: Query and display minute purchase aggregations.
    query_gold_minute_matches_table: Query and display minute match aggregations.
    query_gold_minute_purchases_by_country_table: Query minute purchases by country.
    query_gold_minute_matches_by_country_table: Query minute matches by country.
    parse_arguments: Parse and validate command-line arguments.
    main: Entry point for the data visualizer application.
"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as spark_sum

from src.visualizer.config import VisualizerConfig
from src.common.spark_utils import SparkSessionFactory
from src.common.logger import StructuredLogger


def print_separator(title: str):
    """Print a formatted section separator with title.
    
    Args:
        title: Title text to display in the separator.
    """
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def query_bronze_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display bronze table summary.
    
    Displays total record count, event distribution by topic, and sample records
    from the bronze layer containing raw events from Kafka.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("BRONZE LAYER - Raw Events")
    
    try:
        if not os.path.exists(config.storage_bronze_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Bronze table not found at: {config.storage_bronze_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_bronze_path)
        
        print(f"\n📊 Total Records: {df.count():,}")
        
        print("\n📈 Events by Topic:")
        df.groupBy("topic").count().orderBy("topic").show(truncate=False)
        
        print(f"\n🔍 Sample Records (showing {config.num_rows}):")
        df.select("topic", "kafka_timestamp", "ingestion_timestamp").show(config.num_rows, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading bronze table: {e}")


def query_silver_init_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display silver init events.
    
    Displays transformed init events with uppercase platforms and mapped country names.
    Shows event distribution by platform and country, plus sample records.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("SILVER LAYER - Init Events")
    
    try:
        if not os.path.exists(config.storage_silver_init_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Silver init table not found at: {config.storage_silver_init_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_silver_init_path)
        
        print(f"\n📊 Total Init Events: {df.count():,}")
        
        print("\n📈 Events by Platform (after transformation):")
        df.groupBy("platform").count().orderBy("platform").show(truncate=False)
        
        print("\n🌍 Events by Country Name (after mapping):")
        df.groupBy("country_name").count().orderBy(count("*").desc()).show(10, truncate=False)
        
        print(f"\n🔍 Sample Records (showing {config.num_rows}):")
        df.select("user-id", "platform", "country", "country_name", "processing_timestamp").show(config.num_rows, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading silver init table: {e}")


def query_silver_match_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display silver match events.
    
    Displays transformed match events with uppercase platform names. Shows event
    distribution by platform and sample records with player and match details.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("SILVER LAYER - Match Events")
    
    try:
        if not os.path.exists(config.storage_silver_match_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Silver match table not found at: {config.storage_silver_match_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_silver_match_path)
        
        print(f"\n📊 Total Match Events: {df.count():,}")
        
        print("\n📈 User A Platforms (after transformation):")
        df.groupBy("user_a_platform").count().orderBy("user_a_platform").show(truncate=False)
        
        print(f"\n🔍 Sample Records (showing {config.num_rows}):")
        df.select("user-a", "user-b", "winner", "user_a_platform", "user_b_platform", "game-tier").show(config.num_rows, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading silver match table: {e}")


def query_silver_purchase_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display silver purchase events.
    
    Displays transformed purchase events with mapped product names. Shows total
    revenue, revenue distribution by product, and sample purchase records.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("SILVER LAYER - Purchase Events")
    
    try:
        if not os.path.exists(config.storage_silver_purchase_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Silver purchase table not found at: {config.storage_silver_purchase_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_silver_purchase_path)
        
        print(f"\n📊 Total Purchase Events: {df.count():,}")
        
        total_revenue = df.agg(spark_sum("purchase_value")).collect()[0][0]
        print(f"💰 Total Revenue: ${total_revenue:,.2f}" if total_revenue else "💰 Total Revenue: $0.00")
        
        print("\n📈 Revenue by Product Name (after mapping):")
        df.groupBy("product_name").agg(
            count("*").alias("count"),
            spark_sum("purchase_value").alias("revenue")
        ).orderBy("revenue", ascending=False).show(10, truncate=False)
        
        print(f"\n🔍 Sample Records (showing {config.num_rows}):")
        df.select("user-id", "product-id", "product_name", "purchase_value", "processing_timestamp").show(config.num_rows, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading silver purchase table: {e}")


def query_gold_daily_users_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display gold daily users aggregation.
    
    Displays daily aggregated user counts by country and platform. Shows the total
    number of daily records and recent daily user count data.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("GOLD LAYER - Daily Aggregated Users")
    
    try:
        if not os.path.exists(config.storage_gold_daily_users_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Gold daily users table not found at: {config.storage_gold_daily_users_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_gold_daily_users_path)
        
        print(f"\n📊 Total Daily Records: {df.count():,}")
        
        print("\n📈 Recent Daily User Counts:")
        df.orderBy("event_date", "country_name", "platform").show(20, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading gold daily users table: {e}")


def query_gold_minute_purchases_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display gold minute purchases aggregation.
    
    Displays minute-level global purchase aggregations including purchase counts,
    total revenue, and distinct user counts.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("GOLD LAYER - Minute Purchase Aggregations (Global)")
    
    try:
        if not os.path.exists(config.storage_gold_minute_purchases_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Gold minute purchases table not found at: {config.storage_gold_minute_purchases_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_gold_minute_purchases_path)
        
        print(f"\n📊 Total Minute Records: {df.count():,}")
        
        print("\n📈 Recent Minute Purchase Aggregations:")
        df.orderBy("window_start").show(20, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading gold minute purchases table: {e}")


def query_gold_minute_matches_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display gold minute matches aggregation.
    
    Displays minute-level global match aggregations including match counts and
    distinct user counts.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("GOLD LAYER - Minute Match Aggregations (Global)")
    
    try:
        if not os.path.exists(config.storage_gold_minute_matches_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Gold minute matches table not found at: {config.storage_gold_minute_matches_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_gold_minute_matches_path)
        
        print(f"\n📊 Total Minute Records: {df.count():,}")
        
        print("\n📈 Recent Minute Match Aggregations:")
        df.orderBy("window_start").show(20, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading gold minute matches table: {e}")

def query_gold_minute_purchases_by_country_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display gold minute purchases by country aggregation.
    
    Displays minute-level purchase aggregations segmented by country. Shows top
    countries by revenue and recent minute-level country aggregations.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("GOLD LAYER - Minute Purchase Aggregations by Country")
    
    try:
        if not os.path.exists(config.storage_gold_minute_purchases_by_country_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Gold minute purchases by country table not found at: {config.storage_gold_minute_purchases_by_country_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_gold_minute_purchases_by_country_path)
        
        print(f"\n📊 Total Minute-Country Records: {df.count():,}")
        
        print("\n💰 Top Countries by Revenue:")
        df.groupBy("country_name").agg(
            spark_sum("country_revenue").alias("total_revenue"),
            spark_sum("purchase_count").alias("total_purchases")
        ).orderBy("total_revenue", ascending=False).show(10, truncate=False)
        
        print("\n📈 Recent Minute Purchase Aggregations by Country:")
        df.orderBy("window_start", "country_name").show(20, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading gold minute purchases by country table: {e}")


def query_gold_minute_matches_by_country_table(spark: SparkSession, config: VisualizerConfig):
    """Query and display gold minute matches by country aggregation.
    
    Displays minute-level match aggregations segmented by country. Shows top
    countries by match count and recent minute-level country aggregations.
    
    Args:
        spark: Active SparkSession for querying Delta tables.
        config: Visualizer configuration with storage paths and display settings.
    """
    print_separator("GOLD LAYER - Minute Match Aggregations by Country")
    
    try:
        if not os.path.exists(config.storage_gold_minute_matches_by_country_path.replace("/opt/bitnami/spark", ".")):
            print(f"❌ Gold minute matches by country table not found at: {config.storage_gold_minute_matches_by_country_path}")
            return
        
        df = spark.read.format("delta").load(config.storage_gold_minute_matches_by_country_path)
        
        print(f"\n📊 Total Minute-Country Records: {df.count():,}")
        
        print("\n🎮 Top Countries by Match Count:")
        df.groupBy("country_name").agg(
            spark_sum("match_count").alias("total_matches")
        ).orderBy("total_matches", ascending=False).show(10, truncate=False)
        
        print("\n📈 Recent Minute Match Aggregations by Country:")
        df.orderBy("window_start", "country_name").show(20, truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading gold minute matches by country table: {e}")

def parse_arguments():
    """Parse and validate command-line arguments.
    
    Provides options for selecting which Delta tables to visualize. Supports
    viewing all tables at once or targeting specific tables by name.
    
    Returns:
        Parsed argument namespace with table selection and flags.
    """
    parser = argparse.ArgumentParser(
        description="Gaming Event Processing Data Visualizer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show all tables
  python main.py --all
  
  # Show specific table
  python main.py --table bronze
  python main.py --table silver-init
  python main.py --table gold-daily-users
  
  # Show multiple tables
  python main.py --table bronze --table silver-init
        """
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show all tables (default if no table specified)"
    )
    
    parser.add_argument(
        "--table",
        action="append",
        choices=[
            "bronze",
            "silver-init",
            "silver-match",
            "silver-purchase",
            "gold-daily-users",
            "gold-minute-purchases",
            "gold-minute-matches",
            "gold-minute-purchases-by-country",
            "gold-minute-matches-by-country"
        ],
        help="Specify which table(s) to visualize (can be used multiple times)"
    )
    
    args = parser.parse_args()
    
    if not args.table and not args.all:
        args.all = True
    
    return args


def main():
    """Main entry point for data visualizer.
    
    Orchestrates the full visualization workflow: parses arguments, loads configuration,
    initializes Spark session, and queries selected Delta tables. Displays formatted
    summaries for each selected table in the medallion architecture.
    """
    args = parse_arguments()
    
    config = VisualizerConfig()
    
    logger = StructuredLogger.get_logger(__name__, level=config.log_level)
    
    logger.info("Initializing data visualizer application")
    
    spark = SparkSessionFactory.create_batch_session(
        app_name=config.spark_app_name
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    
    print("\n" + "=" * 80)
    print("  🎮 GAMING EVENT PROCESSING - DATA VISUALIZER")
    print("=" * 80)
    
    tables_to_query = []
    if args.all:
        tables_to_query = [
            "bronze",
            "silver-init",
            "silver-match",
            "silver-purchase",
            "gold-daily-users",
            "gold-minute-purchases",
            "gold-minute-matches",
            "gold-minute-purchases-by-country",
            "gold-minute-matches-by-country"
        ]
    else:
        tables_to_query = args.table
    
    table_functions = {
        "bronze": query_bronze_table,
        "silver-init": query_silver_init_table,
        "silver-match": query_silver_match_table,
        "silver-purchase": query_silver_purchase_table,
        "gold-daily-users": query_gold_daily_users_table,
        "gold-minute-purchases": query_gold_minute_purchases_table,
        "gold-minute-matches": query_gold_minute_matches_table,
        "gold-minute-purchases-by-country": query_gold_minute_purchases_by_country_table,
        "gold-minute-matches-by-country": query_gold_minute_matches_by_country_table
    }
    
    for table_name in tables_to_query:
        if table_name in table_functions:
            table_functions[table_name](spark, config)
    
    print("\n" + "=" * 80)
    print("  ✅ Visualization Complete!")
    print("=" * 80 + "\n")
    
    logger.info("Data visualizer completed successfully")
    
    spark.stop()


if __name__ == "__main__":
    main()
