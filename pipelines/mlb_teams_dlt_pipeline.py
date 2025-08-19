"""
Delta Live Tables Pipeline for MLB Teams Data

This module defines the DLT pipeline for processing MLB teams data
through Bronze, Silver, and Gold layers.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from utils.mlb_data_source_api import get_teams_data
from utils.logger import setup_logger

logger = setup_logger(__name__)


@dlt.table(
    name="teams_bronze",
    comment="Raw MLB teams data from API",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def teams_bronze():
    """
    Bronze table: Raw MLB teams data directly from the API.
    
    This table contains the raw, unprocessed data from the MLB API
    with minimal transformations - only adding extraction metadata.
    """
    
    # Extract data using the custom data source API
    teams_result = get_teams_data()
    teams_data = teams_result["data"]
    
    # Convert to Spark DataFrame
    # Since we're in a DLT context, we'll create a DataFrame from the data
    spark = dlt.spark
    
    # Create DataFrame from the extracted data
    teams_df = spark.createDataFrame(teams_data)
    
    return (
        teams_df
        .withColumn("ingestion_date", F.to_date(F.col("extraction_time")))
        .withColumn("updated_at", F.current_timestamp())
    )


@dlt.table(
    name="teams_silver",
    comment="Cleaned and standardized MLB teams data",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all_or_drop("valid_team_id", "id IS NOT NULL")
@dlt.expect_all_or_drop("valid_team_name", "name IS NOT NULL")
def teams_silver():
    """
    Silver table: Cleaned and standardized teams data.
    
    This table contains cleansed data with:
    - Data quality checks
    - Standardized column names
    - Proper data types
    - Deduplication
    """
    
    return (
        dlt.read("teams_bronze")
        .select(
            F.col("id").cast(IntegerType()).alias("team_id"),
            F.col("name").alias("team_name"),
            F.col("teamName").alias("team_display_name"),
            F.col("abbreviation").alias("team_abbreviation"),
            F.col("locationName").alias("location_name"),
            F.col("division.id").cast(IntegerType()).alias("division_id"),
            F.col("division.name").alias("division_name"),
            F.col("league.id").cast(IntegerType()).alias("league_id"),
            F.col("league.name").alias("league_name"),
            F.col("sport.id").cast(IntegerType()).alias("sport_id"),
            F.col("sport.name").alias("sport_name"),
            F.col("extraction_time").cast(TimestampType()).alias("extracted_at"),
            F.col("ingestion_date"),
            F.col("updated_at")
        )
        .dropDuplicates(["team_id"])
    )


@dlt.table(
    name="teams_gold",
    comment="Business-ready MLB teams dimension table",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def teams_gold():
    """
    Gold table: Business-ready teams dimension.
    
    This table contains the final, analytics-ready teams dimension
    with additional computed fields and business logic.
    """
    
    return (
        dlt.read("teams_silver")
        .select(
            F.col("team_id"),
            F.col("team_name"),
            F.col("team_display_name"),
            F.col("team_abbreviation"),
            F.col("location_name"),
            F.col("division_id"),
            F.col("division_name"),
            F.col("league_id"),
            F.col("league_name"),
            F.col("sport_id"),
            F.col("sport_name"),
            # Business logic: Create full team name
            F.concat_ws(" ", F.col("location_name"), F.col("team_display_name")).alias("full_team_name"),
            # Create team key for joins
            F.concat_ws("_", F.col("league_name"), F.col("team_abbreviation")).alias("team_key"),
            # Add data lineage
            F.col("extracted_at"),
            F.col("ingestion_date"),
            F.col("updated_at"),
            # Add effective dating for SCD Type 2
            F.col("ingestion_date").alias("effective_date"),
            F.lit(None).cast("date").alias("end_date"),
            F.lit(True).alias("is_current")
        )
    )