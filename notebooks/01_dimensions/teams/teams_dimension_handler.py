from datetime import datetime
from utils.mlb_api_client import MlbApiClient
from teams_dimension_metadata import metadata
from utils.databricks_utils import write_dictionary_to_volume_as_json
from databricks.sdk.runtime import *
import pyspark.sql.functions as F

# Logger
from utils.logger import setup_logger

logger = setup_logger(__name__)


class TeamDimensionHandler:
    """
    Handles ETL processes for the Team Dimension, organizing data into Landing, Bronze, Silver, and Gold stages.

    Workflow:
        1. Ingest raw team data from the stats MLP API and store it in a landing volume.
        2. Load the latest ingested JSON file from the landing volume and write it as a Delta table (Bronze stage).
        3. Transform and cleanse the Bronze data into 3NF format, applying SCD Type 2 logic, and store as the Silver table.
        4. Aggregate and prepare the Silver data into a consumable dimension table (Gold stage) for analytics and reporting.
    """

    def __init__(self, spark, api_client, metadata):
        """
        Module initialization.

        Args:
            spark (SparkSession): SparkSession object.
            api_client (MLBApiClient): MLB API client.
            metadata (dict): Dictionary containing metadata for the landing volume.
        """
        self.spark = spark
        self.metadata = metadata
        self.api_client = api_client

    def ingest_to_landing(self) -> tuple:
        """
        Ingest raw team data from the stats MLP API and store it in a landing volume.

        Returns:
            tuple: (path to the ingested JSON file, ingestion date)
        """
        volume_metadata = self.metadata.landing_volume
        teams = self.api_client.get_teams()
        if not teams:
            logger.error("No teams found.")
            raise ValueError("No teams found.")
        now_date = datetime.now().strftime("%Y-%m-%d")
        partition_path = f"{volume_metadata.path}/{volume_metadata.partition_key}={now_date}/{volume_metadata.file_name}"
        json_path = write_dictionary_to_volume_as_json(
            path=partition_path,
            data=teams,
        )
        logger.info(f"Teams data ingested to {json_path}, ingestion date: {now_date}")
        return json_path, now_date

    def load_to_bronze(self, json_path: str, ingestion_date: str) -> bool:
        """
        Load the latest ingested JSON file from the landing volume and write it as a Delta table (Bronze stage).

        Args:
            json_path (str): The path to the ingested JSON file.
            ingestion_date (str): The ingestion date of the ingested JSON file.

        Returns:
            bool: True if the data was successfully loaded.
        """
        teams_df = (
            self.spark.read.format("json")
            .load(json_path)
            .withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))
            .withColumn("updated_at", F.current_timestamp())
            
        )

        teams_df.write.mode("overwrite").option(
            "replaceWhere", f"ingestion_date = '{ingestion_date}'"
        ).saveAsTable("mlb.bronze.teams")

        logger.info(
            f"Teams data loaded to Bronze table, ingestion date: {ingestion_date}"
        )
        return True

    def transform_to_silver(self, ingestion_date: str) -> bool:
        """
        Transform and cleanse the Bronze data into 3NF format
        """

        teams_bronze_df = (
            self.spark.read.table("mlb.bronze.teams")
            .filter(f"ingestion_date = '{ingestion_date}'")
        )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("TeamDimensionHandler").getOrCreate()

    api_client = MlbApiClient()
    metadata = metadata

    handler = TeamDimensionHandler(spark, api_client, metadata)
    json_path, ingestion_date = handler.ingest_to_landing()
    handler.load_to_bronze(json_path, ingestion_date)