# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import modules and initialize resources

# COMMAND ----------

from utils.mlb_api_client import MlbApiClient
from teams_dimension_metadata import metadata 
from teams_dimension_handler import TeamDimensionHandler

api_client = MlbApiClient()
handler = TeamDimensionHandler(
    spark=spark,
    api_client=api_client,
    metadata=metadata
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL Teams Workflow

# COMMAND ----------

try:
    #Ingest raw json into volume landing zone
    landing_path, ingestion_date = handler.ingest_to_landing()

    #Load raw json into bronze table
    loaded_in_bronze = handler.load_to_bronze(landing_path, ingestion_date)

    
except Exception as e:
    raise e
