# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import modules and initialize resources
# MAGIC 
# MAGIC This workflow now supports both legacy ETL and modern Delta Live Tables approaches.

# COMMAND ----------

from utils.mlb_api_client import MlbApiClient
from utils.mlb_data_source_api import create_mlb_data_source_api
from teams_dimension_metadata import metadata 
from teams_dimension_handler import TeamDimensionHandler

api_client = MlbApiClient()

# Choose your approach:
# Option 1: Legacy approach (original)
handler_legacy = TeamDimensionHandler(
    spark=spark,
    api_client=api_client,
    metadata=metadata,
    use_dlt_api=False  # Use original approach
)

# Option 2: DLT-compatible approach (new)
handler_dlt = TeamDimensionHandler(
    spark=spark,
    api_client=api_client,
    metadata=metadata,
    use_dlt_api=True   # Use new data source API
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compare Legacy vs DLT-compatible Data Extraction

# COMMAND ----------

print("=== Legacy Approach ===")
try:
    landing_path_legacy, ingestion_date_legacy = handler_legacy.ingest_to_landing()
    print(f"Legacy - Landing path: {landing_path_legacy}")
    print(f"Legacy - Ingestion date: {ingestion_date_legacy}")
except Exception as e:
    print(f"Legacy extraction failed: {e}")

print("\n=== DLT-compatible Approach ===")
try:
    landing_path_dlt, ingestion_date_dlt = handler_dlt.ingest_to_landing()
    print(f"DLT - Landing path: {landing_path_dlt}")
    print(f"DLT - Ingestion date: {ingestion_date_dlt}")
except Exception as e:
    print(f"DLT extraction failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test Custom Data Source API Directly

# COMMAND ----------

# Test the new data source API directly
mlb_data_api = create_mlb_data_source_api()

print("Available data sources:", mlb_data_api.get_available_sources())

# Extract teams data with metadata
teams_result = mlb_data_api.extract_data("teams")
print(f"\nExtraction metadata:")
print(f"- Source: {teams_result['metadata'].source_name}")
print(f"- Status: {teams_result['metadata'].status}")
print(f"- Record count: {teams_result['metadata'].record_count}")
print(f"- Extraction time: {teams_result['metadata'].extraction_time}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ETL Teams Workflow (Choose Your Approach)
# MAGIC 
# MAGIC You can now choose between legacy and DLT-compatible approaches.

# COMMAND ----------

# Choose which handler to use for the workflow
use_dlt_approach = True  # Set to False for legacy approach

if use_dlt_approach:
    handler = handler_dlt
    print("Using DLT-compatible approach")
else:
    handler = handler_legacy
    print("Using legacy approach")

try:
    # Ingest raw json into volume landing zone
    landing_path, ingestion_date = handler.ingest_to_landing()

    # Load raw json into bronze table
    loaded_in_bronze = handler.load_to_bronze(landing_path, ingestion_date)
    
    print(f"✓ Data successfully processed using {'DLT-compatible' if use_dlt_approach else 'legacy'} approach")
    
except Exception as e:
    print(f"✗ Workflow failed: {e}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC #### Next Steps: Delta Live Tables Pipeline
# MAGIC 
# MAGIC To use the full DLT pipeline:
# MAGIC 1. Create a DLT pipeline using the configuration in `pipelines/mlb_teams_dlt_config.json`
# MAGIC 2. Point the pipeline to `pipelines/mlb_teams_dlt_pipeline.py`
# MAGIC 3. Run the pipeline to generate Bronze/Silver/Gold tables automatically
# MAGIC 4. Query the results from `mlb.dlt.teams_bronze`, `mlb.dlt.teams_silver`, and `mlb.dlt.teams_gold`
