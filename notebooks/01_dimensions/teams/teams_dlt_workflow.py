# Databricks notebook source
# MAGIC %md
# MAGIC # MLB Teams Delta Live Tables Workflow
# MAGIC 
# MAGIC This notebook demonstrates how to work with the new Delta Live Tables pipeline
# MAGIC for MLB teams data using the custom data source API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Custom Data Source API

# COMMAND ----------

from utils.mlb_data_source_api import create_mlb_data_source_api, get_teams_data
from utils.logger import setup_logger

logger = setup_logger(__name__)

# Create the data source API instance
mlb_api = create_mlb_data_source_api()

print("Available data sources:", mlb_api.get_available_sources())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Data Extraction
# MAGIC 
# MAGIC Test the custom data source API to ensure it's working correctly.

# COMMAND ----------

# Extract teams data using the API
try:
    teams_result = mlb_api.extract_data("teams")
    
    print(f"Extraction Status: {teams_result['metadata'].status}")
    print(f"Record Count: {teams_result['metadata'].record_count}")
    print(f"Extraction Time: {teams_result['metadata'].extraction_time}")
    
    # Show sample data
    if teams_result['data']:
        print("\\nSample team data:")
        print(teams_result['data'][0])
        
except Exception as e:
    print(f"Error extracting data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View DLT Pipeline Tables
# MAGIC 
# MAGIC Once the DLT pipeline has run, you can query the generated tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if DLT tables exist
# MAGIC SHOW TABLES IN mlb.dlt

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Bronze table (if exists)
# MAGIC SELECT * FROM mlb.dlt.teams_bronze LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Silver table (if exists)
# MAGIC SELECT * FROM mlb.dlt.teams_silver LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Gold table (if exists)
# MAGIC SELECT 
# MAGIC   team_id,
# MAGIC   full_team_name,
# MAGIC   team_key,
# MAGIC   league_name,
# MAGIC   division_name,
# MAGIC   effective_date,
# MAGIC   is_current
# MAGIC FROM mlb.dlt.teams_gold 
# MAGIC ORDER BY league_name, division_name, team_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring
# MAGIC 
# MAGIC Monitor data quality metrics from the DLT pipeline.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check data quality metrics (if DLT expectations are configured)
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_teams,
# MAGIC   COUNT(DISTINCT league_name) as leagues,
# MAGIC   COUNT(DISTINCT division_name) as divisions,
# MAGIC   MAX(extracted_at) as latest_extraction
# MAGIC FROM mlb.dlt.teams_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring
# MAGIC 
# MAGIC Use the following cells to monitor pipeline performance and status.

# COMMAND ----------

# Query pipeline event logs (example - actual table name may vary)
# This would typically be available in the DLT system tables
display(spark.sql("""
SELECT 
  timestamp,
  level,
  message,
  details
FROM system.access.audit 
WHERE request_params.pipeline_name = 'mlb_teams_pipeline'
ORDER BY timestamp DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Legacy Comparison
# MAGIC 
# MAGIC Compare with the old manual ETL approach to validate data consistency.

# COMMAND ----------

# Optional: Compare with legacy tables if they exist
try:
    legacy_count = spark.sql("SELECT COUNT(*) as count FROM mlb.bronze.teams").collect()[0]['count']
    dlt_count = spark.sql("SELECT COUNT(*) as count FROM mlb.dlt.teams_bronze").collect()[0]['count']
    
    print(f"Legacy Bronze table count: {legacy_count}")
    print(f"DLT Bronze table count: {dlt_count}")
    print(f"Difference: {abs(legacy_count - dlt_count)}")
    
except Exception as e:
    print(f"Could not compare with legacy tables: {e}")