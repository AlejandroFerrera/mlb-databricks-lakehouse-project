# MLB Delta Live Tables Pipeline

This directory contains the Delta Live Tables (DLT) pipeline implementation for processing MLB data through Bronze, Silver, and Gold layers.

## Overview

The pipeline has been refactored to use:

1. **Custom MLB Data Source API** (`utils/mlb_data_source_api.py`) - A reusable, extensible API for accessing MLB data
2. **Delta Live Tables Pipeline** (`pipelines/mlb_teams_dlt_pipeline.py`) - Declarative data pipeline with built-in data quality
3. **DLT Configuration** (`pipelines/mlb_teams_dlt_config.json`) - Pipeline configuration and cluster settings

## Architecture

### Data Flow

```
MLB API → Custom Data Source API → DLT Pipeline → Delta Tables
                                        ↓
                                Bronze → Silver → Gold
```

### Layers

- **Bronze (Raw)**: Direct API data with minimal transformations
- **Silver (Cleaned)**: Standardized schema, data quality checks, deduplication
- **Gold (Business)**: Analytics-ready dimension with business logic

## Custom Data Source API

### Features

- **Extensible**: Easy to add new MLB endpoints (players, games, etc.)
- **Reusable**: Consistent interface across different data sources
- **Error Handling**: Robust error handling with retry logic
- **Metadata**: Rich extraction metadata for data lineage
- **DLT Compatible**: Designed to work seamlessly with Delta Live Tables

### Usage

```python
from utils.mlb_data_source_api import create_mlb_data_source_api

# Create API instance
api = create_mlb_data_source_api()

# Get available sources
sources = api.get_available_sources()  # ['teams']

# Extract data
result = api.extract_data("teams")
data = result["data"]
metadata = result["metadata"]
```

### Adding New Data Sources

To add a new data source (e.g., players):

1. Create a new data source class inheriting from `MLBDataSource`
2. Add it to the `MLBDataSourceAPI._data_sources` dictionary
3. Define the configuration in `_get_default_configs()`

## Delta Live Tables Pipeline

### Tables

1. **teams_bronze**: Raw MLB teams data from API
2. **teams_silver**: Cleaned and standardized teams data with data quality checks
3. **teams_gold**: Business-ready teams dimension with computed fields

### Data Quality

The pipeline includes built-in data quality checks:

- `@dlt.expect_all_or_drop("valid_team_id", "id IS NOT NULL")`
- `@dlt.expect_all_or_drop("valid_team_name", "name IS NOT NULL")`

### Deployment

1. Upload the pipeline files to Databricks workspace
2. Create a DLT pipeline using the configuration file
3. Schedule or trigger the pipeline

## Migration from Legacy ETL

### Backward Compatibility

The `TeamDimensionHandler` has been updated to support both approaches:

```python
# Legacy approach
handler = TeamDimensionHandler(spark, api_client, metadata, use_dlt_api=False)

# New DLT-compatible approach  
handler = TeamDimensionHandler(spark, api_client, metadata, use_dlt_api=True)
```

### Benefits of DLT Approach

1. **Declarative**: Define what you want, not how to get it
2. **Data Quality**: Built-in expectations and monitoring
3. **Auto-scaling**: Automatic cluster management
4. **Lineage**: Automatic data lineage tracking
5. **Monitoring**: Built-in pipeline monitoring and alerting
6. **Cost Optimization**: Auto-scaling and optimized compute

## Files Structure

```
pipelines/
├── mlb_teams_dlt_pipeline.py      # DLT pipeline definition
├── mlb_teams_dlt_config.json      # Pipeline configuration
└── README.md                      # This file

utils/
├── mlb_data_source_api.py         # Custom data source API
└── ...

notebooks/01_dimensions/teams/
├── teams_dlt_workflow.py          # DLT workflow notebook
├── teams_dimension_handler.py     # Updated handler (backward compatible)
└── ...
```

## Getting Started

1. **Test the Data Source API**:
   ```python
   from utils.mlb_data_source_api import create_mlb_data_source_api
   api = create_mlb_data_source_api()
   result = api.extract_data("teams")
   ```

2. **Create DLT Pipeline**:
   - Use the configuration in `mlb_teams_dlt_config.json`
   - Point to `mlb_teams_dlt_pipeline.py` as the pipeline notebook

3. **Monitor Pipeline**:
   - Use the DLT UI for monitoring
   - Query the generated tables in `mlb.dlt` schema

4. **Query Results**:
   ```sql
   SELECT * FROM mlb.dlt.teams_gold ORDER BY full_team_name
   ```

## Future Enhancements

1. **Additional Data Sources**: Players, games, statistics
2. **Streaming**: Real-time data ingestion for live games
3. **Advanced Analytics**: ML-ready features and aggregations
4. **Data Mesh**: Distributed data ownership patterns