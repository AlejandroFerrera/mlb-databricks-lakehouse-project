"""
Custom MLB Data Source API for Delta Live Tables

This module provides a reusable and extensible data source API for MLB data
that can be used with Delta Live Tables pipelines.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import json

from utils.mlb_api_client import MlbApiClient
from utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class DataSourceConfig:
    """Configuration for a data source endpoint."""
    name: str
    endpoint: str
    params: Dict[str, Any]
    fields: List[str]
    refresh_interval: str = "daily"  # daily, hourly, weekly
    

@dataclass
class DataSourceMetadata:
    """Metadata for data source execution."""
    source_name: str
    extraction_time: datetime
    record_count: int
    status: str
    

class MLBDataSource(ABC):
    """Abstract base class for MLB data sources."""
    
    def __init__(self, api_client: MlbApiClient):
        self.api_client = api_client
        
    @abstractmethod
    def extract(self, config: DataSourceConfig) -> Dict[str, Any]:
        """Extract data from the source."""
        pass
    
    @abstractmethod
    def get_metadata(self) -> DataSourceMetadata:
        """Get metadata about the last extraction."""
        pass


class MLBTeamsDataSource(MLBDataSource):
    """Data source for MLB Teams data."""
    
    def __init__(self, api_client: MlbApiClient):
        super().__init__(api_client)
        self._last_extraction_metadata = None
    
    def extract(self, config: DataSourceConfig) -> Dict[str, Any]:
        """
        Extract teams data from MLB API.
        
        Args:
            config: Data source configuration
            
        Returns:
            Dictionary containing teams data and metadata
        """
        try:
            extraction_time = datetime.now()
            teams_data = self.api_client.get_teams()
            
            # Add extraction metadata to each record
            enriched_data = []
            for team in teams_data:
                team['extraction_time'] = extraction_time.isoformat()
                team['source_name'] = config.name
                enriched_data.append(team)
            
            self._last_extraction_metadata = DataSourceMetadata(
                source_name=config.name,
                extraction_time=extraction_time,
                record_count=len(enriched_data),
                status="success"
            )
            
            logger.info(f"Successfully extracted {len(enriched_data)} teams from MLB API")
            
            return {
                "data": enriched_data,
                "metadata": self._last_extraction_metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to extract teams data: {str(e)}")
            self._last_extraction_metadata = DataSourceMetadata(
                source_name=config.name,
                extraction_time=datetime.now(),
                record_count=0,
                status=f"error: {str(e)}"
            )
            raise
    
    def get_metadata(self) -> DataSourceMetadata:
        """Get metadata about the last extraction."""
        return self._last_extraction_metadata


class MLBDataSourceAPI:
    """
    Central API for managing MLB data sources.
    
    This class provides a unified interface for accessing different MLB data sources
    and can be easily extended to support new endpoints.
    """
    
    def __init__(self):
        self.api_client = MlbApiClient()
        self._data_sources = {
            "teams": MLBTeamsDataSource(self.api_client)
        }
        self._configs = self._get_default_configs()
    
    def _get_default_configs(self) -> Dict[str, DataSourceConfig]:
        """Get default configurations for data sources."""
        return {
            "teams": DataSourceConfig(
                name="mlb_teams",
                endpoint="/v1/teams",
                params={"sportId": 1},
                fields=[
                    'teams', 'id', 'abbreviation', 'teamName', 
                    'locationName', 'division', 'name', 'sport', 'league'
                ],
                refresh_interval="daily"
            )
        }
    
    def get_data_source(self, source_name: str) -> MLBDataSource:
        """
        Get a data source by name.
        
        Args:
            source_name: Name of the data source
            
        Returns:
            Data source instance
            
        Raises:
            ValueError: If source_name is not supported
        """
        if source_name not in self._data_sources:
            raise ValueError(f"Data source '{source_name}' not supported. "
                           f"Available sources: {list(self._data_sources.keys())}")
        
        return self._data_sources[source_name]
    
    def extract_data(self, source_name: str, config: Optional[DataSourceConfig] = None) -> Dict[str, Any]:
        """
        Extract data from a specified source.
        
        Args:
            source_name: Name of the data source
            config: Optional custom configuration (uses default if not provided)
            
        Returns:
            Dictionary containing extracted data and metadata
        """
        data_source = self.get_data_source(source_name)
        config = config or self._configs[source_name]
        
        return data_source.extract(config)
    
    def get_available_sources(self) -> List[str]:
        """Get list of available data sources."""
        return list(self._data_sources.keys())
    
    def add_data_source(self, name: str, source: MLBDataSource, config: DataSourceConfig):
        """
        Add a new data source.
        
        Args:
            name: Name for the data source
            source: Data source instance
            config: Configuration for the data source
        """
        self._data_sources[name] = source
        self._configs[name] = config
        logger.info(f"Added data source: {name}")


# Convenience function for easy integration with DLT
def create_mlb_data_source_api() -> MLBDataSourceAPI:
    """Create and return an MLB Data Source API instance."""
    return MLBDataSourceAPI()


# Example usage for DLT integration
def get_teams_data() -> Dict[str, Any]:
    """
    Get teams data for use in DLT pipelines.
    
    Returns:
        Dictionary containing teams data
    """
    api = create_mlb_data_source_api()
    return api.extract_data("teams")


if __name__ == "__main__":
    # Test the data source API
    api = create_mlb_data_source_api()
    
    print("Available data sources:", api.get_available_sources())
    
    # Extract teams data
    result = api.extract_data("teams")
    print(f"Extracted {result['metadata'].record_count} teams")
    print(f"Status: {result['metadata'].status}")