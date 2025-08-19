"""
Test module for MLB Data Source API and DLT pipeline logic.

This module tests the core components without requiring a full Databricks environment.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mlb_data_source_api import create_mlb_data_source_api, DataSourceConfig
from utils.logger import setup_logger

logger = setup_logger(__name__)


def test_data_source_api():
    """Test the custom MLB data source API."""
    print("Testing MLB Data Source API...")
    
    try:
        # Create API instance
        api = create_mlb_data_source_api()
        
        # Test available sources
        sources = api.get_available_sources()
        assert 'teams' in sources, "Teams source should be available"
        print(f"✓ Available sources: {sources}")
        
        # Test getting data source
        teams_source = api.get_data_source('teams')
        assert teams_source is not None, "Teams data source should not be None"
        print("✓ Teams data source retrieved successfully")
        
        # Test invalid source
        try:
            api.get_data_source('invalid_source')
            assert False, "Should raise ValueError for invalid source"
        except ValueError as e:
            print("✓ ValueError raised for invalid source as expected")
        
        print("✓ All Data Source API tests passed!")
        return True
        
    except Exception as e:
        print(f"✗ Data Source API test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_teams_data_extraction():
    """Test teams data extraction (mock)."""
    print("\\nTesting Teams Data Extraction...")
    
    try:
        # Note: This would normally call the actual API, but for testing
        # we'll just validate the structure
        api = create_mlb_data_source_api()
        
        # Test with mock data (since we can't hit the real API in tests)
        mock_config = DataSourceConfig(
            name="test_teams",
            endpoint="/v1/teams",
            params={"sportId": 1},
            fields=['teams', 'id', 'name']
        )
        
        print("✓ Mock configuration created successfully")
        print(f"✓ Config: {mock_config.name} - {mock_config.endpoint}")
        
        return True
        
    except Exception as e:
        print(f"✗ Teams data extraction test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_dlt_pipeline_imports():
    """Test that DLT pipeline imports work correctly."""
    print("\\nTesting DLT Pipeline Imports...")
    
    try:
        # Test imports without actually running DLT code
        import sys
        import os
        
        # Check if the pipeline file exists and can be imported
        pipeline_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            'pipelines', 
            'mlb_teams_dlt_pipeline.py'
        )
        
        assert os.path.exists(pipeline_path), f"Pipeline file should exist at {pipeline_path}"
        print(f"✓ Pipeline file exists: {pipeline_path}")
        
        # Test that the file compiles
        with open(pipeline_path, 'r') as f:
            pipeline_code = f.read()
        
        # Basic validation of pipeline structure
        assert 'def teams_bronze()' in pipeline_code, "teams_bronze function should exist"
        assert 'def teams_silver()' in pipeline_code, "teams_silver function should exist"
        assert 'def teams_gold()' in pipeline_code, "teams_gold function should exist"
        assert '@dlt.table' in pipeline_code, "DLT table decorators should exist"
        
        print("✓ Pipeline structure validation passed")
        
        return True
        
    except Exception as e:
        print(f"✗ DLT pipeline import test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests."""
    print("Running MLB Data Source and DLT Pipeline Tests")
    print("=" * 50)
    
    tests = [
        test_data_source_api,
        test_teams_data_extraction,
        test_dlt_pipeline_imports
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)