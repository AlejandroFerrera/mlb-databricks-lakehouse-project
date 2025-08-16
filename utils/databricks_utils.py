import json
import os
from utils.logger import setup_logger
from databricks.sdk.runtime import *

logger = setup_logger(__name__)

def write_dictionary_to_volume_as_json(path: str, data: dict, pretty_print: bool = False) -> str:
    """
    Writes a dictionary to a volume path as a JSON file.

    Args:
        path (str): The path to the volume where the JSON file will be written.
        data (dict): The dictionary to be written as a JSON file.
    Returns:
        str: The path to the written JSON file.
    Raises:
        ValueError: If the path is not a valid Databricks Volume path starting with /Volumes/.
    """

    parent_dir = os.path.dirname(path)
    if not parent_dir.startswith('/Volumes/'):
        logger.error("Path must be a valid Databricks Volume path starting with /Volumes/")
        raise ValueError("Path must be a valid Databricks Volume path starting with /Volumes/")
    
    indent_level = 4 if pretty_print else None
    json_data = json.dumps(data, indent=indent_level)

    dbutils.fs.put(path, json_data, overwrite=True)

    return path


