import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data(file_path):


    try:

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)


        return data
    
    except FileNotFoundError as fnf_error:
        logger.error(f"File not found: {fnf_error}")
        raise
    
    except json.JSONDecodeError as json_error:
        logger.error(f"Error decoding JSON: {json_error}")
        raise