from functions import *
import time
import datetime
import logging 
import yaml
import psycopg2

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


if __name__ == "__main__":

    config = load_config('./config.yaml')

    logging.basicConfig(
        filename="data_info.log",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    try:

        service = init_google_sheets_api(config)
        spreadsheet_id = config["file_id"]
        range_name = 'bow'
        

        data = extract_data_from_sheets(service, spreadsheet_id, range_name)
        if not data.empty:
            connection= init_db_connection(config)
            insert_data_to_db(data, connection, config['schema_name'], config['staging_table_name'])
        else:
            logging.info("No data to load.")

    except Exception as e:
            logging.error(f"Error in ETL process: {e}", exc_info=True)

    finally:
        logging.info("ETL process completed.")  