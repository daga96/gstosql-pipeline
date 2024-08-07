import pandas as pd
import logging
import psycopg2
from psycopg2 import sql
import psycopg2.extras
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def extract_data_from_sheets(service, spreadsheet_id, range_name):
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    
    logging.info(f"Raw values: {values}")

    if not values:
        logging.info('No data found in the sheet.')
        return pd.DataFrame()
    else:
        df = pd.DataFrame(values[1:], columns=values[0])  # Use first row as header
        return df

def init_google_sheets_api(config):
    creds = Credentials.from_service_account_file(config['service_account_file'])
    service = build('sheets', 'v4', credentials=creds)
    logging.info("Successfully connected to Google Sheets Service.")
    return service

def convert_dataframe_types(df):
    # Define columns that should be integers
    int_columns = [
        'grade', 'durability', 'damage','upgrade_max_level', 'level_required', 'sell_currency_cnt', 'normal_repair_cost_cnt', 'weight'
    ]
    
    # Convert specified columns to integers, if possible
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
   

    #take care of empty values 
    df = df.applymap(lambda x: None if x == '' else x)

    return df

def insert_data_to_db(dataframe, conn, schema, table_name):
    try:
        # Convert DataFrame types
        dataframe = convert_dataframe_types(dataframe)

        # Ensure column names match table schema
        dataframe.columns = [col.lower() for col in dataframe.columns]

        # Prepare SQL for creating table if it doesn't exist
        create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        seq VARCHAR PRIMARY KEY,
        annotation TEXT,
        name_term VARCHAR,
        category_type VARCHAR,
        grade INTEGER,
        durability INTEGER,
        damage INTEGER,
        is_sell BOOLEAN,
        sell_currency_type VARCHAR,
        sell_currency_id INTEGER,
        sell_currency_cnt NUMERIC,
        normal_repair_cost_type VARCHAR,
        normal_repair_cost_id INTEGER,
        normal_repair_cost_cnt NUMERIC,
        upgrade_max_level INTEGER,
        weight NUMERIC,
        level_required INTEGER
        )
        """).format(
            schema=sql.Identifier(schema),
            table_name=sql.Identifier(table_name)
        )
        
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
        
        # Insert data into the table
        insert_query = sql.SQL("""
        INSERT INTO {schema}.{table_name} (
            seq,
            annotation,
            name_term,
            category_type,
            grade,
            durability,
            damage,
            is_sell,
            sell_currency_type,
            sell_currency_id,
            sell_currency_cnt,
            normal_repair_cost_type,
            normal_repair_cost_id,
            normal_repair_cost_cnt,
            upgrade_max_level,
            weight,
            level_required
        ) VALUES %s
        """).format(
            schema=sql.Identifier(schema),
            table_name=sql.Identifier(table_name)
        )
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]
        
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, insert_query, data_tuples)
            conn.commit()
        
        logging.info(f"Data successfully loaded into {schema}.{table_name} table.")
    except Exception as e:
        logging.error(f"Error inserting data into {schema}.{table_name} table: {e}")
        raise

def init_db_connection(config):
    try:
        conn = psycopg2.connect(
            dbname=config['db_name'],
            user=config['db_username'],
            password=config['db_password'],
            host=config['db_host'],
            port=config['db_port']
        )
        logging.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise
