import sqlite3
import re
from dotenv import load_dotenv
import csv
import os
import logging
from datetime import datetime, timedelta
from lang_detect_FT import detect_language_for_note
import pandas as pd
import time


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def get_anki_db_path():
    anki_db_path = os.getenv("ANKI_DB_PATH")
    if not anki_db_path:
        raise ValueError("ANKI_DB_PATH environment variable is not set.")
    return anki_db_path

def connect_to_anki_db(db_path):
    """Establishes a read-only connection to the Anki SQLite database."""
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) # sqlite3.connect(r"/mnt/c/Users/sacha/AppData/Roaming/Anki2/Utilisateur 1/collection.anki2")
        conn.row_factory = sqlite3.Row
        logging.info("Connected to Anki DB successfully.")
        if conn is None:
            raise Exception("Failed to connect to Anki DB. Aborting extraction.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to Anki DB: {e}")
        raise


def extract_anki_data(conn_sqlite,high_watermarks):
    try:
        dfs = {}

        tables_to_extract = {
            'cards': 'id, nid, mod, type, due, ivl, factor, reps, lapses, left',
            'notes': 'id, mod, flds',
            'revlog': 'id, cid, ease, ivl, lastIvl, time, type',
        }

        for table_name, columns in tables_to_extract.items():
            logging.info(f"Extracting data from Anki table: {table_name}")
            value = 0
            if high_watermarks[table_name] is not None:                  
                value = high_watermarks[table_name]

            query = f"SELECT {columns} FROM {table_name}"
            if table_name in ["cards", "notes"]:
                query += f" WHERE mod > {value}"
            if table_name in ["revlog"]:
                query += f" WHERE id > {value}"

            dfs[table_name] = pd.read_sql_query(query, conn_sqlite)
            logging.info(f"Extracted {len(dfs[table_name])} rows from {table_name}")

        conn_sqlite.close()
        return dfs
    except Exception as e:
        logging.error(f"Error extracting data from Anki DB: {e}")
        raise

def transform_data(dfs):
    if len(dfs['revlog'] != 0):
        dfs['revlog'] = transform_revlog_table(dfs['revlog'])
    if len(dfs['notes'] != 0):
        dfs['notes'] = transform_notes_table(dfs['notes'])
    if len(dfs['cards'] != 0):
        dfs['cards'] = transform_cards_table(dfs['cards'])
    return dfs

def normalize_ivl_value(ivl):
    if ivl < 0:
        return -ivl # Convertion to seconds : negative values represents second
    else :
        return ivl*24*3600 # Convertion to seconds : positive value represents days

def transform_cards_table(cards_df):
    cards_df['left'] = cards_df['left'] % 1000
    cards_df['ivl'] = cards_df['ivl'].apply(normalize_ivl_value)  
    cards_df['id'] = cards_df['id'].astype(pd.Int64Dtype())  # Or .astype(int) depending on how you need to handle Nulls
    cards_df['nid'] = cards_df['nid'].astype(pd.Int64Dtype()) # Or .astype(int)
    cards_df['mod'] = cards_df['mod'].astype(pd.Int64Dtype())  # Or .astype(int)
    cards_df['factor'] = cards_df['factor'].astype(pd.Int64Dtype())  # Or .astype(int)  
    print('')
    return cards_df

def transform_revlog_table(revlog_df):
    revlog_df['review_date'] = pd.to_datetime(revlog_df['id'], unit='ms', origin='unix')
    revlog_df['time'] = revlog_df['time'] / 1000
    revlog_df['ivl'] = revlog_df['ivl'].apply(normalize_ivl_value)
    revlog_df['lastIvl'] = revlog_df['lastIvl'].apply(normalize_ivl_value)
    return revlog_df

def transform_notes_table(notes_df):
    if 'flds' in notes_df.columns and not notes_df.empty:
        notes_df['flds_split'] = notes_df['flds'].str.split('\x1f')
    else:
        notes_df['flds_split'] = None    

    if notes_df['flds_split'] is not None:
        notes_df[['language', 'cleaned_field']] = notes_df['flds_split'].apply(detect_language_for_note).tolist()
    else:
        notes_df['language'] = None
        notes_df['cleaned_field'] = None

    notes_df.drop(columns=['flds', 'flds_split'], inplace=True)
    notes_df = notes_df.fillna({'language':'NULL', 'cleaned_field': 'NULL', 'language2':'NULL', 'cleaned_field2': 'NULL'})
    return notes_df

    
def extract_and_transform(high_watermarks):
    try:
        ### call connection function in main instead than in function extract_anki_data to avoid multiple connection to the anki db
        anki_db_path = get_anki_db_path()
        logging.info(f"Attempting to connect to Anki DB at: {anki_db_path}")
        conn_sqlite = connect_to_anki_db(anki_db_path)
        extracted_data = extract_anki_data(conn_sqlite, high_watermarks)
        transformed_data = transform_data(extracted_data)
        # df.to_csv('/mnt/c/Users/sacha/Downloads/output.csv', index=False, encoding='utf-8')
        logging.info("Data extraction and transformation complete.")
        return transformed_data
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise