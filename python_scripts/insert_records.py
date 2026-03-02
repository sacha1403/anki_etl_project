from extraction_script import extract_and_transform
import psycopg2
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from copy_db_to_env import copy_db


def get_db_credentials():
    try:
        pg_user = os.getenv("POSTGRES_USER")
        pg_password = os.getenv("POSTGRES_PASSWORD")
        pg_db = os.getenv("POSTGRES_DB")
        pg_host = os.getenv("POSTGRES_HOST")
        pg_port = os.getenv("POSTGRES_PORT")
        if not all([pg_user, pg_password, pg_db, pg_host, pg_port]):
            raise ValueError("One or more POSTGRES_* environment variables are not set.")
        return {
            "host": pg_host,
            "port": pg_port,
            "user": pg_user,
            "password": pg_password,
            "database": pg_db
            }
    except Exception as e:
        logging.error(f"Error retrieving database credentials: {e}")
        raise

def connect_to_db(pg_creds):
    logging.info("Connecting to the postgresql database...")
    try:
        conn = psycopg2.connect(
        host='postgres',                #pg_creds['host'],
        port=pg_creds['port'],
        user=pg_creds['user'],
        password=pg_creds['password'],
        database=pg_creds['database']
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise

def insert_new_revlog_records(conn, revlog_df):
    if revlog_df.empty:
        logging.info("No new revlog records to insert.")
        return
    try:
        cursor = conn.cursor()
        for _, row in revlog_df.iterrows():
            cursor.execute('''
                INSERT INTO anki_raw.raw_revlog (id, card_id, ease, ivl, lastIvl, time, type, review_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                card_id = EXCLUDED.card_id,
                ease = EXCLUDED.ease,
                ivl = EXCLUDED.ivl,
                lastIvl = EXCLUDED.lastIvl,
                time = EXCLUDED.time,
                type = EXCLUDED.type,
                review_date = EXCLUDED.review_date;'''
                , (
                row['id'],
                row['cid'],
                row['ease'],
                row['ivl'],
                row['lastIvl'],
                row['time'],
                row['type'],
                row['review_date']
            ))
        conn.commit()
        logging.info(f"Inserted {len(revlog_df)} new revlog records.")
    except Exception as e:
        logging.error(f"Failed to insert revlog records: {e}")
        conn.rollback()
        raise

def insert_new_cards_records(conn, cards_df):
    if cards_df.empty:
        logging.info("No new cards records to insert.")
        return
    try:
        cursor = conn.cursor()
        for _,row in cards_df.iterrows():
            cursor.execute('''
                INSERT INTO anki_raw.raw_cards (id, note_id, modif_date, type, due, interval, factor, reps, lapses, reps_left)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT (id) DO UPDATE SET
                note_id = EXCLUDED.note_id,
                modif_date = EXCLUDED.modif_date,
                type = EXCLUDED.type,
                due = EXCLUDED.due,
                interval = EXCLUDED.interval,
                factor = EXCLUDED.factor,
                reps = EXCLUDED.reps,
                lapses = EXCLUDED.lapses,
                reps_left = EXCLUDED.reps_left;'''
                , (
                row['id'],
                row['nid'],
                row['mod'],
                row['type'],
                row['due'],
                row['ivl'],
                row['factor'],
                row['reps'],
                row['lapses'],
                row['left']
            ))
        conn.commit()
        logging.info(f"Inserted {len(cards_df)} new cards records.")
    except Exception as e:
        logging.error(f"Failed to insert cards records: {e}")
        conn.rollback()
        raise

def insert_new_notes_records(conn, notes_df):
    if notes_df.empty:
        logging.info("No new notes records to insert.")
        return
    try:
        cursor = conn.cursor()
        for _,row in notes_df.iterrows():
            cursor.execute('''
                INSERT INTO anki_raw.raw_notes (id, modif_date, word, language)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                modif_date = EXCLUDED.modif_date,
                word = EXCLUDED.word,
                language = EXCLUDED.language;'''
                , (
                row['id'],
                row['mod'],
                row['cleaned_field'],
                row['language']
            ))
        conn.commit()
        logging.info(f"Inserted {len(notes_df)} new notes records.")
    except Exception as e:
        logging.error(f"Failed to insert notes records: {e}")
        conn.rollback()
        raise

def update_revlog_high_watermarks(conn):
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE anki_raw.etl_high_watermarks
        SET last_processed = GREATEST(
            last_processed,
            (SELECT MAX(id) FROM anki_raw.raw_revlog)
        )
        WHERE table_name = 'revlog';
    ''')
    conn.commit()

def update_cards_high_watermarks(conn):
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE anki_raw.etl_high_watermarks
        SET last_processed = GREATEST(
            last_processed,
            (SELECT MAX(modif_date) FROM anki_raw.raw_cards)
        )
        WHERE table_name = 'cards';
    ''')
    conn.commit()

def update_notes_high_watermarks(conn):
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE anki_raw.etl_high_watermarks
        SET last_processed = GREATEST(
            last_processed,
            (SELECT MAX(modif_date) FROM anki_raw.raw_notes)
        )
        WHERE table_name = 'notes';
    ''')
    conn.commit()

def insert_data(conn, data):
    print("Inserting data into the database...")
    insert_new_revlog_records(conn, data['revlog'])
    update_revlog_high_watermarks(conn)
    insert_new_cards_records(conn, data['cards'])
    update_cards_high_watermarks(conn)
    insert_new_notes_records(conn, data['notes'])
    update_notes_high_watermarks(conn)

def get_high_watermarks(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM anki_raw.etl_high_watermarks;")
    high_watermarks = cursor.fetchall()
    return {
        row[0]: row[1] for row in high_watermarks
    }

def pipeline():
    try:
        load_dotenv()
        copy_db()
        pg_creds = get_db_credentials()
        conn = connect_to_db(pg_creds)
        high_watermarks = get_high_watermarks(conn)
        data = extract_and_transform(high_watermarks)
        insert_data(conn, data)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")