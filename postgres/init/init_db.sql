CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;

CREATE SCHEMA IF NOT EXISTS anki_raw;

-- Grant permissions to your user
GRANT ALL PRIVILEGES ON SCHEMA anki_raw TO db_user;

-- raw_revlog (for review history)
CREATE TABLE IF NOT EXISTS anki_raw.raw_revlog (
    id BIGINT PRIMARY KEY, -- Anki's revlog.id is a timestamp in ms
    card_id BIGINT,
    ease SMALLINT,
    ivl INTEGER,
    lastIvl INTEGER,
    time DECIMAL,
    type SMALLINT,
    review_date TIMESTAMP WITH TIME ZONE
);

-- raw_cards (for card data)
CREATE TABLE IF NOT EXISTS anki_raw.raw_cards (
    id BIGINT PRIMARY KEY, -- Anki's card ID
    note_id BIGINT, -- Anki's note ID
    modif_date BIGINT, -- Last modification timestamp (ms epoch)
    type SMALLINT,
    due BIGINT, -- To be determined if useful
    interval INTEGER, 
    factor BIGINT,
    reps INTEGER,
    lapses INTEGER,
    reps_left INTEGER
);

-- raw_notes (for note content)
CREATE TABLE IF NOT EXISTS anki_raw.raw_notes (
    id BIGINT PRIMARY KEY, 
    modif_date BIGINT, 
    word VARCHAR(255), -- Assuming the first field contains the word, adjust as neede
    language CHAR(4),
    word_fasttext VARCHAR(255), -- Assuming the first field contains the word, adjust as neede
    language_fasttext CHAR(4),
    time_df DECIMAL
);

-- ETL High-Watermarks table for incremental loading
CREATE TABLE IF NOT EXISTS anki_raw.etl_high_watermarks (
    table_name VARCHAR(255) PRIMARY KEY,
    last_processed BIGINT
);

-- Initialize watermarks if not present
INSERT INTO anki_raw.etl_high_watermarks (table_name, last_processed)
VALUES ('revlog', 0), ('cards', 0), ('notes', 0)
ON CONFLICT (table_name) DO NOTHING;
