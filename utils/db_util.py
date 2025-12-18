import os
import sqlite3
import pandas as pd
import numpy as np
import traceback
# Use the custom logger instead of the standard logging module
from utils.logging import logger


def get_db_path() -> str:
    """
    Returns the absolute path to the SQLite database file.

    The DB file is fixed to `db/4h_candle.db` under the repository root.
    """
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    db_dir = os.path.join(repo_root, "db")
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, "4h_candle.db")


# CHANGE: The function now accepts a specific path to the database.
def connect_db(path):
    """
    Establishes a connection to the SQLite database.

    :param path: The file path to the database.
    :return: A tuple containing the connection and cursor objects.
    """
    try:
        # Connect to the database. It will be created if it doesn't exist.
        conn = sqlite3.connect(path, timeout=10) # Added a timeout
        c = conn.cursor()
        logger.info(f"Successfully connected to database at {path}")
        return conn, c
    except sqlite3.Error as e:
        logger.error(f"Database connection issue: {e}")
        logger.error(traceback.format_exc())
        return None, None


# CHANGE: The function now accepts a specific path for the database.
def refresh_data(db_path: str, table_name: str, symbols: list, interval: str):
    """
    Fetches only the newest OHLCV data from the Coinalyze API and inserts
    the new rows into a single table in a local SQLite database.

    It is designed to be efficient by only fetching data since the last
    known timestamp and uses transactions to ensure data integrity.

    :param db_path: The absolute path to the SQLite database file.
    :param symbols: A list of symbols to fetch data for (e.g., ['BTCUSDT', 'ETHUSDT']).
    :param interval: The time interval for the OHLCV data (e.g., '1h', '4h', '1d').
    """
    conn = None # Initialize conn to None
    try:
        # Import here so callers can import `utils.db_util` without requiring
        # optional runtime deps (e.g. `requests`) unless they actually refresh.
        from utils.coinalyze_rest_adapter import CoinalyzeRestAdapter

        # --- Step 1: Connect to DB and set up the master table ---
        conn, c = connect_db(db_path) # CHANGE: Use the provided path
        if not conn:
            return # Exit if the database connection failed.
                
        # Create the table if it doesn't exist. The composite PRIMARY KEY is crucial.
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT,
            t INTEGER,
            o REAL,
            h REAL,
            l REAL,
            c REAL,
            v REAL,
            bv REAL,
            tx INTEGER,
            btx INTEGER,
            PRIMARY KEY (symbol, t)
        )
        """
        c.execute(create_table_sql)

        # --- Step 2: Fetch data efficiently ---
        ca = CoinalyzeRestAdapter()
        logger.info(f"Fetching data for symbols: {symbols} with interval: {interval}")
        raw_data = ca.get_ohlcv_history(symbols=symbols, interval=interval)

        # --- Step 3: Combine all data into a single DataFrame ---
        all_data_frames = []
        for data in raw_data:
            df = pd.DataFrame(data['history'])
            if not df.empty:
                df['symbol'] = data['symbol']
                # The 'interval' column is no longer needed.
                # df['interval'] = interval 
                all_data_frames.append(df)

        if not all_data_frames:
            logger.info("No data returned from API for any symbols. Nothing to insert.")
            return

        master_df = pd.concat(all_data_frames, ignore_index=True)
        
        # Get the latest timestamp from the data for verification.
        latest_timestamp = master_df['t'].max()
        latest_datetime_str = pd.to_datetime(latest_timestamp, unit='s').strftime('%Y-%m-%d %H:%M:%S UTC')


        # --- Step 4: Use a transaction for the entire database operation ---
        conn.execute('BEGIN TRANSACTION')
        
        logger.info(f"Attempting to insert {len(master_df)} rows into table: {table_name}.")
        # Use index=False as we are not using the DataFrame index as a column.
        master_df.to_sql('temporary_table', conn, if_exists='replace', index=False)

        columns = master_df.columns.tolist()
        columns_str = ', '.join([f'"{col}"' for col in columns])

        insert_sql = f'INSERT OR IGNORE INTO {table_name} ({columns_str}) SELECT {columns_str} FROM temporary_table'
        c.execute(insert_sql)
        
        rows_inserted = c.rowcount
        logger.info(f"Successfully inserted {rows_inserted} new rows. Latest timestamp from API call: {latest_datetime_str}")

        # Commit the transaction
        conn.commit()
        logger.info("Database transaction committed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the data refresh process: {e}")
        logger.error(traceback.format_exc())
        if conn:
            # If an error occurs, roll back any changes from the current transaction
            logger.warning("Rolling back database transaction.")
            conn.rollback()

    finally:
        # Ensure the database connection is always closed
        if conn:
            conn.close()
            logger.info("Database connection closed.")
