import pandas as pd
import re
import sqlite3
import time
import logging
import os
import json
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import random
from datetime import datetime, timedelta
import gzip
import bz2
import lzma
import pyarrow.parquet as pq

# --- GLOBAL CONFIGURATION & SETUP ---
CONFIG_FILE = 'config.json'
DB_PATH = 'master_bank_logs.db'
TABLE_NAME = 'transactions'
INPUT_LOGS_DIR = 'input_logs' # Designated folder for user's log files

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CORE DATA CLEANING & STANDARDIZATION ---
def standardize_and_clean_df(df, source_format_name):
    """
    Takes a raw DataFrame from any source and standardizes it to the target schema.
    This is a critical function to ensure all data, regardless of origin, looks the same.
    """
    if df.empty:
        return None

    # Create a new standardized DataFrame
    clean_df = pd.DataFrame()

    # --- Column Mapping (flexible) ---
    # This section makes educated guesses about column names.
    # For a real-world scenario, this might be moved into the config.json for each source.
    column_map = {
        'timestamp': ['timestamp', 'time', 'date', 'transaction_date'],
        'transaction_id': ['transaction_id', 'txn_id', 'id', 'unique_id'],
        'description': ['description', 'desc', 'details', 'memo'],
        'amount': ['amount', 'value', 'transaction_amount'],
        'type': ['type', 'kind', 'transaction_type'],
        'balance': ['balance', 'account_balance', 'running_balance']
    }

    for target_col, potential_names in column_map.items():
        for name in potential_names:
            if name in df.columns:
                clean_df[target_col] = df[name]
                break

    # --- Data Cleaning and Transformation ---
    if 'timestamp' in clean_df.columns:
        clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'], errors='coerce')
    else: # If no timestamp, generate one to prevent errors (or could skip the row)
        clean_df['timestamp'] = pd.NaT

    if 'amount' in clean_df.columns:
        clean_df['amount'] = pd.to_numeric(clean_df['amount'], errors='coerce').fillna(0)
    else: # If no amount, default to 0
        clean_df['amount'] = 0.0

    # Fill in other missing essential columns
    for col in ['transaction_id', 'description', 'type']:
        if col not in clean_df.columns:
            clean_df[col] = 'N/A'

    if 'balance' not in clean_df.columns:
        clean_df['balance'] = 0.0
    else:
        clean_df['balance'] = pd.to_numeric(clean_df['balance'], errors='coerce').fillna(0)


    # --- Final Schema Creation ---
    clean_df['debit'] = clean_df['amount'].apply(lambda x: abs(x) if pd.notna(x) and x < 0 else 0).astype(float)
    clean_df['credit'] = clean_df['amount'].apply(lambda x: x if pd.notna(x) and x > 0 else 0).astype(float)
    clean_df['source_format'] = source_format_name
    
    clean_df.dropna(subset=['timestamp', 'transaction_id'], inplace=True)

    return clean_df.reindex(columns=['timestamp', 'transaction_id', 'type', 'description', 'debit', 'credit', 'balance', 'source_format'], fill_value=None)


# --- PARSERS FOR SPECIFIC FILE FORMATS ---

def process_text_chunk(chunk_lines, log_format_config):
    """(Original Function) Parses a chunk of text/json lines using regex or json mapping."""
    parsed_data = []
    log_format_name = log_format_config.get('name', 'unknown')

    if 'json_mapping' in log_format_config:
        mapping = log_format_config['json_mapping']
        for line in chunk_lines:
            try:
                data = json.loads(line)
                parsed_data.append({k: data.get(v) for k, v in mapping.items()})
            except (json.JSONDecodeError, KeyError):
                continue
    else:
        regex = re.compile(log_format_config['regex'])
        for line in chunk_lines:
            match = regex.match(line)
            if match:
                parsed_data.append(match.groupdict())
    
    return standardize_and_clean_df(pd.DataFrame(parsed_data), log_format_name) if parsed_data else None

def process_structured_file(file_path, file_type):
    """Processes entire structured files like CSV, Excel, Parquet."""
    logging.info(f"Processing structured {file_type} file: {file_path}")
    df = None
    try:
        if file_type == 'csv':
            df = pd.read_csv(file_path, low_memory=False)
        elif file_type == 'tsv':
            df = pd.read_csv(file_path, sep='\t', low_memory=False)
        elif file_type == 'excel':
            # This will read the first sheet by default
            df = pd.read_excel(file_path, engine='openpyxl')
        elif file_type == 'parquet':
            df = pd.read_parquet(file_path)
        elif file_type == 'sqlite':
            # Connect to the DB and try to read the largest table
            with sqlite3.connect(file_path) as con:
                tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", con)
                if not tables.empty:
                    # For this example, we assume the first table is the one we want.
                    # A more robust solution might inspect table schemas or look for a specific name.
                    table_name = tables['name'][0]
                    logging.info(f"Reading table '{table_name}' from SQLite file.")
                    df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)

        if df is not None:
            return standardize_and_clean_df(df, file_type)

    except Exception as e:
        logging.error(f"Failed to process {file_path} as {file_type}: {e}")
    return None

# --- FILE AND FORMAT HANDLING ---

def get_file_opener(file_path):
    """Returns the correct file open function for compressed files."""
    if file_path.endswith('.gz'):
        return lambda p: gzip.open(p, 'rt', encoding='utf-8', errors='ignore')
    elif file_path.endswith('.bz2'):
        return lambda p: bz2.open(p, 'rt', encoding='utf-8', errors='ignore')
    elif file_path.endswith('.xz'):
        return lambda p: lzma.open(p, 'rt', encoding='utf-8', errors='ignore')
    else:
        return lambda p: open(p, 'r', encoding='utf-8', errors='ignore')

def detect_and_process_file(file_path, log_formats, conn):
    """
    Detects the file type and orchestrates its processing, loading data into the master DB.
    Returns the number of rows processed.
    """
    file_extension = os.path.splitext(file_path)[1].lower().replace('.', '')
    # Handle compressed files by looking at the extension before the compression one
    if file_extension in ['gz', 'bz2', 'xz']:
        file_extension = os.path.splitext(os.path.splitext(file_path)[0])[1].lower().replace('.', '')

    logging.info(f"Detecting format for '{os.path.basename(file_path)}' (type: .{file_extension})...")
    rows_processed = 0

    # --- Structured Data Files (CSV, Parquet, Excel, etc.) ---
    structured_formats = {
        'csv': 'csv', 'tsv': 'tsv',
        'xlsx': 'excel', 'xls': 'excel', 'xlsm': 'excel', 'ods': 'excel',
        'parquet': 'parquet',
        'db': 'sqlite', 'sqlite': 'sqlite', 'sqlite3': 'sqlite'
    }

    if file_extension in structured_formats:
        file_type = structured_formats[file_extension]
        clean_df = process_structured_file(file_path, file_type)
        if clean_df is not None and not clean_df.empty:
            clean_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
            rows_processed = len(clean_df)
    
    # --- Text-Based/Log Files (Regex, JSON) ---
    elif file_extension in ['log', 'txt', 'json', 'ndjson', 'out', 'err']:
        opener = get_file_opener(file_path)
        with opener(file_path) as f:
            sample_lines = [next(f, '').strip() for _ in range(10)]
        
        detected_format_config = None
        for fmt in log_formats:
            # JSON format check
            if 'json_mapping' in fmt:
                try:
                    if sum(1 for line in sample_lines if line and json.loads(line)) > 3:
                        detected_format_config = fmt
                        break
                except (json.JSONDecodeError, KeyError): continue
            # Regex-based format check
            else:
                regex = re.compile(fmt['regex'])
                if sum(1 for line in sample_lines if line and regex.match(line)) > 5:
                    detected_format_config = fmt
                    break
        
        if not detected_format_config:
            logging.warning(f"Skipping '{file_path}': No matching text/json format found in config.")
            return 0
        
        logging.info(f"Processing '{file_path}' as '{detected_format_config['name']}' using multi-processing...")
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor, opener(file_path) as f:
            futures = []
            while True:
                chunk = [line for _, line in zip(range(100000), f)]
                if not chunk: break
                futures.append(executor.submit(process_text_chunk, chunk, detected_format_config))
            
            for future in as_completed(futures):
                try:
                    clean_df = future.result()
                    if clean_df is not None and not clean_df.empty:
                        clean_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                        rows_processed += len(clean_df)
                except Exception as e:
                    logging.error(f"Error processing a chunk result from '{file_path}': {e}", exc_info=True)
    
    # --- Unsupported Files ---
    else:
        logging.warning(f"Skipping '{os.path.basename(file_path)}'. Unsupported file extension: '.{file_extension}'")

    return rows_processed

# --- SAMPLE GENERATION ---

def generate_sample_files_and_config():
    """Creates a sample config.json and a wider variety of corresponding log files."""
    logging.info("Generating sample configuration and log files...")

    sample_config = {
        "log_formats": [ # Only for text-based files
            {
                "name": "pipe_delimited_bank_log",
                "regex": r"(?P<timestamp>[\d-]+-[\d:]+)\s*\|\s*TRN-ID:\s*(?P<transaction_id>\w+)\s*\|\s*TYPE:\s*(?P<type>\w+)\s*\|\s*AMT:\s*(?P<amount>-?[\d.]+)\s*\|\s*DESC:\s*(?P<description>.*?)\s*\|\s*BAL:\s*(?P<balance>[\d.]+)",
                "datetime_format": "%Y-%m-%d-%H:%M:%S"
            },
            {
                "name": "modern_json_log",
                "json_mapping": {
                    # target_column: source_field
                    "timestamp": "time", "transaction_id": "txn_id", "description": "details",
                    "amount": "value", "type": "kind", "balance": "account_balance"
                }
            }
        ]
    }
    with open(CONFIG_FILE, 'w') as f: json.dump(sample_config, f, indent=4)
    if not os.path.exists(INPUT_LOGS_DIR): os.makedirs(INPUT_LOGS_DIR)

    # --- Generate Sample Data ---
    sample_rows = []
    balance = 5000.0
    current_time = datetime.now()
    for i in range(1000):
        is_debit = random.choice([True, True, False])
        amount = round(random.uniform(-150, -5) if is_debit else random.uniform(500, 2500), 2)
        balance += amount
        ts_obj = current_time - timedelta(minutes=i*30)
        sample_rows.append({
            'timestamp': ts_obj,
            'transaction_id': f"TXN{1000000 + i}",
            'description': random.choice(["AMAZON MKTPLACE", "STARBUCKS", "PAYROLL"]),
            'amount': amount,
            'type': 'DEBIT' if is_debit else 'CREDIT',
            'balance': balance
        })
    
    df_sample = pd.DataFrame(sample_rows)

    # 1. Pipe-delimited log
    with open(os.path.join(INPUT_LOGS_DIR, "sample_pipe_bank.log"), 'w') as f:
        for row in df_sample.itertuples():
            f.write(f"{row.timestamp.strftime('%Y-%m-%d-%H:%M:%S')} | TRN-ID: {row.transaction_id} | TYPE: {row.type} | AMT: {row.amount:.2f} | DESC: {row.description} | BAL: {row.balance:.2f}\n")
    
    # 2. JSON log
    with open(os.path.join(INPUT_LOGS_DIR, "sample_modern.json"), 'w') as f:
        for row in df_sample.itertuples():
            json.dump({
                "time": row.timestamp.isoformat(), "txn_id": row.transaction_id, "details": row.description,
                "value": row.amount, "kind": row.type, "account_balance": row.balance
            }, f)
            f.write('\n')

    # 3. CSV file
    df_sample.to_csv(os.path.join(INPUT_LOGS_DIR, "sample_transactions.csv"), index=False)
    
    # 4. Excel file
    df_sample.to_excel(os.path.join(INPUT_LOGS_DIR, "sample_transactions.xlsx"), index=False, engine='openpyxl')

    # 5. Parquet file
    df_sample.to_parquet(os.path.join(INPUT_LOGS_DIR, "sample_transactions.parquet"), index=False)

    # 6. SQLite DB file
    db_file = os.path.join(INPUT_LOGS_DIR, "sample_transactions.db")
    if os.path.exists(db_file): os.remove(db_file)
    with sqlite3.connect(db_file) as con:
        df_sample.to_sql('account_statements', con, index=False, if_exists='replace')

    logging.info("\nSample files and config generated in 'input_logs/' directory.")
    logging.info("Generated: .log, .json, .csv, .xlsx, .parquet, and .db files.")


# --- MAIN ORCHESTRATOR ---
def main():
    parser = argparse.ArgumentParser(description="A universal, high-performance file parser for financial data.")
    parser.add_argument('--generate-samples', action='store_true', help="Generate a sample 'config.json' and various log files.")
    args = parser.parse_args()

    if args.generate_samples:
        generate_sample_files_and_config()
        return

    if not os.path.exists(CONFIG_FILE):
        logging.error(f"'{CONFIG_FILE}' not found! Run with '--generate-samples' first.")
        return
    with open(CONFIG_FILE, 'r') as f: config = json.load(f)
    log_formats = config['log_formats']

    if not os.path.exists(INPUT_LOGS_DIR) or not os.listdir(INPUT_LOGS_DIR):
        logging.error(f"Input directory '{INPUT_LOGS_DIR}' is empty or not found. Please add files or run with '--generate-samples'.")
        return

    log_files_to_process = [os.path.join(INPUT_LOGS_DIR, f) for f in os.listdir(INPUT_LOGS_DIR)]

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logging.info(f"Removed existing database at '{DB_PATH}'.")

    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Successfully connected to master database '{DB_PATH}'.")
    except sqlite3.Error as e:
        logging.error(f"Fatal: Could not connect to database: {e}")
        return

    logging.info(f"Starting universal parser...")
    total_rows_processed = 0
    overall_start_time = time.time()

    for file_path in log_files_to_process:
        if not os.path.isfile(file_path): continue
        
        file_start_time = time.time()
        try:
            rows_added = detect_and_process_file(file_path, log_formats, conn)
            if rows_added > 0:
                total_rows_processed += rows_added
                logging.info(f"Successfully processed '{os.path.basename(file_path)}' ({rows_added:,} rows) in {time.time() - file_start_time:.2f}s.")
                logging.info(f"Total rows in DB so far: {total_rows_processed:,}")
        except Exception as e:
            logging.error(f"A critical error occurred while processing {os.path.basename(file_path)}: {e}", exc_info=True)

    conn.close()
    logging.info("\n--- UNIVERSAL PARSING COMPLETE ---")
    logging.info(f"Total time taken: {time.time() - overall_start_time:.2f} seconds.")
    logging.info(f"Total rows processed from all files: {total_rows_processed:,}")

    if total_rows_processed > 0:
        with sqlite3.connect(DB_PATH) as conn:
            count = pd.read_sql_query(f"SELECT COUNT(*) FROM {TABLE_NAME}", conn).iloc[0, 0]
            logging.info(f"VERIFICATION: Final database contains {count:,} total rows.")
            sample_data = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} ORDER BY RANDOM() LIMIT 5", conn)
            print("\n--- Random Sample of Cleaned Data in Database ---")
            print(sample_data.to_string())
            print("-------------------------------------------------")

if __name__ == "__main__":
    main()
