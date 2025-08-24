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

# --- GLOBAL CONFIGURATION & SETUP ---
CONFIG_FILE = 'config.json'
DB_PATH = 'master_bank_logs.db'
TABLE_NAME = 'transactions'
INPUT_LOGS_DIR = 'input_logs' # Designated folder for user's log files

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CORE PARSING AND CLEANING LOGIC (UNCHANGED) ---

def process_chunk(chunk_lines, log_format_config):
    """Parses a chunk of lines based on the provided format config and returns a clean DataFrame."""
    parsed_data = []
    log_format_name = log_format_config.get('name', 'unknown')

    if 'json_mapping' in log_format_config: # JSON parsing logic
        mapping = log_format_config['json_mapping']
        for line in chunk_lines:
            try:
                data = json.loads(line)
                parsed_data.append({
                    'timestamp': data.get(mapping['timestamp']),
                    'transaction_id': data.get(mapping['transaction_id']),
                    'description': data.get(mapping['description']),
                    'amount': data.get(mapping['amount']),
                    'type': data.get(mapping['type']),
                    'balance': data.get(mapping['balance'])
                })
            except (json.JSONDecodeError, KeyError):
                continue
    else: # Regex parsing logic
        regex = re.compile(log_format_config['regex'])
        for line in chunk_lines:
            match = regex.match(line)
            if match:
                parsed_data.append(match.groupdict())

    if not parsed_data:
        return None

    # --- Create the "Clean ASF Panel" ---
    df = pd.DataFrame(parsed_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format=log_format_config.get('datetime_format', 'mixed'), errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['balance'] = pd.to_numeric(df['balance'], errors='coerce').fillna(0) # Handle missing balance
    df['type'] = df['type'].astype('category')
    df['debit'] = df['amount'].apply(lambda x: abs(x) if pd.notna(x) and x < 0 else 0).astype(float)
    df['credit'] = df['amount'].apply(lambda x: x if pd.notna(x) and x > 0 else 0).astype(float)
    df['source_format'] = log_format_name
    
    df.dropna(subset=['timestamp', 'transaction_id'], inplace=True) # Ensure essential columns are not null

    return df.reindex(columns=['timestamp', 'transaction_id', 'type', 'description', 'debit', 'credit', 'balance', 'source_format'], fill_value=None)


# --- FILE AND FORMAT HANDLING ---

def detect_log_format(file_path, formats):
    """Reads the first few lines of a file to determine its format from the provided list."""
    logging.info(f"Detecting format for '{file_path}'...")
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            sample_lines = [next(f, '').strip() for _ in range(10)]
    except (StopIteration, FileNotFoundError):
        return None

    for fmt in formats:
        # JSON format check
        if 'json_mapping' in fmt:
            try:
                if all(json.loads(line) for line in sample_lines if line):
                    logging.info(f"Detected format: '{fmt['name']}'")
                    return fmt
            except (json.JSONDecodeError, KeyError):
                continue
        # Regex-based format check
        else:
            regex = re.compile(fmt['regex'])
            if sum(1 for line in sample_lines if line and regex.match(line)) > 5: # Match >50% of sample lines
                logging.info(f"Detected format: '{fmt['name']}'")
                return fmt
    return None

# --- SAMPLE GENERATION ---

def generate_sample_files_and_config():
    """Creates a sample config.json and corresponding log files to demonstrate usage."""
    logging.info("Generating sample configuration and log files...")

    sample_config = {
        "parser_settings": {
            "max_workers": os.cpu_count() or 1,
            "chunk_size": 100000
        },
        "log_formats": [
            {
                "name": "pipe_delimited_bank_log",
                "regex": r"(?P<timestamp>[\d-]+-[\d:]+)\s*\|\s*TRN-ID:\s*(?P<transaction_id>\w+)\s*\|\s*TYPE:\s*(?P<type>\w+)\s*\|\s*AMT:\s*(?P<amount>-?[\d.]+)\s*\|\s*DESC:\s*(?P<description>.*?)\s*\|\s*BAL:\s*(?P<balance>[\d.]+)",
                "datetime_format": "%Y-%m-%d-%H:%M:%S",
                "example_line_format": "{timestamp} | TRN-ID: {transaction_id} | TYPE: {type} | AMT: {amount:.2f} | DESC: {description} | BAL: {balance:.2f}"
            },
            {
                "name": "modern_json_log",
                "json_mapping": {
                    "timestamp": "time",
                    "transaction_id": "txn_id",
                    "description": "details",
                    "amount": "value",
                    "type": "kind",
                    "balance": "account_balance"
                },
                "datetime_format": "iso",
                "example_line_format": '{{"time": "{timestamp}", "txn_id": "{transaction_id}", "details": "{description}", "value": {amount:.2f}, "kind": "{type}", "account_balance": {balance:.2f}}}'
            }
        ]
    }

    # Write the config file
    with open(CONFIG_FILE, 'w') as f:
        json.dump(sample_config, f, indent=4)
    logging.info(f"'{CONFIG_FILE}' created successfully.")

    # Create input directory
    if not os.path.exists(INPUT_LOGS_DIR):
        os.makedirs(INPUT_LOGS_DIR)

    # Generate log files based on the new config
    for fmt in sample_config['log_formats']:
        file_path = os.path.join(INPUT_LOGS_DIR, f"sample_{fmt['name']}.log")
        logging.info(f"Generating sample file: '{file_path}'")
        with open(file_path, 'w') as f:
            balance = 5000.0
            current_time = datetime.now()
            for i in range(50000): # Smaller sample files for quick demo
                is_debit = random.choice([True, True, False])
                amount = round(random.uniform(-150, -5) if is_debit else random.uniform(500, 2500), 2)
                balance += amount
                ts_obj = current_time - timedelta(seconds=i*30)
                
                log_data = {
                    'timestamp': ts_obj.isoformat() + "Z" if fmt['datetime_format'] == 'iso' else ts_obj.strftime(fmt['datetime_format']),
                    'transaction_id': f"TXN{1000000 + i}",
                    'description': random.choice(["AMAZON MKTPLACE", "STARBUCKS", "PAYROLL"]),
                    'amount': amount,
                    'type': 'DEBIT' if is_debit else 'CREDIT',
                    'balance': balance
                }
                line = fmt['example_line_format'].format(**log_data)
                f.write(line + '\n')

    logging.info("\nSample files and config generated. You can now run 'python parser.py' to process them.")

# --- MAIN ORCHESTRATOR ---

def main():
    """Main function to orchestrate the file parsing and database loading."""
    parser = argparse.ArgumentParser(description="A universal, high-performance log parser.")
    parser.add_argument('--generate-samples', action='store_true', help="Generate a sample 'config.json' and log files.")
    args = parser.parse_args()

    if args.generate_samples:
        generate_sample_files_and_config()
        return

    if not os.path.exists(CONFIG_FILE):
        logging.error(f"'{CONFIG_FILE}' not found! Run with '--generate-samples' first.")
        return
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    settings = config['parser_settings']
    log_formats = config['log_formats']
    MAX_WORKERS, CHUNK_SIZE = settings['max_workers'], settings['chunk_size']

    if not os.path.exists(INPUT_LOGS_DIR):
        logging.error(f"Input directory '{INPUT_LOGS_DIR}' not found. Please create it and add log files.")
        return

    log_files_to_process = [os.path.join(INPUT_LOGS_DIR, f) for f in os.listdir(INPUT_LOGS_DIR) if os.path.isfile(os.path.join(INPUT_LOGS_DIR, f))]
    if not log_files_to_process:
        logging.warning(f"No log files found in '{INPUT_LOGS_DIR}'.")
        return

    # Remove old database if it exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logging.info(f"Removed existing database at '{DB_PATH}'.")

    # Establish a single database connection for the main process
    try:
        conn = sqlite3.connect(DB_PATH)
        logging.info(f"Successfully connected to database '{DB_PATH}'.")
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return

    logging.info(f"Starting universal parser with {MAX_WORKERS} workers.")
    total_rows_processed = 0
    overall_start_time = time.time()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for log_file in log_files_to_process:
            file_start_time = time.time()
            detected_format_config = detect_log_format(log_file, log_formats)
            
            if not detected_format_config:
                logging.warning(f"Skipping '{log_file}' due to unknown format.")
                continue

            logging.info(f"Processing '{log_file}' as '{detected_format_config['name']}'...")
            
            futures = []
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                while True:
                    chunk = [line for _, line in zip(range(CHUNK_SIZE), f)]
                    if not chunk: break
                    futures.append(executor.submit(process_chunk, chunk, detected_format_config))
            
            for future in as_completed(futures):
                try:
                    clean_df = future.result()
                    if clean_df is not None and not clean_df.empty:
                        # Pandas can use the standard sqlite3 connection object directly
                        clean_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                        total_rows_processed += len(clean_df)
                        logging.info(f"Loaded chunk. Total rows in DB: {total_rows_processed:,}")
                except Exception as e:
                    logging.error(f"Error processing a chunk result from '{log_file}': {e}", exc_info=True)
            
            logging.info(f"Finished processing '{log_file}' in {time.time() - file_start_time:.2f}s.")

    conn.close() # Close the database connection
    logging.info("--- UNIVERSAL PARSING COMPLETE ---")
    logging.info(f"Total time taken: {time.time() - overall_start_time:.2f} seconds.")
    logging.info(f"Total rows processed from all files: {total_rows_processed:,}")

    # --- Verification ---
    if total_rows_processed > 0:
        with sqlite3.connect(DB_PATH) as conn:
            count = pd.read_sql_query(f"SELECT COUNT(*) FROM {TABLE_NAME}", conn).iloc[0, 0]
            logging.info(f"VERIFICATION: Database contains {count:,} total rows.")
            sample_data = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} ORDER BY RANDOM() LIMIT 5", conn)
            print("\n--- Sample of Cleaned Data in Database ---")
            print(sample_data.to_string())
            print("------------------------------------------")

if __name__ == "__main__":
    main()