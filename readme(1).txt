User Guide: The Universal Log Parser

This guide explains how to use the parser.py script. The main principle
is that you never edit the Python code. All control comes from the
commands you run and the files you place in the project folders.

Part 1: Initial Setup (Do This Only Once)

Before you can run any commands, you need to set up your project folder
and install the necessary software.

1.  Create Your Project Folder Structure:

    -   Create a main folder for your project (e.g., my_parser_project).
    -   Inside that folder, create another folder named exactly
        input_logs.

2.  Add the Necessary Files:

    -   Save the Python code as parser.py inside the main
        my_parser_project folder.

    -   Create a file named requirements.txt inside the same folder and
        put this single line in it:

        pandas==2.2.2

Your folder structure must look like this:

    my_parser_project/
    ├── input_logs/
    ├── parser.py
    └── requirements.txt

3.  Install the Required Library:

    -   Open your terminal or command prompt.

    -   Navigate into your project folder: cd path/to/my_parser_project

    -   Run the installation command:

        pip install -r requirements.txt

You are now fully set up.

Part 2: The Two Core Commands

The script has two main commands that you run from your terminal.

Command 1: –generate-samples (For First-Time Use)

This command automatically creates the “control panel” (config.json) and
populates your input_logs folder with sample files so you can see how
everything works.

How to Run:

    python parser.py --generate-samples

What it Does:

-   Creates config.json: This critical file is created in your project
    folder. It defines all the log formats the parser can understand.
-   Creates Sample Logs: It adds two sample log files
    (sample_pipe_delimited_bank_log.log and sample_modern_json_log.log)
    inside your input_logs folder.

After running this, your project folder will look like this:

    my_parser_project/
    ├── input_logs/
    │   ├── sample_pipe_delimited_bank_log.log
    │   └── sample_modern_json_log.log
    ├── config.json
    ├── parser.py
    └── requirements.txt

Command 2: Main Processing (The One You Use Most)

This command tells the script to find all log files in the input_logs
folder, parse them, and load the clean data into a database.

How to Run:

    python parser.py

What it Does:

-   Scans input_logs: It looks at every file inside the input_logs
    folder.
-   Detects Formats: For each file, it compares the lines against the
    format “recipes” in your config.json file to find a match.
-   Parses and Cleans: It processes all recognized files in parallel for
    maximum speed.
-   Creates Database: It creates a single database file named
    master_bank_logs.db in your project folder. This file contains all
    the clean, combined data from every log file.

Part 3: A Typical Workflow

Here is how you would use the script in practice.

Step 1: Generate the Config File (Only if it’s your first time)

    python parser.py --generate-samples

Step 2: Add Your Own Log Files

-   Drag and drop your own log files (e.g., my_transactions.csv,
    server.log) into the input_logs folder.
-   You can delete the sample log files if you wish.

Step 3: Check and Update config.json (If Necessary)

-   Open config.json with a text editor.
-   Does it have a format definition that matches your log files?
    -   If yes, you don’t need to do anything.
    -   If no, you need to add a new format definition. (See Part 4 for
        instructions).

Step 4: Run the Parser

    python parser.py

The script will now process all the files in the input_logs folder and
update the master_bank_logs.db with the results.

Part 4: How to Add a New Custom Log Format

This is the most powerful feature. You can teach the parser to read any
text-based log format by editing the config.json file.

Let’s say you have a new log file with lines that look like this:

    [LOG] 2025-08-24 15:30:12 | ID=T9876 | ACTION=WITHDRAW | AMT=-75.50

Steps:

1.  Open config.json.
2.  Go to the "log_formats" list.
3.  Copy an existing format block (like the pipe_delimited one) and
    paste it at the end of the list (make sure to add a comma after the
    preceding block).
4.  Modify the new block to match your file:

    {
        "name": "custom_server_log",
        "regex": "\[LOG\] (?P<timestamp>[\d\s-:]+) \| ID=(?P<transaction_id>\w+) \| ACTION=(?P<type>\w+) \| AMT=(?P<amount>-?[\d.]+)",
        "datetime_format": "%Y-%m-%d %H:%M:%S",
        "example_line_format": ""
    }

-   "name": Give it a unique, descriptive name. e.g.,
    "custom_server_log".
-   "regex": Write a regular expression with named capture groups
    (?P<name>...).
-   "datetime_format": For this example, %Y-%m-%d %H:%M:%S.

Save config.json. That’s it! The parser is now upgraded. The next time
you run python parser.py, it will be able to read and process your new
log file format automatically.

Part 5: Accessing Your Clean Data

The final output is the master_bank_logs.db file. This is a standard
SQLite database file. You can open it with many free tools.

The best tool for beginners is DB Browser for SQLite:

1.  Download it from sqlitebrowser.org.
2.  Open the application and click “Open Database”.
3.  Navigate to your project folder and select master_bank_logs.db.
4.  Go to the “Browse Data” tab.

You will see all your clean, processed, and combined data in a single
table, ready for analysis.
