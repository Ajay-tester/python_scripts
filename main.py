import os
import oracledb
from pathlib import Path
from etl_validation.count_validation import count_check
from etl_validation.accuracy_validation import accuracy_check
from etl_validation.duplicate_validation import duplicate_check
from etl_validation.null_check_validation import Null_check
from etl_validation.table_difference_validation import difference_check
from etl_validation.logger import log_message

if __name__ == "__main__":
    # Initialize Oracle Client
    try:
        oracledb.init_oracle_client(lib_dir=r"C:\\Users\\tgt839\\Downloads\\instantclient-basic-windows.x64-11.2.0.4.0\\instantclient_11_2\\")
    except Exception as e:
        print(f"Error initializing Oracle Client: {e}")
        exit(1)

    # Database configuration
    db_config = {
        "username": os.getenv("DB_USERNAME", "system"),
        "password": os.getenv("DB_PASSWORD", "system"),  # Replace with env variables
        "dsn": os.getenv("DB_DSN", "localhost:1521/xe")
    }

    # Test database connection
    try:
        connection = oracledb.connect(
            user=db_config["username"],
            password=db_config["password"],
            dsn=db_config["dsn"]
        )
        connection.close()
    except Exception as e:
        log_message("FAIL", f"Error connecting to the database: {e}")
        exit(1)

    # Source and Target tables
    source_table = "HR.EMPLOYEES"
    target_table = "TARGET.TARGET_EMPLOYEES"

    # Mandatory columns for the Null check
    mandatory_columns_table = "mandatory_columns"

    # Log file paths
    log_dir = Path("log_files")
    log_dir.mkdir(exist_ok=True)  # Create log directory if it doesn't exist

    count_log_path = log_dir / "count_log.csv"
    accuracy_log_path = log_dir / "accuracy_log.csv"
    duplicate_log_path = log_dir / "duplicate_log.csv"
    not_null_log_path = log_dir / "not_null_log.csv"
    table_difference_log_path = log_dir / "table_difference_log.csv"

    # Run validations
    validations = [
        ("Count Validation", count_check, [source_table, target_table, db_config, count_log_path]),
        ("Duplicate Check", duplicate_check, [target_table, db_config, duplicate_log_path]),
        # ("Accuracy Validation", accuracy_check, [source_table, target_table, db_config, accuracy_log_path]),
        # ("Null Check", Null_check, [target_table, mandatory_columns_table, db_config, not_null_log_path]),
        # ("Table Difference Validation", difference_check, [source_table, target_table, db_config, table_difference_log_path]),
    ]

    for name, func, args in validations:
        try:
            status, message = func(*args)
            log_message(status, message)
        except Exception as e:
            log_message("FAIL", f"Error during {name}: {e}")
