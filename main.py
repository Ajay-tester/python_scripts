import os
import oracledb
from pathlib import Path
from dotenv import load_dotenv
from etl_validation.count_validation import count_check
from etl_validation.accuracy_validation import accuracy_check
from etl_validation.duplicate_validation import duplicate_check
from etl_validation.null_check_validation import Null_check
# from etl_validation.table_difference_validation import difference_check
from etl_validation.logger import log_message

# Load environment variables early
load_dotenv()

if __name__ == "__main__":
    # Initialize Oracle Client
    try:
        oracle_client_path = os.getenv("ORACLE_CLIENT_PATH")
        if not oracle_client_path:
            raise ValueError("Oracle Client Path is not defined in the .env file.")
        oracledb.init_oracle_client(lib_dir=oracle_client_path)
    except Exception as e:
        print(f"Error initializing Oracle Client: {e}")
        exit(1)

    # Database configuration
    db_config = {
        "username": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "dsn": os.getenv("DB_DSN"),
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
    source_table = os.getenv("SOURCE_TABLE", "HR.EMPLOYEES")
    target_table = os.getenv("TARGET_TABLE", "TARGET.TARGET_EMPLOYEES")

    # Mandatory columns for the Null check
    mandatory_columns_table = os.getenv("MANDATORY_COLUMNS_FILE", "CSV_files/Not_null_columns.csv")

    # Log file paths
    log_dir = Path(os.getenv("LOG_DIR", "log_files"))
    log_dir.mkdir(exist_ok=True)  # Create log directory if it doesn't exist

    count_log_path = log_dir / os.getenv("COUNT_LOG_FILE", "count_log.csv")
    accuracy_log_path = log_dir / os.getenv("ACCURACY_LOG_FILE", "accuracy_log.csv")
    duplicate_log_path = log_dir / os.getenv("DUPLICATE_LOG_FILE", "duplicate_log.csv")
    not_null_log_path = log_dir / os.getenv("NOT_NULL_LOG_FILE", "not_null_log.csv")
    table_difference_log_path = log_dir / os.getenv("TABLE_DIFFERENCE_LOG_FILE", "table_difference_log.csv")

    # Run validations
    validations = [
        ("Count Validation", count_check, [source_table, target_table, db_config, count_log_path]),
        ("Duplicate Check", duplicate_check, [target_table, db_config, duplicate_log_path]),
        ("Null Check", Null_check, [target_table, mandatory_columns_table, db_config, not_null_log_path]),
        #("Accuracy Validation", accuracy_check, [source_table, target_table, db_config, accuracy_log_path]),
        #("Table Difference Validation", difference_check, [source_table, target_table, db_config, table_difference_log_path]),
    ]

    for name, func, args in validations:
        try:
            status, message = func(*args)
            log_message(status, message)
        except Exception as e:
            log_message("FAIL", f"Error during {name}: {e}")
