from etl_validation.count_validation import count_check
from etl_validation.accuracy_validation import accuracy_check
from etl_validation.duplicate_validation import duplicate_check
from etl_validation.null_check_validation import Null_check
from etl_validation.table_difference_validation import difference_check
from etl_validation.logger import log_message

if __name__ == "__main__":
    # Database configuration
    db_config = {
        "username": "system",
        "password": "system",  # Replace with your actual password
        "dsn": "localhost:1521/xepdb1"  # Update with your database's DSN
    }

    # Source and Target tables in the database
    source_table = "HR.EMPLOYEES"
    target_table = "TARGET.TARGET_EMPLOYEES"

    # Mandatory columns for the Null check
    mandatory_columns_table = "mandatory_columns"

    # Log file paths
    count_log_path = "log_files/count_log.csv"
    accuracy_log_path = "log_files/accuracy_log.csv"
    duplicate_log_path = "log_files/duplicate_log.csv"
    not_null_log_path = "log_files/not_null_log.csv"
    table_difference_log_path = "log_files/table_difference_log.csv"

    # Count Validation
    status, message = count_check(source_table, target_table, db_config, count_log_path)
    log_message(status, message)

    # Duplicate Check
    status, message = duplicate_check(target_table, db_config, duplicate_log_path)
    log_message(status, message)

    # # Accuracy Validation
    # status, message = accuracy_check(source_table, target_table, db_config, accuracy_log_path)
    # log_message(status, message)

    # # Null Check
    # status, message = Null_check(target_table, mandatory_columns_table, db_config, not_null_log_path)
    # log_message(status, message)

    # # Table Difference Validation (Source Minus Target and Vice Versa)
    # status, message = difference_check(source_table, target_table, db_config, table_difference_log_path)
    # log_message(status, message)
