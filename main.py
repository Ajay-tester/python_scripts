from etl_validation.count_validation import count_check
from etl_validation.accuracy_validation import accuracy_check
from etl_validation.duplicate_validation import duplicate_check
from etl_validation.null_check_validation import Null_check
from etl_validation.table_difference_validation import difference_check
from etl_validation.logger import log_message

if __name__ == "__main__":
    source_file_path = "CSV_files/source_employees.csv"
    target_file_path = "CSV_files/target_employees.csv"
    mandatory_column = "CSV_files/Not_null_columns.csv"
    
    #log_files
    count_log_path = "log_files/count_log.csv"
    accuracy_log_path = "log_files/accuracy_log.csv"
    duplicate_log_path = "log_files/duplicate_log.csv"
    not_null_log_path = "log_files/not_null_log.csv"
    table_difference_log_path ="log_files/table_difference_log.csv"

    # Count Validation
    status, message = count_check(source_file_path, target_file_path, count_log_path)
    log_message(status, message)

    # Accuracy Validation
    status, message = accuracy_check(source_file_path, target_file_path, accuracy_log_path)
    log_message(status, message)

    # Duplicate Check
    status, message = duplicate_check(target_file_path, duplicate_log_path)
    log_message(status, message)

    # Null Check
    status, message = Null_check(target_file_path,mandatory_column,not_null_log_path)
    log_message(status, message)

    # Table difference check source minus target and Vice Versa
    status, message = difference_check(source_file_path,target_file_path,table_difference_log_path)
    log_message(status, message)