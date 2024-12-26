from connection.database_connection import DatabaseConnection
import pandas as pd

def count_check(source_table, target_table, db_config, log_file):
    """
    Validates if row counts between source and target tables in the database match.
    """
    try:
        # Initialize the database connection
        db = DatabaseConnection(**db_config)
        db.connect()

        # Ensure the table names include schema (fully qualified names)
        source_table = source_table.strip()
        target_table = target_table.strip()

        # Log the source and target table names to check for issues
        print(f"Checking row count for source table: {source_table}")
        print(f"Checking row count for target table: {target_table}")

        # Query to count rows in the source table
        source_query = f"SELECT COUNT(*) AS row_count FROM {source_table}"
        source_count = db.execute_query(source_query)[0][0]

        # Query to count rows in the target table
        target_query = f"SELECT COUNT(*) AS row_count FROM {target_table}"
        target_count = db.execute_query(target_query)[0][0]

        # Close the database connection
        db.close()

        # Compare the counts
        if source_count == target_count:
            return True, "Row counts match! No mismatch found."
        else:
            # Log the mismatch details
            log_data = {
                "Source Count": [source_count],
                "Target Count": [target_count],
                "Difference": [abs(source_count - target_count)],
            }

            # Write mismatch details to the log file
            log_df = pd.DataFrame(log_data)
            log_df.to_csv(log_file, index=False)

            return False, f"Row count mismatch. Details written to {log_file}."
    except Exception as e:
        # Log more specific details about the exception
        if "ORA-00942" in str(e):
            error_message = f"Error: Table or view does not exist. Please check table names and permissions. ({e})"
        else:
            error_message = f"Error during count_check: {e}"

        return False, error_message
