from connection.database_connection import DatabaseConnection
from .column_utils import get_column_names

def duplicate_check(target_table, db_config, log_file):
    """
    Checks for duplicate records in the target table using SQL.
    """
    try:
        # Establish database connection
        db = DatabaseConnection(**db_config)
        db.connect()

        # Fetch column names from the target table
        target_columns = get_column_names(target_table, db)

        # Build the SQL query to find duplicates
        query = f"""
        SELECT {', '.join(target_columns)}, COUNT(*) AS count
        FROM {target_table}
        GROUP BY {', '.join(target_columns)}
        HAVING COUNT(*) > 1
        """

        # Execute the query
        duplicates = db.execute_query(query)

        # Close the database connection
        db.close()

        # Process query results
        if not duplicates:  # If the query returns no rows
            return True, "No duplicates found."
        else:
            # Save duplicates to a log file
            import pandas as pd
            duplicates_df = pd.DataFrame(duplicates, columns=target_columns + ["count"])
            duplicates_df.to_csv(log_file, index=False)
            return False, f"Duplicates found. Details written to {log_file}."
    except Exception as e:
        return False, f"Error during duplicate_check: {e}"
