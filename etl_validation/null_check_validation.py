from connection.database_connection import DatabaseConnection
import pandas as pd

def Null_check(target_table, mandatory_columns_file, db_config, log_file):
    """
    Checks for null records in the mandatory columns in the given database table.

    Parameters:
        target_table (str): Fully qualified name of the target table (e.g., schema.table).
        mandatory_columns_file (str): Path to the CSV file containing mandatory column names.
        db_config (dict): Database connection configuration.
        log_file (str): Path to the log file to save null record details.

    Returns:
        tuple: A tuple containing a boolean (pass/fail status) and a message.
    """
    try:
        # Load the mandatory columns from the CSV file
        mandatory_columns_df = pd.read_csv(mandatory_columns_file, header=None)
        mandatory_columns_list = mandatory_columns_df[0].tolist()  # Extract column names as a list

        # Initialize the database connection
        db = DatabaseConnection(**db_config)
        db.connect()

        # Validate the mandatory columns exist in the target table
        target_columns_query = f"""
        SELECT column_name
        FROM all_tab_columns
        WHERE table_name = '{target_table.split('.')[-1].upper()}'
        AND owner = '{target_table.split('.')[0].upper()}'
        """
        target_columns_result = db.execute_query(target_columns_query)
        target_columns = [row[0] for row in target_columns_result]

        # Check if all mandatory columns exist in the target table
        for column in mandatory_columns_list:
            if column.upper() not in target_columns:
                raise Exception(f"Mandatory column '{column}' not found in the target table.")

        # Generate SQL query to find null records in mandatory columns
        null_conditions = [f"{col} IS NULL" for col in mandatory_columns_list]
        null_query = f"""
        SELECT *
        FROM {target_table}
        WHERE {" OR ".join(null_conditions)}
        """
        
        # Execute the SQL query to find null records
        null_records = db.execute_query(null_query)

        # Close the database connection
        db.close()

        # Check if any null records were found
        if not null_records:  # If the query returns no rows
            return True, "No null values found in mandatory columns."
        else:
            # Save null records to the log file
            null_records_df = pd.DataFrame(null_records, columns=target_columns)
            null_records_df.to_csv(log_file, index=False)
            return False, f"Null values found in mandatory columns. Details written to {log_file}."
    except Exception as e:
        return False, f"Error during Null_check: {e}"
