import pandas as pd
from pandasql import sqldf
from .column_utils import get_column_names

def Null_check(target_file, mandatory_columns_file, log_file):
    """
    Checks for null records in the mandatory columns in the given file using SQL.

    Parameters:
        target_file (str): Path to the target file to check.
        mandatory_columns_file (str): Path to the CSV file containing mandatory column names.
        log_file (str): Path to the log file to save null record details.

    Returns:
        tuple: A tuple containing a boolean (pass/fail status) and a message.
    """
    try:
        # Load the target file into a DataFrame
        df = pd.read_csv(target_file)

        # Extract mandatory column names from the provided file
        mandatory_columns = get_column_names(mandatory_columns_file)

        # Validate the mandatory columns exist in the DataFrame
        available_columns = get_column_names(target_file)
        for column in mandatory_columns:
            if column not in available_columns:
                raise Exception(f"Mandatory column '{column}' not found in the target file.")

        # Generate SQL query to find null records in mandatory columns
        null_conditions = [f"{col} IS NULL" for col in mandatory_columns]
        query = f"""
        SELECT *
        FROM df
        WHERE {" OR ".join(null_conditions)};
        """

        # Execute the SQL query to find null records
        null_records = sqldf(query, locals())

        # Check if any null records were found
        if null_records.empty:
            return True, "No null values found in mandatory columns."
        else:
            # Save the null records to the log file
            null_records.to_csv(log_file, index=False)
            return False, f"Null values found in mandatory columns. Details written to {log_file}."
    except Exception as e:
        return False, f"Error during Null_check: {e}"
