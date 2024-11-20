import pandas as pd
from pandasql import sqldf

def count_check(source_file, target_file, log_file):
    """
    Validates if row counts between source and target files match using SQL.
    """
    try:
        # Load source and target files
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        # SQL queries to count rows
        source_query = "SELECT COUNT(*) AS row_count FROM source_df;"
        target_query = "SELECT COUNT(*) AS row_count FROM target_df;"

        # Execute queries
        source_count = sqldf(source_query, locals()).iloc[0, 0]
        target_count = sqldf(target_query, locals()).iloc[0, 0]

        # Compare row counts
        if source_count == target_count:
            return True, "Row counts match! No mismatch found."
        else:
            # Prepare log details
            log_data = {
                "Source Count": [source_count],
                "Target Count": [target_count],
                "Difference": [abs(source_count - target_count)],
            }
            log_df = pd.DataFrame(log_data)
            log_df.to_csv(log_file, index=False)
            return False, f"Row count mismatch. Details written to {log_file}."
    except Exception as e:
        return False, f"Error during count_check: {e}"
