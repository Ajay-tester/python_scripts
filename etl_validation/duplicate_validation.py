import pandas as pd
from pandasql import sqldf
from .column_utils import get_column_names

def duplicate_check(target_file, log_file):
    """
    Checks for duplicate rows in the target file using SQL.
    """
    try:
        target_df = pd.read_csv(target_file)
        target_columns = get_column_names(target_file)

        query = f"""
        SELECT {', '.join(target_columns)}, COUNT(*) AS count
        FROM target_df
        GROUP BY {', '.join(target_columns)}
        HAVING COUNT(*) > 1;
        """

        duplicates = sqldf(query, locals())
        if duplicates.empty:
            return True, "No duplicates found."
        else:
            duplicates.to_csv(log_file, index=False)
            return False, f"Duplicates found. Details written to {log_file}."
    except Exception as e:
        return False, f"Error during duplicate_check: {e}"
