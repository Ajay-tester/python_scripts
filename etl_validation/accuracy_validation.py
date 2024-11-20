import pandas as pd
from pandasql import sqldf
from .column_utils import get_column_names

def accuracy_check(source_file, target_file, log_file):
    """
    Validates data accuracy between source and target datasets dynamically using SQL.
    """
    try:
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        source_columns = get_column_names(source_file)
        target_columns = get_column_names(target_file)

         # SQL SELECT clause for column comparison
        select_clauses = [
            f"source.{col} AS Source_{col}, target.{col} AS Target_{col}"
            for col in source_columns if col in target_columns
        ]

        query = f"""
        SELECT source.EMPLOYEE_ID,
               {', '.join(select_clauses)}
        FROM source_df AS source
        LEFT JOIN target_df AS target 
        ON source.EMPLOYEE_ID = target.EMPLOYEE_ID
        WHERE {" OR ".join([f"(source.{col} != target.{col} OR target.{col} IS NULL)" for col in source_columns if col in target_columns])}
        AND source.EMPLOYEE_ID IS NOT NULL;
        """

        mismatched_rows = sqldf(query, locals())
        if mismatched_rows.empty:
            return True, "Data accuracy check passed! No mismatches found."
        else:
            mismatched_rows.to_csv(log_file, index=False)
            return False, f"Data accuracy issues found. Details written to {log_file}."
    except Exception as e:
        return False, f"Error during accuracy_check: {e}"
