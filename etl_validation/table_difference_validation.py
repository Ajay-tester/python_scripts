import pandas as pd
from pandasql import sqldf

def difference_check(source_file, target_file, log_file):
    """
    Identifies rows in source but not in target, and vice versa,
    and logs the results into a single file with a difference type column using pandasql.
    """
    try:
        # Load source and target files into DataFrames
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        # Find common columns between source and target
        common_columns = list(set(source_df.columns).intersection(set(target_df.columns)))
        if not common_columns:
            raise ValueError("No common columns found between source and target files.")

        # Format columns for SQL
        source_columns = ", ".join([f"s.{col}" for col in common_columns])
        target_columns = ", ".join([f"t.{col}" for col in common_columns])
        column_list = ", ".join(common_columns)

        # SQL query for rows in source but not in target
        query_source_minus_target = f"""
        SELECT {source_columns} , 'Source Minus Target' AS Difference_Type
        FROM source_df s
        LEFT JOIN target_df t
        ON {" AND ".join([f"s.{col} = t.{col}" for col in common_columns])}
        WHERE {" AND ".join([f"t.{col} IS NULL" for col in common_columns])};
        """

        # SQL query for rows in target but not in source
        query_target_minus_source = f"""
        SELECT {target_columns} , 'Target Minus Source' AS Difference_Type
        FROM target_df t
        LEFT JOIN source_df s
        ON {" AND ".join([f"t.{col} = s.{col}" for col in common_columns])}
        WHERE {" AND ".join([f"s.{col} IS NULL" for col in common_columns])};
        """

        # Execute the queries using pandasql
        source_minus_target = sqldf(query_source_minus_target, locals())
        target_minus_source = sqldf(query_target_minus_source, locals())

        # Combine the results into a single DataFrame
        combined_diff = pd.concat([source_minus_target, target_minus_source], ignore_index=True)

        # Write to a single log file
        if not combined_diff.empty:
            combined_diff.to_csv(log_file, index=False)
            return False, f"Differences logged in {log_file}."
        else:
            return True, "No differences found between source and target."

    except Exception as e:
        return False, f"Error during table_difference_combined_log: {e}"
