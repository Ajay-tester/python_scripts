def get_column_names(csv_file):
    """
    Extracts column names from a CSV file.
    """
    try:
        with open(csv_file, "r") as file:
            columns = file.readline().strip().split(",")
        return columns
    except Exception as e:
        raise Exception(f"Error reading columns from {csv_file}: {e}")
