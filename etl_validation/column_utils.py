def get_column_names(table_name, db):
    """
    Extracts column names from a database table.
    Args:
    - table_name: Fully qualified table name (schema.table)
    - db: DatabaseConnection object
    Returns:
    - List of column names
    """
    try:
        query = f"""
        SELECT column_name
        FROM all_tab_columns
        WHERE table_name = '{table_name.split('.')[-1].upper()}'
        AND owner = '{table_name.split('.')[0].upper()}'
        """
        result = db.execute_query(query)
        return [row[0] for row in result]
    except Exception as e:
        raise Exception(f"Error fetching columns from {table_name}: {e}")
