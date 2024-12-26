import oracledb

class DatabaseConnection:
    def __init__(self, username, password, dsn):
        """
        Initialize the database connection parameters.
        """
        self.username = username
        self.password = password
        self.dsn = dsn
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establish a connection to the database.
        """
        try:
            self.connection = oracledb.connect(user=self.username, password=self.password, dsn=self.dsn)
            self.cursor = self.connection.cursor()
            print("Connected to the database successfully.")
        except oracledb.DatabaseError as e:
            print(f"Error connecting to the database: {e}")
            raise

    def execute_query(self, query, params=None):
        """
        Execute a SQL query and return the results.
        """
        if not self.cursor:
            raise Exception("Database connection is not established.")
        try:
            self.cursor.execute(query, params or {})
            return self.cursor.fetchall()
        except oracledb.DatabaseError as e:
            print(f"Error executing query: {e}")
            raise

    def close(self):
        """
        Close the database connection and cursor.
        """
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print("Database connection closed.")
        except oracledb.DatabaseError as e:
            print(f"Error closing the database connection: {e}")
            raise
