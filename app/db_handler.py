import os
import duckdb
from typing import Optional, Tuple, Union, Any, List
import pandas as pd


class DBHandler:
    """A handler for DuckDB database operations including connection management and query execution.

    This class provides a convenient interface for interacting with DuckDB databases,
    handling table initialization, and executing queries with optional parameterization.

    Attributes:
        con (duckdb.DuckDBPyConnection): The active DuckDB database connection.
    """

    def __init__(self) -> None:
        """Initialize the DBHandler and establish a database connection.

        The database path is determined by the DB_PATH environment variable with a default
        of 'data/index_data.duckdb'. The parent directory will be created if it doesn't exist.

        Raises:
            RuntimeError: If database connection cannot be established.
        """
        db_path = os.getenv("DB_PATH", "data/index_data.duckdb")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.con = duckdb.connect(db_path)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get the active database connection.

        Returns:
            duckdb.DuckDBPyConnection: The active DuckDB connection object.
        """
        return self.con

    def fetchall(self, query: str, params: Tuple[Any, ...] = ()) -> List:
        """Execute a parameterized query and return all results as a list of tuples.

        Args:
            query: SQL query string, which may contain parameter placeholders.
            params: Tuple of parameters to substitute into the query (default: empty tuple).

        Returns:
            List of tuples containing the query results.
        """
        return self.con.execute(query, params).fetchall()

    def fetchdf(self, query: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
        """Execute a parameterized query and return results as a pandas DataFrame.

        Args:
            query: SQL query string, which may contain parameter placeholders.
            params: Tuple of parameters to substitute into the query (default: empty tuple).

        Returns:
            pandas.DataFrame: A DataFrame containing the query results.

        """
        return self.con.execute(query, params).fetchdf()

    def execute(self, query: str, params: Tuple[Any, ...] = ()):
        """Execute a SQL command without returning results (for INSERT/UPDATE/etc).

        Args:
            query: SQL command string, which may contain parameter placeholders.
            params: Tuple of parameters to substitute into the command (default: empty tuple).

        """
        return self.con.execute(query, params)

    def initialize_tables(self) -> None:
        """Initialize the required database tables if they don't already exist.

        Creates two tables:
        1. index_composition - tracks components of financial indices
        2. index_performance - tracks performance metrics of indices

        Tables are created with IF NOT EXISTS clauses to prevent errors on re-creation.
        """
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS index_composition (
                date DATE,
                ticker TEXT,
                weight DOUBLE
            )
            """
        )
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS index_performance (
                date DATE,
                index_value DOUBLE,
                daily_return DOUBLE
            )
            """
        )


# Module-level database handler instance
db_handler = DBHandler()
