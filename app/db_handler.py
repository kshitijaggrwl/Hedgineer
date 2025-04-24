import os
import duckdb


class DBHandler:
    def __init__(self):
        db_path = os.getenv("DB_PATH", "data/index_data.duckdb")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.con = duckdb.connect(db_path)

    def get_connection(self):
        return self.con

    def fetchall(self, query: str, params: tuple = ()):
        return self.con.execute(query, params).fetchall()

    def fetchdf(self, query: str, params: tuple = ()):
        return self.con.execute(query, params).fetchdf()

    def execute(self, query: str, params: tuple = ()):
        return self.con.execute(query, params)

    def initialize_tables(self):
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


db_handler = DBHandler()
