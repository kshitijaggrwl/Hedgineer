import os
import duckdb
from functools import lru_cache

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

# Singleton instance
db_handler = DBHandler()
