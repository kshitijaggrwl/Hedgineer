import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/index_data.duckdb")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


# Create DuckDB connection
def get_db_connection():
    """Create and return a DuckDB connection to the database specified by DB_PATH.

    Returns:
        duckdb.DuckDBPyConnection: An active connection to the DuckDB database.
    """
    return duckdb.connect(DB_PATH)


# Create table if not exists
def create_stock_metadata_table(con):
    """Create the stock_metadata table if it doesn't exist.

    Args:
        con (duckdb.DuckDBPyConnection): Active database connection.
    """
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS stock_metadata (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        market TEXT,
        locale TEXT,
        currency TEXT,
        cik TEXT,
        active BOOLEAN
    );
    """
    )


def create_daily_stock_data_table(con):
    """Create the daily_stock_data table if it doesn't exist.

    Args:
        con (duckdb.DuckDBPyConnection): Active database connection.
    """
    con.execute(
        """
    CREATE TABLE IF NOT EXISTS daily_stock_data (
        ticker TEXT,
        date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        market_cap DOUBLE,
        PRIMARY KEY (ticker, date)
    );
    """
    )


# Insert tickers into the stock_metadata table
def insert_tickers_into_db(con, tickers):
    """Insert or replace ticker metadata into the stock_metadata table.

    Args:
        con (duckdb.DuckDBPyConnection): Active database connection.
        tickers (list[dict]): List of dictionaries containing ticker metadata.
    """
    for t in tickers:
        con.execute(
            """
        INSERT OR REPLACE INTO stock_metadata (ticker, name, market, locale, currency, cik, active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                t.get("ticker"),
                t.get("name"),
                t.get("market"),
                t.get("locale"),
                t.get("currency_name"),
                t.get("cik"),
                t.get("active"),
            ),
        )


# Insert daily stock data
def insert_stock_data(con, stock_data):
    """Insert or replace daily stock data into the daily_stock_data table.

    Args:
        con (duckdb.DuckDBPyConnection): Active database connection.
        stock_data (pd.DataFrame): DataFrame containing daily stock data with columns:
            ['ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'market_cap'].
            The index should be a datetime.
    """
    for _, row in stock_data.iterrows():
        con.execute(
            """
        INSERT OR REPLACE INTO daily_stock_data (date, ticker, open, high, low, close, volume, market_cap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                row.name.date(),
                row["ticker"],
                row["Open"],
                row["High"],
                row["Low"],
                row["Close"],
                row["Volume"],
                row["market_cap"],
            ),
        )
