import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/index_data.duckdb")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Create DuckDB connection
def get_db_connection():
    return duckdb.connect(DB_PATH)

# Create table if not exists
def create_stock_metadata_table(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS stock_metadata (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        market TEXT,
        locale TEXT,
        currency TEXT,
        cik TEXT,
        active BOOLEAN
    );
    """)

def create_daily_stock_data_table(con):
    con.execute("""
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
    """)

# Insert tickers into the stock_metadata table
def insert_tickers_into_db(con, tickers):
    for t in tickers:
        con.execute("""
        INSERT OR REPLACE INTO stock_metadata (ticker, name, market, locale, currency, cik, active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            t.get("ticker"),
            t.get("name"),
            t.get("market"),
            t.get("locale"),
            t.get("currency_name"),
            t.get("cik"),
            t.get("active")
        ))

# Insert daily stock data
def insert_stock_data(con, stock_data):
    for _, row in stock_data.iterrows():
        con.execute("""
        INSERT OR REPLACE INTO daily_stock_data (date, ticker, close, volume, market_cap)
        VALUES (?, ?, ?, ?, ?)
        """, (row.name.date(), row['ticker'], row['Close'], row['Volume'], row['market_cap']))
