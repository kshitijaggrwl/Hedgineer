import httpx
import pandas as pd
import duckdb
import os

API_KEY = "e2cVzuJd_0zCqIDoCrIoklYYq6oxG8f5"
DB_PATH = "data/index_data.duckdb"
con = duckdb.connect(DB_PATH)

def fetch_bulk_snapshot():
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    params = {"apiKey": API_KEY}
    r = httpx.get(url, params=params)

    if r.status_code != 200:
        raise Exception(f"Failed to fetch snapshot: {r.status_code} - {r.text}")

    data = r.json()
    results = data.get("tickers", [])
    records = []

    for item in results:
        ticker = item.get("ticker")
        close_price = item.get("lastTrade", {}).get("p")
        market_cap = item.get("marketCap")
        if ticker and close_price and market_cap:
            records.append((ticker, close_price, market_cap))

    df = pd.DataFrame(records, columns=["ticker", "close_price", "market_cap"])
    df["date"] = pd.to_datetime("today").date()

    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_stock_data (
            ticker TEXT,
            date DATE,
            close_price DOUBLE,
            market_cap DOUBLE,
            PRIMARY KEY (ticker, date)
        )
    """)
    con.execute("INSERT OR REPLACE INTO daily_stock_data SELECT * FROM df")

    top_100 = df.sort_values("market_cap", ascending=False).head(100)
    print("🏆 Top 5 by Market Cap:")
    print(top_100.head(5))
    return top_100

if __name__ == "__main__":
    fetch_bulk_snapshot()
