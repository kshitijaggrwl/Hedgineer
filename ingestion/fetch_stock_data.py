import yfinance as yf
import pandas as pd
from db import get_db_connection, create_daily_stock_data_table, insert_stock_data

def fetch_and_store_stock_data(ticker, start_date, end_date):
    print(f"📡 Fetching data for {ticker}...")

    # Fetch stock data from Yahoo Finance
    stock = yf.Ticker(ticker)
    hist = stock.history(start=start_date, end=end_date)

    if hist.empty:
        return None

    # Get market cap from stock info
    info = stock.info
    market_cap = info.get("marketCap", None)

    if market_cap is None:
        print(f"Skipping {ticker}: Market Cap not available.")
        return None

    # Add market cap to the historical data
    hist['market_cap'] = market_cap
    hist['ticker'] = ticker

    # Save to DuckDB
    con = get_db_connection()
    create_daily_stock_data_table(con)
    insert_stock_data(con, hist)

    print(f"✅ Data for {ticker} saved to DuckDB.")

def fetch_top_100_stocks(start_date, end_date):
    # Load tickers from DuckDB
    con = get_db_connection()
    tickers_df = con.execute("SELECT ticker FROM stock_metadata WHERE active = TRUE").fetchdf()
    tickers = tickers_df['ticker'].tolist()

    for ticker in tickers:
        fetch_and_store_stock_data(ticker, start_date, end_date)

    print("🎉 Data acquisition complete.")

if __name__ == "__main__":
    start_date = "2025s-03-15"
    end_date = "2025-04-20"
    fetch_top_100_stocks(start_date, end_date)
