import yfinance as yf
import pandas as pd
from db import get_db_connection, create_daily_stock_data_table, insert_stock_data


def fetch_and_store_stock_data(ticker, start_date, end_date):
    """Fetch historical stock data from Yahoo Finance and store in database.

    Args:
        ticker: Stock symbol to fetch (e.g., 'AAPL')
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        None: Data is stored directly in database. Returns None if:
            - No data available for the ticker
            - Market cap information is missing
    """
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
    hist["market_cap"] = market_cap
    hist["ticker"] = ticker

    # Save to DuckDB
    con = get_db_connection()
    create_daily_stock_data_table(con)
    insert_stock_data(con, hist)

    print(f"✅ Data for {ticker} saved to DuckDB.")


def fetch_active_stocks(start_date, end_date):
    """Fetch and store historical data for all active stocks in database.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        None: Data is stored directly in database for all active tickers
    """
    # Load tickers from DuckDB
    con = get_db_connection()
    tickers_df = con.execute(
        "SELECT ticker FROM stock_metadata WHERE active = TRUE"
    ).fetchdf()
    tickers = tickers_df["ticker"].tolist()

    for ticker in tickers:
        fetch_and_store_stock_data(ticker, start_date, end_date)

    print("🎉 Data acquisition complete.")


if __name__ == "__main__":
    start_date = "2025-01-01"
    end_date = "2025-04-23"
    fetch_active_stocks(start_date, end_date)
