from datetime import date
import pandas as pd
from typing import List, Dict

from app.db_handler import db_handler
from app.logger import Logger
from app.exceptions import DataNotFoundError


def get_top_100_stocks_on_date(d: date) -> pd.DataFrame:
    query = """
    SELECT ticker, close, market_cap
    FROM daily_stock_data
    WHERE date = ?
    ORDER BY market_cap DESC
    LIMIT 100
    """
    df = db_handler.fetchdf(query, (d,))
    return df


def fetch_index_performance(start_date: date, end_date: date) -> List[Dict]:
    """
    Fetch index performance data from the database if not cached.

    Args:
        start_date (date): The start of the date range.
        end_date (date): The end of the date range.

    Returns:
        List[Dict]: List of records with date, index_value, and daily_return.
    """
    query = """
    SELECT date, index_value, daily_return
    FROM index_performance
    WHERE date BETWEEN ? AND ?
    ORDER BY date
    """
    df = db_handler.fetchdf(query, (start_date, end_date))
    if df.empty:
        raise DataNotFoundError(
            f"No index performance data found in DB for {start_date} to {end_date}"
        )

    df = df.replace([float("inf"), float("-inf")], None).fillna(0)
    return df.to_dict(orient="records")


def fetch_index_composition(query_date: date) -> List[Dict]:
    """
    Fetches index composition data for the given date from DuckDB.

    Args:
        query_date (date): The date for which composition data is requested.

    Returns:
        List[Dict]: A list of dictionaries containing ticker and weight.

    Raises:
        DataNotFoundError: If no data is found in the database for the given date.
    """
    query = "SELECT ticker, weight FROM index_composition WHERE date = ?"
    rows = db_handler.fetchall(query, (query_date,))

    if not rows:
        raise DataNotFoundError(f"No index composition data found for {query_date}")

    return [{"ticker": r[0], "weight": r[1]} for r in rows]


def fetch_composition_changes(start_date: date, end_date: date) -> List[Dict]:
    """
    Computes composition changes between two dates.

    Args:
        start_date (date): Start of date range.
        end_date (date): End of date range.

    Returns:
        List[dict]: Each dict contains the date, entered tickers, and exited tickers.

    Raises:
        DataNotFoundError: If no composition data is found for the given range.
    """
    query = """
    SELECT date, ticker FROM index_composition
    WHERE date BETWEEN ? AND ?
    ORDER BY date
    """
    rows = db_handler.fetchall(query, (start_date, end_date))

    if not rows:
        raise DataNotFoundError(
            f"No index composition data found from {start_date} to {end_date}."
        )

    df = pd.DataFrame(rows, columns=["date", "ticker"])
    df["date"] = pd.to_datetime(df["date"])

    # Group by date into {date: set(tickers)}
    grouped = df.groupby("date")["ticker"].apply(set).sort_index()
    changes = []

    previous_date = None
    previous_tickers = set()

    for current_date, current_tickers in grouped.items():
        if previous_date is not None:
            entered = list(current_tickers - previous_tickers)
            exited = list(previous_tickers - current_tickers)
            if entered or exited:
                changes.append(
                    {
                        "date": current_date.date(),
                        "entered": sorted(entered),
                        "exited": sorted(exited),
                    }
                )
        previous_date = current_date
        previous_tickers = current_tickers

    return changes
