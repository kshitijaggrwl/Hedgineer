from datetime import date
import pandas as pd
from app.db_handler import db_handler

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

def compute_index_performance(start: date, end: date) -> pd.DataFrame:
    query = """
    SELECT date, ticker, close, market_cap
    FROM daily_stock_data
    WHERE date BETWEEN ? AND ?
    """
    df = db_handler.fetchdf(query, (start, end))
    if df.empty:
        return df

    grouped = df.groupby(['date'])
    results = []

    for day, group in grouped:
        group = group.sort_values("market_cap", ascending=False).head(100)
        group['weight'] = 1 / 100
        group['notional'] = group['weight'] * group['close']
        index_value = group['notional'].sum()
        results.append({'date': day, 'index_value': index_value})

    result_df = pd.DataFrame(results).sort_values("date")
    result_df['daily_return'] = result_df['index_value'].pct_change()
    result_df['cumulative_return'] = (1 + result_df['daily_return'].fillna(0)).cumprod() - 1
    return result_df
