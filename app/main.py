from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from datetime import date
import pandas as pd
from typing import Optional
from app.utils import get_top_100_stocks_on_date, compute_index_performance
from app.db_handler import db_handler
from app.cache import lifespan
import json

app = FastAPI(title="Custom Equal-Weighted Index API", lifespan=lifespan)


@app.post("/build-index")
async def build_index(start_date: date = Query(...), end_date: Optional[date] = Query(None)):
    end_date = end_date or start_date
    date_range = pd.date_range(start=start_date, end=end_date)
    compositions = []
    performances = []

    previous_index_value = None

    for day in date_range:
        day_str = day.date()
        top_stocks = get_top_100_stocks_on_date(day_str)
        if top_stocks.empty:
            continue

        top_stocks['weight'] = 1 / 100
        top_stocks['date'] = day_str
        top_stocks['notional'] = top_stocks['weight'] * top_stocks['close']
        index_value = top_stocks['notional'].sum()

        # Calculate daily_return only
        if previous_index_value is None:
            daily_return = 0.0
        else:
            daily_return = ((index_value - previous_index_value) / previous_index_value) * 100 if previous_index_value != 0 else 0.0
        previous_index_value = index_value

        performances.append({
            'date': day_str,
            'index_value': index_value,
            'daily_return': daily_return
        })

        # Cache each day's performance in Redis
        redis_backend = FastAPICache.get_backend()
        perf_key = f"build-index:performance:{day_str}"
        await redis_backend.redis.set(
            perf_key, 
            json.dumps({
                'index_value': index_value, 
                'daily_return': daily_return
            }), 
            ex=86400
        )

        compositions.append(top_stocks[['date', 'ticker', 'weight']])

    if not compositions:
        return JSONResponse(status_code=404, content={"message": "No index data could be built."})

    composition_df = pd.concat(compositions)
    db_handler.execute("CREATE TABLE IF NOT EXISTS index_composition (date DATE, ticker TEXT, weight DOUBLE)")
    db_handler.execute("DELETE FROM index_composition WHERE date BETWEEN ? AND ?", (start_date, end_date))
    db_handler.con.register("tmp_comp", composition_df)
    db_handler.execute("INSERT INTO index_composition SELECT * FROM tmp_comp")

    performance_df = pd.DataFrame(performances).sort_values("date")
    db_handler.execute("""
        CREATE TABLE IF NOT EXISTS index_performance (
            date DATE,
            index_value DOUBLE,
            daily_return DOUBLE
        )
    """)
    db_handler.execute("DELETE FROM index_performance WHERE date BETWEEN ? AND ?", (start_date, end_date))
    db_handler.con.register("tmp_perf", performance_df)
    db_handler.execute("INSERT INTO index_performance SELECT * FROM tmp_perf")

    return {
        "message": "Index built and performance calculated.",
        "days_processed": len(performance_df)
    }

@app.get("/index-performance")
@cache(expire=60)
async def index_performance(start_date: date = Query(...), end_date: date = Query(...)):
    redis_backend = FastAPICache.get_backend()
    date_range = pd.date_range(start=start_date, end=end_date)
    results = []
    all_cached = True

    for d in date_range:
        key = f"build-index:performance:{d.date()}"
        cached = await redis_backend.redis.get(key)

        if cached:
            data = json.loads(cached)
            results.append({
                "date": str(d.date()),
                "index_value": data.get("index_value"),
                "daily_return": data.get("daily_return", 0.0)
            })
        else:
            all_cached = False
            break

    if not all_cached:
        query = """
        SELECT date, index_value, daily_return
        FROM index_performance
        WHERE date BETWEEN ? AND ?
        ORDER BY date
        """
        df = db_handler.fetchdf(query, (start_date, end_date))
        if df.empty:
            return JSONResponse(status_code=404, content={"message": "No index performance data available."})
        df = df.replace([float('inf'), float('-inf')], None).fillna(0)
        results = df.to_dict(orient="records")

    df_results = pd.DataFrame(results).sort_values("date")
    df_results['cumulative_return'] = df_results['daily_return'].cumsum()
    return df_results.to_dict(orient="records")

@app.get("/index-composition")
@cache(expire=60)
async def index_composition(date: date = Query(...)):
    redis = FastAPICache.get_backend().redis
    key = f"build-index:composition:{date}"

    cached = await redis.get(key)
    if cached:
        tickers = json.loads(cached)
        return [{"ticker": t["ticker"], "weight": t["weight"]} for t in tickers]

    # Fallback to DuckDB
    query = "SELECT ticker, weight FROM index_composition WHERE date = ?"
    rows = db_handler.fetchall(query, (date,))
    if not rows:
        return JSONResponse(status_code=404, content={"message": "No composition data found for this date."})

    return [{"ticker": r[0], "weight": r[1]} for r in rows]

@app.get("/composition-changes")
@cache(expire=60)
async def composition_changes(start_date: date = Query(...), end_date: date = Query(...)):
    query = """
    SELECT date, ticker FROM index_composition
    WHERE date BETWEEN ? AND ?
    ORDER BY date
    """
    rows = db_handler.fetchall(query, (start_date, end_date))

    if not rows:
        return JSONResponse(
            status_code=404,
            content={"message": "No composition data found for this date range."}
        )

    df = pd.DataFrame(rows, columns=["date", "ticker"])
    df['date'] = pd.to_datetime(df['date'])

    # Group by date into {date: set(tickers)}
    grouped = df.groupby('date')['ticker'].apply(set).sort_index()
    changes = []

    previous_date = None
    previous_tickers = set()

    for current_date, current_tickers in grouped.items():
        if previous_date is not None:
            entered = list(current_tickers - previous_tickers)
            exited = list(previous_tickers - current_tickers)
            if entered or exited:
                changes.append({
                    "date": current_date.date(),
                    "entered": sorted(entered),
                    "exited": sorted(exited)
                })
        previous_date = current_date
        previous_tickers = current_tickers

    if not changes:
        return JSONResponse(
            status_code=200,
            content={"message": "No composition changes detected."}
        )

    return {"changes": changes}

@app.post("/export-data")
async def export_data(start_date: date = Query(...), end_date: date = Query(...)):
    from openpyxl import Workbook
    import io
    import tempfile
    def safe_excel_date(d):
        if isinstance(d, pd.Timestamp):
            return d.date()
        elif isinstance(d, tuple) and isinstance(d[0], pd.Timestamp):
            return d[0].date()
        return d

    # Fetch index performance directly from DB
    performance_query = """
    SELECT date, index_value, daily_return FROM index_performance
    WHERE date BETWEEN ? AND ? ORDER BY date
    """
    df_performance = db_handler.fetchdf(performance_query, (start_date, end_date))

    if df_performance.empty:
        return JSONResponse(status_code=404, content={"message": "No performance data found."})

    # Calculate cumulative return only for the response
    df_performance['cumulative_return'] = df_performance['daily_return'].cumsum()

    wb = Workbook()
    ws = wb.active
    ws.title = "Index Performance"
    ws.append(["Date", "Index Value", "Daily Return", "Cumulative Return"])

    for row in df_performance.itertuples():
        ws.append([
            safe_excel_date(row.date),
            row.index_value,
            row.daily_return,
            row.cumulative_return
        ])

    # Index composition
    composition_query = "SELECT date, ticker, weight FROM index_composition WHERE date BETWEEN ? AND ?"
    composition_df = db_handler.fetchdf(composition_query, (start_date, end_date))

    if not composition_df.empty:
        ws_composition = wb.create_sheet(title="Index Composition")
        ws_composition.append(["Date", "Ticker", "Weight"])
        for row in composition_df.itertuples():
            ws_composition.append([
                safe_excel_date(row.date),
                row.ticker,
                row.weight
            ])

    # Composition changes (based on day-over-day comparison)
    change_query = "SELECT date, ticker FROM index_composition WHERE date BETWEEN ? AND ? ORDER BY date"
    changes_df = db_handler.fetchdf(change_query, (start_date, end_date))
    if not changes_df.empty:
        changes_df['date'] = pd.to_datetime(changes_df['date'])
        grouped = changes_df.groupby('date')['ticker'].apply(set).sort_index()

        previous_tickers = set()
        changes = []

        for current_date, current_tickers in grouped.items():
            entered = sorted(current_tickers - previous_tickers)
            exited = sorted(previous_tickers - current_tickers)
            if entered or exited:
                changes.append({
                    "date": current_date.date(),
                    "entered": entered,
                    "exited": exited
                })
            previous_tickers = current_tickers

        if changes:
            ws_changes = wb.create_sheet(title="Composition Changes")
            ws_changes.append(["Date", "Entered Tickers", "Exited Tickers"])
            for change in changes:
                ws_changes.append([
                    change['date'],
                    ', '.join(change['entered']),
                    ', '.join(change['exited'])
                ])

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        wb.save(tmp.name)
        tmp_path = tmp.name

    return FileResponse(
        tmp_path,
        filename="index_data.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )