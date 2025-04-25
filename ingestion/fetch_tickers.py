import httpx
import time
import urllib.parse
from db import get_db_connection, create_stock_metadata_table, insert_tickers_into_db

BASE_URL = "https://api.polygon.io/v3/reference/tickers"
API_KEY = "e2cVzuJd_0zCqIDoCrIoklYYq6oxG8f5"  # Replace with your API key


def fetch_tickers():
    """Fetch all active US common stock tickers from Polygon API and store in database.

    This function:
    1. Makes paginated requests to Polygon's /v3/reference/tickers endpoint
    2. Handles rate limiting with 15-second delays between requests
    3. Filters for active US common stocks (type='CS')
    4. Stores results in the stock_metadata table

    Notes:
        - Requires POLYGON_API_KEY environment variable
        - Implements automatic pagination using cursor tokens
        - Skips failed requests and continues with available data

    Returns:
        None: Data is stored directly in database. Prints status updates.
    """
    tickers = []
    cursor = None
    params = {
        "market": "stocks",
        "active": "true",
        "type": "CS",  # Common Stock
        "limit": 1000,
        "apiKey": API_KEY,
    }

    print("📡 Fetching all US stock tickers from Polygon...")

    while True:
        query_params = params.copy()
        if cursor:
            query_params["cursor"] = cursor

        response = httpx.get(BASE_URL, params=query_params)

        if response.status_code != 200:
            print(f"❌ Error: {response.status_code} - {response.text}")
            break

        data = response.json()
        results = data.get("results", [])
        tickers.extend(results)

        cursor = data.get("next_url")
        if cursor:
            cursor = urllib.parse.parse_qs(urllib.parse.urlparse(cursor).query).get(
                "cursor", [None]
            )[0]

        if not cursor:
            break

        time.sleep(15)

    # Save to DuckDB
    con = get_db_connection()
    create_stock_metadata_table(con)
    print(f"✅ Got {len(tickers)} tickers. Saving to DuckDB...")
    insert_tickers_into_db(con, tickers)
    print("🎉 All symbols saved to DuckDB.")


if __name__ == "__main__":
    fetch_tickers()
