# 📈 Index Performance API

A FastAPI application for financial index analysis with Redis caching and DuckDB storage.

## ✨ Features
- **Real-time index calculation**
- **Redis caching layer** for performance
- **DuckDB persistent storage**
- **Async endpoints** for efficient I/O
- **Swagger/OpenAPI documentation**
- **Docker-ready** configuration

## 🚀 Installation Guide

### Local Installation
```bash
# Clone repository
git clone https://github.com/kshitijaggrwl/Hedgineer.git
cd Hedgineer

# Setup environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run application
uvicorn app.main:app --reload
```

### Docker Setup
```bash
docker-compose up -d --build
```
- Access: http://localhost:8000/docs


## Data Acquisition
```bash
python ingestion/fetch_tickers.py
python ingestion/fetch_stock_data.py
```

## Database Schema

### Tables
- `daily_stock_data`

| Column     | Type    | Nullable | Description             |
|------------|---------|----------|-------------------------|
| ticker     | VARCHAR | NO       | Stock ticker symbol     |
| date       | DATE    | NO       | Trading date           |
| open       | DOUBLE  | YES      | Opening price          |
| high       | DOUBLE  | YES      | Daily high price       |
| low        | DOUBLE  | YES      | Daily low price        |
| close      | DOUBLE  | YES      | Closing price          |
| volume     | BIGINT  | YES      | Trading volume         |
| market_cap | DOUBLE  | YES      | Market capitalization  |

- `stock_metadata` 

| Column   | Type    | Nullable | Description           |
|----------|---------|----------|-----------------------|
| ticker   | VARCHAR | NO       | Stock ticker symbol   |
| name     | VARCHAR | YES      | Company name          |
| market   | VARCHAR | YES      | Market identifier     |
| locale   | VARCHAR | YES      | Market locale         |
| currency | VARCHAR | YES      | Trading currency      |
| cik      | VARCHAR | YES      | Central Index Key     |
| active   | BOOLEAN | YES      | Active status flag    |
- `index_performance`

| Column       | Type    | Nullable | Description             |
|--------------|---------|----------|-------------------------|
| date         | DATE    | YES      | Calculation date        |
| index_value  | DOUBLE  | YES      | Index value             |
| daily_return | DOUBLE  | YES      | Daily return percentage |
- `index_composition`

| Column  | Type    | Nullable | Description      |
|---------|---------|----------|------------------|
| date    | DATE    | YES      | Calculation date |
| ticker  | VARCHAR | YES      | Component ticker |
| weight  | DOUBLE  | YES      | Index weight     |


## Production and Scaling Improvements

- **Use Gunicorn with Uvicorn Workers:**
 For better performance and handling multiple requests, run the application with Gunicorn and Uvicorn workers.
    ```bash
    gunicorn -w 4 -k uvicorn.workers.UvicornWorker app.main:app
    ```

- **Set Up Monitoring and Alerts**
 Setup alerts for failure cases,(application level, server level)

- **Container Orchestration:**
 Use Docker Swarm or Kubernetes for managing containers in production environments.

- **Scale App Tier and Data Tier Independently:**
For App Tier, we can use NGINX as forward proxy. This will balance load among multiple Application Servers
For DataBase Tier, we have more read requests, we can use single master and multiple slaves architecture.

- **Security**
Access API through a key. Add rate limiting on key level to prevent DDOS attacks

