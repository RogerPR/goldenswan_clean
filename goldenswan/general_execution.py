import pandas as pd
import numpy as np

import goldenswan.utils.database as db
from goldenswan.utils.alphavantage import (
    request_api_data,
    format_alphavantage_to_db_daily,
)


# Insert NASDAQ market -------------------------------------------------------------------------------------------------
market = pd.DataFrame([["NASDAQ", "USA", "NORTH_AMERICA", "GMT-4", -4]])
market.columns = "name country region timezone timediff".split()
db.bulk_insert_df("market", market)

# Insert NYSE market ---------------------------------------------------------------------------------------------------
market = pd.DataFrame([["NYSE", "USA", "NORTH_AMERICA", "GMT-4", -4]])
market.columns = "name country region timezone timediff".split()
db.bulk_insert_df("market", market)

db_market = db.query_db("SELECT * FROM Market")

# Insert AMAZON product ------------------------------------------------------------------------------------------------
product = pd.DataFrame([["AMZN", "AMAZON", "STOCKS", db_market[db_market["name"] == "NASDAQ"]["market_id"].values[0]]])
product.columns = "symbol name product_type market_id".split()
db.bulk_insert_df("product", product)

# Insert AMAZON daily
ticker = "AMZN"
daily = request_api_data(
    symbol=ticker, interval_type="TIME_SERIES_DAILY_ADJUSTED", outputsize="full"
)
daily = format_alphavantage_to_db_daily(daily, ticker)
db.bulk_insert_df("daily", daily)

# Minibenchmark
import time

a = time.time()
db.insert_df_row_by_row("daily", daily)
print(time.time() - a)

a = time.time()
db.bulk_insert_df("daily", daily)
print(time.time() - a)

# Insert ALIBABA product -----------------------------------------------------------------------------------------------
product = pd.DataFrame([["BABA", "ALIBABA", "STOCKS", db_market[db_market["name"] == "NASDAQ"]["market_id"].values[0]]])
product.columns = "symbol name product_type market_id".split()
db.bulk_insert_df("product", product)


# Insert ALIBABA daily
ticker = "BABA"
daily = request_api_data(
    symbol=ticker, interval_type="TIME_SERIES_DAILY_ADJUSTED", outputsize="full"
)
daily = format_alphavantage_to_db_daily(daily, ticker)
db.bulk_insert_df("daily", daily)


db_daily = db.query_db("SELECT * FROM daily")
print(db_daily)
db_daily = db.query_db("SELECT * FROM Product")
print(db_daily)
db_daily = db.query_db("SELECT * FROM Market")
print(db_daily)
