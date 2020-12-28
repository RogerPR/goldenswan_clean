#### ALPHA VANTAGE UTILS
import pandas as pd
import requests
from io import StringIO
from GS_config import API_KEYS
import goldenswan.utils.database as db

api_key = API_KEYS["alphavantage"]


def request_api_data(symbol, interval_type="TIME_SERIES_DAILY", outputsize="compact", intraday_interval="15min",
                     key=api_key):
    """
    https://www.alphavantage.co/documentation/

    :param symbol:
    :param interval_type: TIME_SERIES_INTRADAY / TIME_SERIES_DAILY / TIME_SERIES_DAILY_ADJUSTED /
                          TIME_SERIES_WEEKLY / TIME_SERIES_WEEKLY_ADJUSTED / TIME_SERIES_MONTHLY /
                          TIME_SERIES_MONTHLY_ADJUSTED / GLOBAL_QUOTE (This one for last value)
    :param outputsize: compact = last 100 instances, full = 20 years
    :param intraday_interval:  1min, 5min, 15min, 30min, 60min
    :param key:
    :return:
    """

    API_URL = "https://www.alphavantage.co/query"

    data = {
        "function": interval_type,
        "symbol": symbol,
        "outputsize": outputsize,
        "interval": intraday_interval,
        "datatype": "csv",
        "apikey": key
    }

    response = requests.get(API_URL, params=data)
    df = pd.read_csv(StringIO(response.text))

    return df

# amazon = request_api_data(symbol="AMZN", interval_type="TIME_SERIES_DAILY_ADJUSTED", outputsize="full")
# amazon_intra = request_api_data(symbol="AMZN", interval_type="TIME_SERIES_INTRADAY",
#                                 outputsize="full", intraday_interval="30min")
# alibaba = request_api_data(symbol="BABA", interval_type="TIME_SERIES_MONTHLY", outputsize="full")
# alibaba_daily = request_api_data(symbol="BABA", interval_type="TIME_SERIES_INTRADAY", outputsize="full",
#                                  intraday_interval="1min")


def format_alphavantage_to_db_daily(df, product_symbol):
    """
    Adapt the format returned by alphavantage daily adjusted to DB insert format
    :param df:
    :param product_symbol:
    :return:
    """

    product_id = db.query_db("SELECT product_id FROM product WHERE symbol = '{}'".format(product_symbol))
    new_df = {"time_st": pd.to_datetime(df.timestamp),
              "product_id": [product_id.iloc[0, 0]] * df.shape[0],
              "open": df.open,
              "high": df.high,
              "low": df.low,
              "close": df.close,
              "volume": df.volume,
              "adjusted_close": df.adjusted_close}

    return pd.DataFrame(new_df)


def format_alphavantage_to_db_intraday(df, product_symbol):
    """
    Adapt the format returned by alphavantage intraday adjusted to DB insert format
    :param df:
    :param product_symbol:
    :return:
    """

    product_id = db.query_db("SELECT product_id FROM product WHERE symbol = '{}'".format(product_symbol))
    new_df = {"time_st": pd.to_datetime(df.timestamp),
              "product_id": [product_id.iloc[0, 0]] * df.shape[0],
              "open": df.open,
              "high": df.high,
              "low": df.low,
              "close": df.close,
              "volume": df.volume}

    return pd.DataFrame(new_df)
