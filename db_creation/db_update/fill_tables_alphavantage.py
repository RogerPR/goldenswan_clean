########################################################################################################################
# Use alphavantage to upload data
########################################################################################################################
import time
import sys
import pandas as pd
import logging

import goldenswan.utils.database as db
from goldenswan.utils.alphavantage import (
    request_api_data,
    format_alphavantage_to_db_daily,
    format_alphavantage_to_db_intraday
)
from GS_config import DIR_ENV


def fill_db_from_alphavantage(tickers, table, replace_info=True, sleep_time=60):
    """
    Fills the database with the info provided by alphavantage for all the <tickers>
    :param tickers: pandas dataframe
    :param replace_info: Replaces the original information in the database
    :param sleep_time: amount of seconds to wait until next alphavantage query (after 5 queries)
    """

    alphavantage_code_dict = {
        "daily": "TIME_SERIES_DAILY_ADJUSTED",
        "intraday_5": "TIME_SERIES_INTRADAY",
        "intraday_60": "TIME_SERIES_INTRADAY",
    }
    alphavantage_interval_dict = {
        "daily": "15min",  # Not used but parameter req by alphavantage
        "intraday_5": "5min",
        "intraday_60": "60min",
    }
    counter = 0
    tickers_with_error = []
    for ticker in tickers:

        try:
            df = request_api_data(
                symbol=ticker,
                interval_type=alphavantage_code_dict[table],
                intraday_interval=alphavantage_interval_dict[table],
                outputsize="full",
            )

            if table == "daily":
                df = format_alphavantage_to_db_daily(df, ticker)
            
            else:
                df = format_alphavantage_to_db_intraday(df, ticker)

            print("Uploading " + ticker + " with " + str(df.shape[0]) + " rows")
            logging.info(
                "Uploading " + ticker + " with " + str(df.shape[0]) + " rows"
            )

            db.bulk_insert_df(table, df, delete_before_insert=True)
            # db.insert_df_row_by_row("daily", daily, update_value_table=replace_info)

            tickers = tickers[1:]

        except Exception as e:
            tickers_with_error.append(ticker)
            logging.info(f"Ticker {ticker} failed with error: {e}")

        counter += 1
        if counter == 5:
            print(f"sleeping for {sleep_time} sec ..")
            counter = 0
            time.sleep(sleep_time)

    logging.info("The following tickers had errors: " + str(tickers_with_error))


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s :: %(levelname)s :: %(message)s",
        handlers=[
            logging.FileHandler(
                DIR_ENV["root"] + "/db_creation/db_update/LOGFILE.log", mode="a"
            )
        ],
    )
    print(f"Updating table {sys.argv[1]}")
    logging.info(
        f"Updating table {sys.argv[1]} ----------------------------------------------------------------------"
    )
    logging.info(f"Reading tickers")

    tickers = pd.read_csv(
        DIR_ENV["root"] + "/db_creation/db_update/tickers_clean.txt"
    ).Symbol.to_list()
    fill_db_from_alphavantage(tickers, table=sys.argv[1], replace_info=True)

    logging.info(
        f"END Updating table {sys.argv[1]} ------------------------------------------------------------------"
    )
