################################################################################
# Script to read and clean S&P500 tickers from Wikipedia
# https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
################################################################################

import pandas as pd

from GS_config import DIR_ENV
import goldenswan.utils.database as db


def clean_tickers():

    raw_tickers = pd.read_csv(
        DIR_ENV["root"] + "/db_creation/db_update/tickers.txt",
        sep="\t",
        lineterminator="\n",
        header=None,
    )

    raw_tickers.columns = [
        "Symbol",
        "Security",
        "SEC filings",
        "GICS Sector",
        "GICS Sub-Industry",
        "Headquarters Location",
        "Date first added",
        "CIK",
        "Founded",
    ]

    tickers_db = db.query_db("Select * from product;")
    tickers_not_in_db = list(set(raw_tickers.Symbol) - set(tickers_db.symbol))
    discarded_tickers = tickers_not_in_db + ['NVR', 'COO', 'BIO', 'HII', 'TFX', 'RE', 'MTD', 'ABMD', 'POOL', 'TDY']

    ticker_list = raw_tickers[[a not in discarded_tickers for a in raw_tickers.Symbol]][["Symbol", "Security"]]
    ticker_list.to_csv(DIR_ENV["root"] + "/db_creation/db_update/tickers_clean.txt", index=False)


if __name__ == "__main__":

    clean_tickers()
