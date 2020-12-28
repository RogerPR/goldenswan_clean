import pandas as pd
import numpy as np

import goldenswan.utils.database as db




market = "NASDAQ"
tickers = ["BABA", "AMZN"]
table = "daily"

aa = db.query_db("SELECT * FROM PRODUCT")
print(aa)

bb = db.query_db("SELECT * FROM MARKET")
print(bb)

raw_data = db.read_data(tickers, market, table)

def create_investment_opportuniy(raw_data, day):
    """
    Create an investment opportunity, generating a context (X vars) and the
    reward over distinct time intervals (Y vars)

    Args:
        raw_data ([type]): [description]
        day ([type]): [description]
    """

    