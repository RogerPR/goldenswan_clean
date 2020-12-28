########################################################################################################################
# Upload data from https://stooq.com/db/h/ to the database
########################################################################################################################
import pandas as pd
import os
import goldenswan.utils.database as db

data_dir = "C:/Users/roger/Desktop/pych_projects/data/"

# Fill the product table with NASDAQ and NYSE STOCKS -------------------------------------------------------------------
nasdaq_1 = os.listdir(data_dir + "daily/data/daily/us/" + "nasdaq stocks/" + "1/")
nasdaq_2 = os.listdir(data_dir + "daily/data/daily/us/" + "nasdaq stocks/" + "2/")
nasdaq = [a[:-7].upper() for a in nasdaq_1 + nasdaq_2]
product_nasdaq = pd.DataFrame({"symbol": nasdaq,
                               "name": ["TBD"] * len(nasdaq),
                               "product_type": ["STOCKS"] * len(nasdaq),
                               "market_id": [1] * len(nasdaq)})


nyse_1 = os.listdir(data_dir + "daily/data/daily/us/" + "nyse stocks/" + "1/")
nyse_2 = os.listdir(data_dir + "daily/data/daily/us/" + "nyse stocks/" + "2/")
nyse_3 = os.listdir(data_dir + "daily/data/daily/us/" + "nyse stocks/" + "3/")
nyse = [a[:-7].upper() for a in nyse_1 + nyse_2 + nyse_3]
product_nyse = pd.DataFrame({"symbol": nyse,
                             "name": ["TBD"] * len(nyse),
                             "product_type": ["STOCKS"] * len(nyse),
                             "market_id": [2] * len(nyse)})

product = pd.concat([product_nasdaq, product_nyse], axis=0)
db.insert_df_row_by_row("product", product)
