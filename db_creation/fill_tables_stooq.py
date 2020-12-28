########################################################################################################################
# Update the database with https://stooq.com/db/h/
########################################################################################################################
import pandas as pd
import os
import sys

sys.path.insert(0, "/home/database/goldenswan")

import goldenswan.utils.database as db


def format_stooq_to_db(df, product_symbol):
    """
    Adapt the format returned by alphavantage daily adjusted to DB insert format
    :param df:
    :param product_symbol:
    :return:
    """

    # Convert to timestamp
    df["ts"] = df["<DATE>"].apply(str) + df["<TIME>"].apply(str)
    df["ts"] = df.ts.apply(pd.Timestamp)

    # Load the product ID:
    product_id = db.query_db("SELECT product_id FROM product WHERE symbol = '{}'".format(product_symbol))

    # Create the new df:
    if len(product_id)!=0:

        new_df = {"time_st": pd.to_datetime(df.ts),
                  "product_id": [product_id.iloc[0, 0]] * df.shape[0],
                  "open": df['<OPEN>'],
                  "high": df['<HIGH>'],
                  "low": df["<LOW>"],
                  "close": df["<CLOSE>"],
                  "volume": df["<VOL>"]}

        return pd.DataFrame(new_df)

    else:
        # PENDENT AFEGIR codi que afegeixi el producte a la taula product
        return None


def fill_table(data_dir, table_name, replace_info=True):
    # Load all the files in the folder and subfolders:
    folders = os.listdir(data_dir)
    files = []
    for folder in folders:
        files = os.listdir(data_dir + str(folder) + "/")

        for file in files:
            ticker = file[:-7].upper()
            path = data_dir + str(folder) + "/" + file
            print(path)
            if os.stat(path).st_size!=0:
                data = pd.read_csv(path)
                df = format_stooq_to_db(data, ticker)
            else:
                df=None

            if isinstance(df, pd.DataFrame):

                print("Uploading " + ticker + " with " + str(df.shape[0]) + " rows")
                db.insert_df_row_by_row(table_name, df, update_value_table=replace_info)
            else:
                print("lala")


# Fill nasdaq
#print("\n\n5min nasdaq\n")
#fill_table(data_dir="/home/database/goldenswan/data/5_us_txt/data/5 min/us/nasdaq stocks/", table_name="intraday_5")
print("\n\n60min nasdaq\n")
fill_table(data_dir="/home/database/goldenswan/data/h_us_txt/data/hourly/us/nasdaq stocks/", table_name="intraday_60")

# Fill nyse
print("\n\n5min nyse\n")
fill_table(data_dir="/home/database/goldenswan/data/5_us_txt/data/5 min/us/nyse stocks/", table_name="intraday_5")
print("\n\n60min nyse\n")
fill_table(data_dir="/home/database/goldenswan/data/h_us_txt/data/hourly/us/nyse stocks/", table_name="intraday_60")
