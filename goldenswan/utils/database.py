import pandas as pd
import psycopg2
import sqlalchemy

from GS_config import DB_CONFIG


# This is a decorator function
def dbConnect(func):
    def inner(*args, **func_params):

        conn = psycopg2.connect(
            database=DB_CONFIG["name"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
        )
        cur = conn.cursor()

        output = func(*args, **func_params, cursor=cur, connection=conn)

        conn.close()
        cur.close()

        return output

    return inner


@dbConnect
def query_db(query, cursor, connection, fetch_output=True):
    """[summary]

    Args:
        query ([type]): [description]
        cursor ([type]): [description]
        connection ([type]): [description]
        fetch_output (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    cursor.execute(query)

    if not fetch_output:
        return 1

    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    output_df = pd.DataFrame(rows, columns=colnames)

    return output_df


def _delete_records_before_insert(db_object, df, table_name):
    """

    Args:
        db_object ([type]): [description]
        df ([type]): [description]
        table_name ([type]): [description]
    """

    # Delete before insert
    if table_name in ["daily", "intraday_30"]:
        query = (
            "DELETE FROM "
            + table_name
            + " WHERE time_st in ("
            + str(set([str(x) for x in df["time_st"]]))[1:-1]
            + ") AND product_id in ("
            + str(set(df["product_id"]))[1:-1] + ");"
        )
        db_object.execute(query)

    elif table_name == "product":
        query = (
            "DELETE FROM "
            + table_name
            + " WHERE symbol in ("
            + str(set(df["symbol"]))[1:-1] + ");"
        )
        db_object.execute(query)

    elif table_name == "market":
        query = (
            "DELETE FROM "
            + table_name
            + " WHERE name in ("
            + str(set(df["name"]))[1:-1] + ");"
        )
        db_object.execute(query)


def bulk_insert_df(table_name, df, delete_before_insert=True):
    """[summary]

    Args:
        df ([type]): [description]
        table_name ([type]): [description]
    """
    # [DB_FLAVOR]+[DB_PYTHON_LIBRARY]://[USERNAME]:[PASSWORD]@[DB_HOST]:[PORT]/[DB_NAME]
    sqlalchemy_uri = f"postgres+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:" \
                     f"{DB_CONFIG['port']}/{DB_CONFIG['name']}"
    
    engine = sqlalchemy.create_engine(sqlalchemy_uri, echo=False,  use_batch_mode=True)

    # Delete before insert
    if delete_before_insert:
        _delete_records_before_insert(engine, df, table_name)
    
    df.to_sql(table_name, engine, index=False, if_exists="append")

    return str(df.shape[0]) + " rows inserted"


@dbConnect
def insert_df_row_by_row(table_name, df, cursor, connection, delete_before_insert=True):
    """[summary]

    Args:
        table_name ([type]): [description]
        df ([type]): [description]
        cursor ([type]): [description]
        connection ([type]): [description]
        update_value (bool, optional): [description]. Defaults to True.
        update_value_table (bool, optional): [description]. Defaults to False.
        update_product_table (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """

    # Delete before insert
    if delete_before_insert:
        _delete_records_before_insert(cursor, df, table_name)

    # Add row by row
    cols = ",".join([str(i) for i in df.columns.tolist()])
    for _, row in df.iterrows():

        query = (
            "INSERT INTO "
            + table_name
            + " ("
            + cols
            + ") VALUES ("
            + "'{}'," * (len(row) - 1)
            + "{})"
        )
        # print(query.format(row))
        cursor.execute(query.format(*list(row)))
        connection.commit()

    return str(df.shape[0]) + " rows inserted"


def read_data(ticker, market, table, start_date="01-01-1990", end_date="01-01-2999"):
    """[summary]

    Args:
        ticker ([type]): List of tickers
        market ([type]): [description]
        table ([type]): [description]
        start_date (str, optional): [description]. Defaults to "01-01-1990".
        end_date (str, optional): [description]. Defaults to "01-01-2999".

    Returns:
        [type]: [description]
    """    

    if type(ticker) == str:
        ticker = [ticker]

    query = f"""SELECT product_id 
                FROM product a INNER JOIN market b 
                ON a.market_id = b.market_id
                WHERE b.name = '{market}' AND a.symbol in ({str(set(ticker))[1:-1]});"""

    product_id = query_db(query)  # pylint: disable=E1120

    query = f"""SELECT * 
                FROM {table} 
                WHERE product_id in ({str(set(product_id["product_id"]))[1:-1]}) 
                AND time_st >= '{start_date}'
                AND time_st <= '{end_date}';"""

    df = query_db(query)  # pylint: disable=E1120

    return df
