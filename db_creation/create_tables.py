import sys
sys.path.insert(0, "/home/gaelexp/github/goldenswan/")

from goldenswan.utils.database import dbConnect


@dbConnect
def create_tables(cursor, connection):

    # MARKET TABLE
    # This is test, later market will be market_id and will be foreign key
    query = """
               CREATE TABLE MARKET (
                   market_id SERIAL PRIMARY KEY,
                   name VARCHAR(50) NOT NULL,
                   country VARCHAR(50),
                   region VARCHAR(50),
                   timezone VARCHAR(50),
                   timediff NUMERIC,
                   UNIQUE (name)
                   ); 
            """
    cursor.execute(query)
    connection.commit()

    # PRODUCT TABLE
    # This is test, later market will be market_id and will be foreign key
    query = """
               CREATE TABLE PRODUCT (
                   product_id SERIAL PRIMARY KEY,
                   symbol VARCHAR(10) NOT NULL,
                   name VARCHAR(50) NOT NULL,
                   product_type VARCHAR(50) NOT NULL,
                   market_id INT NOT NULL,
                   UNIQUE (symbol),
                   CONSTRAINT fk_market
                      FOREIGN KEY(market_id) 
                      REFERENCES MARKET(market_id)
                   ); 
            """
    cursor.execute(query)
    connection.commit()

    # DAILY TABLE
    query = """
                CREATE TABLE DAILY (
                   id SERIAL PRIMARY KEY,
                   time_st TIMESTAMP NOT NULL,
                   product_id INT NOT NULL,
                   open NUMERIC,
                   high NUMERIC,
                   low NUMERIC,
                   close NUMERIC,
                   volume INT,
                   adjusted_close NUMERIC,
                   
                   UNIQUE (time_st, product_id),
                   CONSTRAINT fk_product
                      FOREIGN KEY(product_id) 
                      REFERENCES PRODUCT(product_id)
                );
            """
    cursor.execute(query)
    connection.commit()

    # Intraday 30 TABLE
    # This is test, later market will be market_id and will be foreing key
    query = """
                CREATE TABLE INTRADAY_30 (
                   id SERIAL PRIMARY KEY,
                   time_st TIMESTAMP NOT NULL,
                   product_id INT NOT NULL,
                   open NUMERIC,
                   high NUMERIC,
                   low NUMERIC,
                   close NUMERIC,
                   volume INT,
                   
                   UNIQUE (time_st, product_id),
                   CONSTRAINT fk_product
                      FOREIGN KEY(product_id) 
                      REFERENCES PRODUCT(product_id)
                      
                );
            """

    cursor.execute(query)
    connection.commit()

    # Intraday 5 TABLE
    # This is test, later market will be market_id and will be foreing key
    query = """
                 CREATE TABLE INTRADAY_5 (
                    id SERIAL PRIMARY KEY,
                    time_st TIMESTAMP NOT NULL,
                    product_id INT NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume INT,

                    UNIQUE (time_st, product_id),
                    CONSTRAINT fk_product
                       FOREIGN KEY(product_id) 
                       REFERENCES PRODUCT(product_id)

                 );
             """

    cursor.execute(query)
    connection.commit()

    # Intraday 60 TABLE
    # This is test, later market will be market_id and will be foreing key
    query = """
                 CREATE TABLE INTRADAY_60 (
                    id SERIAL PRIMARY KEY,
                    time_st TIMESTAMP NOT NULL,
                    product_id INT NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume INT,

                    UNIQUE (time_st, product_id),
                    CONSTRAINT fk_product
                       FOREIGN KEY(product_id) 
                       REFERENCES PRODUCT(product_id)

                 );
             """
    cursor.execute(query)
    connection.commit()

    connection.close()
    cursor.close()


if __name__ == "__main__":

    create_tables()
