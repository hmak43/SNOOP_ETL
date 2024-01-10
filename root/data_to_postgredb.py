import psycopg2
import pandas as pd



def customer_SQLtable_update(latest_file_date):

    df_customers = pd.read_csv(fr"data\final_data\customers_only_transactions_{latest_file_date}.csv")

    cols = "CUSTOMER_ID,CUSTOMER_NAME,CREATED_ON"

    table ="customers"

    create_table_query = """CREATE TABLE IF NOT EXISTS customersTest
        (
    CUSTOMER_ID VARCHAR (100) NOT NULL PRIMARY KEY, 
    CUSTOMER_NAME VARCHAR (100) NOT NULL,
    CREATED_ON timestamp 

        );"""

    select_customers_query = "SELECT * FROM CUSTOMERS"

    conn = psycopg2.connect("dbname='{db}' user='{user}' host='{host}' port='{port}' password='{passwd}'".format(
                user="postgres",
                passwd="password143",
                host="localhost",
                port="5432",
                db="SNOOP_DATABASE"))


    with conn.cursor() as cur:

        cur.execute(create_table_query)
        conn.commit()

        all_customer_ids = []

        try:
            cur.execute(select_customers_query)
            all_records = cur.fetchall()    
            for row in all_records:
                all_customer_ids.append(row[0])
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()

        if len(all_customer_ids) > 0:

            df_customers = df_customers[~ df_customers["customerId"].isin(all_customer_ids)]
            print(df_customers)

        if df_customers.shape[0] == 0:

            print("Customer table already up to date.")
            return

        tuples = list(set([tuple(x) for x in df_customers.to_numpy()]))
        
        tuples_as_string = ""
        print(tuples_as_string)

        for x in tuples:
            tuples_as_string += str(x) + ","

        tuples_as_string = tuples_as_string[:-1]

        insert_records_query = f"INSERT INTO {table}({cols}) VALUES {tuples_as_string}" 
        print(insert_records_query)
        try:
            cur.execute(insert_records_query)
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()


def transaction_SQLtable_update(latest_file_date):

    df_transactions = pd.read_csv(fr"data\final_data\transactions_only_transactions_{latest_file_date}.csv")
    cols = "CUSTOMER_ID,TRANSACTION_ID,TRANSACTION_DATE,CURRENCY,AMOUNT,CREATED_ON"

    table ="TRANSACTIONS"

    create_table_query = """CREATE TABLE IF NOT EXISTS transactions
        (
    CUSTOMER_ID VARCHAR (100) NOT NULL , 
    TRANSACTION_ID VARCHAR (100) NOT NULL PRIMARY KEY,
    TRANSACTION_DATE date,
    CURRENCY varchar(20),
    AMOUNT DECIMAL(4),
    CREATED_ON timestamp 

        );"""

    select_customers_query = f"SELECT * FROM {table}"

    conn = psycopg2.connect("dbname='{db}' user='{user}' host='{host}' port='{port}' password='{passwd}'".format(
                user="postgres",
                passwd="password143",
                host="localhost",
                port="5432",
                db="SNOOP_DATABASE"))


    with conn.cursor() as cur:

        cur.execute(create_table_query)
        conn.commit()

        all_transaction_ids = []

        try:
            cur.execute(select_customers_query)
            all_records = cur.fetchall()    
            for row in all_records:
                all_transaction_ids.append(row[1])
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()

        if len(all_transaction_ids) > 0:

            df_transactions = df_transactions[~ df_transactions["transactionId"].isin(all_transaction_ids)]
            print(df_transactions)

        if df_transactions.shape[0] == 0:

            print("Transaction table already up to date.")
            return

        tuples = list(set([tuple(x) for x in df_transactions.to_numpy()]))
        
        tuples_as_string = ""
        # print(tuples_as_string)

        for x in tuples:
            tuples_as_string += str(x) + ","

        tuples_as_string = tuples_as_string[:-1]

        insert_records_query = f"INSERT INTO {table}({cols}) VALUES {tuples_as_string}" 
        
        try:
            cur.execute(insert_records_query)
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()


