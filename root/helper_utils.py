from collections import Counter
import pandas as pd
import os
from datetime import datetime

currency_allowed = ["GBP", "USD", "EUR"]

format_col_transactionDate = '%Y-%m-%d'
format_col_sourceDate = '%Y-%m-%dT%H:%M:%S'

def row_count(df, stage):

    r, c = (df.shape)
    row_text = f"Total number of rows in table at {stage} stage : {r}"
    column_text = f"Total number of columns: {c}"
    return r , c

def col_dtypes_check(df):

    orig_columns = list(df.columns);
    col_desc_text = ""

    for n , col in enumerate(orig_columns, 1):

        dtype1 = str(df[f"{col}"].dtype)
        dtype2 = ""

        if "str" or "obj" in dtype1:
            dtype2 = "Text or Alphanumeric"
        elif "int" in dtype1:
            dtype2 = "Integer (Numerical)"
        elif "float" in dtype1:
            dtype2 = "Decimal"
        elif "bool" in dtype1:
            dtype2 = "Flag (1 or 0)"

        col_desc_text += f"Column {str(n)}  -->\tName: {col}, DataType : {dtype2} \n"

    return col_desc_text

def null_check(df):

    rows_with_nulls = list(df[df.isnull().any(axis=1)].index.values)
    cols_with_nulls = list(df.columns[df.isnull().any(axis=0)].values)

    l_rows, c_rows = len(rows_with_nulls), len(cols_with_nulls)

    if not len(rows_with_nulls):
        print(f"No nulls found.")
        return 0
    else:
        print(f"Nulls present in table in rows: {rows_with_nulls}.")
        print(f"Nulls present in table in columns: {cols_with_nulls}.")


        dict_results =  {

                            "Rows" : (rows_with_nulls),
                            "Columns" : (cols_with_nulls)

                        }

        return dict_results


def currency_type_counter(invalid_currency_df):

    incorrect_curr_list = invalid_currency_df["currency"];
    incorrect_curr_list_Count = Counter(incorrect_curr_list);
    final_log = ""

    for currency_name, num_of_rows in incorrect_curr_list_Count.items():
        final_log += f"Found {num_of_rows} rows with currency: {currency_name}.\n"

    return final_log
        
class LoggerClass():

    def __init__(self, path, latest_filename) -> None:

        self.path = path
        self.filename = "log_" + latest_filename.replace(".json", ".log")
        self.file_w_path = os.path.join(self.path, self.filename)

        if os.path.exists(self.file_w_path):
            os.remove(self.file_w_path)

    def logging_function(self, log_text):

        with open(self.file_w_path, 'a') as f:

            f.write("\n")
            f.write(log_text)

def get_latest_filename(raw_data_path, date_fmt):

    all_files = os.listdir(raw_data_path)
    all_dates = []  

    for file in all_files:
        
        file = file.replace(".json", "")
        file_date = list(map(int, file.split("_")[1:]))
        date_ = datetime(*file_date)
        all_dates.append(date_)

    return "transactions" + "_" +  max(all_dates).strftime(date_fmt.replace("-", "_")) + ".json"