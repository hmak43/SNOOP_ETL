import pandas as pd
import os, json
from helper_utils import (currency_type_counter, LoggerClass, get_latest_filename)
from datetime import datetime
import warnings
from typing import Dict, Any, Tuple, List
from config import (raw_data_path, removed_data_path, final_data_path, log_path, date_fmt, allowed_currency, REQUIRED_COLUMNS) 


# Remove Pandas warning.

from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

# Pick up file from "S3" or local file system.

raw_data_path = r"data/raw_data/"
removed_data_path = r"data/removed_data"
final_data_path = r"data/final_data"
log_path = r"data/logs/" # config
date_fmt = '%Y-%m-%d' # config
allowed_currency = ["GBP", "USD", "EUR"] # config
REQUIRED_COLUMNS = "customerId","customerName","transactionId","transactionDate","sourceDate","merchantId","categoryId","currency","amount","description"

latest_file = get_latest_filename(raw_data_path, date_fmt=date_fmt)

Logger = LoggerClass(path = "logs", latest_filename = latest_file)

def read_json(path: str, file: str) -> Dict[str, Any]:
    """

    Reads a JSON file from the specified path and returns the 'transactions' field.

    Params:
    - path (str): The path to the directory containing the JSON file.
    - file (str): The name of the JSON file.

    Returns:
    - Dict[str, Any]: A dictionary containing the 'transactions' field from the JSON file.

    Raises:
    - FileNotFoundError: If the specified file is not found.
    - json.JSONDecodeError: If the JSON decoding fails.

    """
    try:
        with open(os.path.join(path, file), 'r') as f:
            json_data = json.load(f)
            transactions = json_data.get("transactions", [])
            return transactions
    except FileNotFoundError as e:
        Logger.logging_function("File error!")
        raise FileNotFoundError(f"File not found: {os.path.join(path, file)}") from e
    except json.JSONDecodeError as e:
        Logger.logging_function("JSON error!")
        raise json.JSONDecodeError(f"Error decoding JSON file: {os.path.join(path, file)}") from e



def initial_df_quality_checks(df : pd.DataFrame):
    """
    Performs initial quality checks on a DataFrame.
    Each step is explained within the function.

    Parameters:
    - df (pd.DataFrame): The DataFrame to perform quality checks on.

    Returns:
    - pd.DataFrame: The DataFrame with renamed columns if necessary.

    Raises:
    - Exception: If the DataFrame is empty or if the columns don't match the requirements.

    """

    global REQUIRED_COLUMNS

    r,c = df.shape

    # Stops processing if file has zero rows.
    if r == 0:

        Logger.logging_function("ERROR: File is empty. Exiting.")
        raise Exception("File is empty. Exiting.")
        exit()
    
    df_columns = list(df.columns)
    df_columns_lowercase = list(map(str.lower, df_columns))
    REQUIRED_COLUMNS_lowercase = list(map(str.lower, REQUIRED_COLUMNS))

    # Stops processing if number of columns don't match required (required rows saved in config.py)
    if len(df_columns) != len(REQUIRED_COLUMNS):

        message = "Number of columns in latest transactions does not match requirements. Exiting!"

        Logger.logging_function(f"ERROR: {message}")
        raise Exception(f"{message}")
        exit()
    
    # Stops processing if any column(s) don't match required columns.
    # The following is done to check whether columns are in the correct order and spelled correctly.

    try:
        # Make column names lowercase
        df = df.rename(columns={old_col_name : new_col_name for old_col_name, new_col_name in zip(df_columns, df_columns_lowercase)}) 

        # Change order using column names saved in config.py
        df = df[REQUIRED_COLUMNS_lowercase] 
    except:

        # If column names cannot be reordered, then error is raised.
        message = "Columns names do not match. Exiting!"
        Logger.logging_function(f"ERROR: {message}")
        raise Exception(f"{message}")

    else:

        # If no errir raised, then columns are renamed as per requirements.
        df = df.rename(columns={old_col_name : new_col_name for old_col_name, new_col_name in zip(df.columns, REQUIRED_COLUMNS)})

    row_text = f"Total number of rows in raw file: {r}"
    column_text = f"Total number of columns: {c}"

    Logger.logging_function(row_text)
    Logger.logging_function(column_text)

    col_desc_text = ""

    for n,col in enumerate(df_columns, 1):

        dtype_str = str(df[f"{col}"].dtype)
        col_desc_text += f"Column {str(n)}  -->\tName: {col}, DataType : {dtype_str} \n"

    Logger.logging_function(col_desc_text)

    return df



def validate_currency(df: pd.DataFrame, allowed_currency: List[str], latest_filename: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate currency types in a DataFrame against a list of allowed currencies.

    Parameters:
    - df (pd.DataFrame): The DataFrame to validate.
    - allowed_currency (List[str]): List of allowed currency types.
    - latest_filename (str): The name of the latest file being processed.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames -
      1. DataFrame with rows containing valid currencies.
      2. DataFrame with rows containing invalid currencies.

    """

    valid_currency_df = df.loc[  df['currency'].isin(allowed_currency) ] 
    valid_currency_df = valid_currency_df.reset_index(drop=True)

    invalid_currency_df = df.loc[~ df['currency'].isin(allowed_currency)] 

    invalid_currency_log = currency_type_counter(invalid_currency_df)
    Logger.logging_function(invalid_currency_log)

    invalid_currency_df["ErrorType"] = "INCORRECT CURRENCY = " + invalid_currency_df["currency"]
    invalid_currency_df["FileName"] = latest_file

    return valid_currency_df, invalid_currency_df

def check_invalid_transaction_date(df: pd.DataFrame, date_fmt: str, latest_filename: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Check the validity of transaction dates in a DataFrame based on a specified date format.

    Parameters:
    - df (pd.DataFrame): The DataFrame to check.
    - date_fmt (str): The expected date format for the 'transactionDate' column.
    - latest_filename (str): The name of the latest file being processed.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames -
      1. DataFrame with rows containing correctly formatted dates.
      2. DataFrame with rows containing incorrectly formatted dates.
    """

    df['transactionDateErrorCheck'] = pd.to_datetime(df['transactionDate'],format= date_fmt, errors='coerce')

    correct_datefmt_df = df[~df['transactionDateErrorCheck'].isna()] # NOT NAN
    correct_datefmt_df = correct_datefmt_df.drop(columns=['transactionDateErrorCheck'])
    correct_datefmt_df = correct_datefmt_df.reset_index(drop=True)

    incorrect_datefmt_df = df[df['transactionDateErrorCheck'].isna()]
    incorrect_datefmt_df = incorrect_datefmt_df.drop(columns=['transactionDateErrorCheck'])
    incorrect_datefmt_df["ErrorType"] = "INCORRECT DATE FORMAT"
    incorrect_datefmt_df["FileName"] = latest_file

    num_of_rows = incorrect_datefmt_df.shape[0]

    incorrect_date_log = f"Found {num_of_rows} incorrect date rows (removed)." 
    Logger.logging_function(incorrect_date_log)

    return correct_datefmt_df, incorrect_datefmt_df


def handle_duplicates(df: pd.DataFrame, latest_filename: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Handle duplicates in a DataFrame based on the 'transactionId' column and 'sourceDate'.
    Please note: The transaction with the latest sourceDate is kept as we're assuming that the latest sourceDate 
    gives the most up to date (amended) record.

    Parameters:
    - df (pd.DataFrame): The DataFrame to handle duplicates in.
    - latest_filename (str): The name of the latest file being processed.

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames -
      1. DataFrame with duplicates removed based on the latest 'sourceDate'.
      2. DataFrame with rows containing removed duplicate entries.
    """

    df_all_duplicates_only = df[df.duplicated(subset = ["transactionId"], keep=False)]
    df_all_duplicates_only['sourceDate'] = pd.to_datetime(df_all_duplicates_only['sourceDate'], errors='coerce')

    # Keep duplicate entry based on latest Source Date.

    df_latest_dup_entry = df_all_duplicates_only.sort_values(by = ['transactionId',"sourceDate"], ascending=True)
    df_latest_dup_entry = df_latest_dup_entry.drop_duplicates(subset = ["transactionId"], keep="last")

    # Separate DataFrame of duplicate entries removed.

    df_removed_duplicates = df_all_duplicates_only[~df_all_duplicates_only.isin(df_latest_dup_entry).any(axis=1)]
    df_removed_duplicates["ErrorType"] = "DUPLICATE VALUE (Removed from final DataFrame)"
    df_removed_duplicates["FileName"] = latest_file

    num_of_rows = df_removed_duplicates.shape[0]

    duplicate_rows_log = f"Found {num_of_rows} duplicate rows (removed)." 
    Logger.logging_function(duplicate_rows_log)

    # df_latest_dup_entry.to_csv(r'data\final_data\check_duplicates.csv', index = False)
    
    df['sourceDate'] = pd.to_datetime(df['sourceDate'], errors='coerce')
    df = df.sort_values(by = ['transactionId',"sourceDate"], ascending=True)
    df = df.drop_duplicates(subset = ["transactionId"], keep="last")
    df = df.reset_index(drop=True)

    return df, df_removed_duplicates


def convert_dtypes(df: pd.DataFrame, date_fmt: str) -> pd.DataFrame:
    """
    Convert data types of specific columns in a DataFrame.

    Parameters:
    - df (pd.DataFrame): The DataFrame to convert data types for.
    - date_fmt (str): The expected date format for the 'transactionDate' column.

    Returns:
    - pd.DataFrame: The DataFrame with converted data types.
    """

    df["transactionDate"] = pd.to_datetime(df['transactionDate'],format = date_fmt, errors='coerce')
    df["amount"] = df["amount"].astype(float)
    return df

def run_all_validation_checks(
    allowed_currency: List[str],
    latest_file: str,
    date_fmt: str,
    removed_data_path: str,
    final_data_path: str
) -> pd.DataFrame:
    """
    Run a series of validation checks on a JSON file of transactions.

    Parameters:
    - allowed_currency (List[str]): List of allowed currency types.
    - latest_file (str): The name of the latest JSON file being processed.
    - date_fmt (str): The expected date format for the 'transactionDate' column.
    - removed_data_path (str): The path to store the removed data CSV files.
    - final_data_path (str): The path to store the final validated data CSV file.

    Returns:
    - pd.DataFrame: The DataFrame with validated data.
    """

    transactions_json = read_json(path = raw_data_path, file = latest_file)

    df = pd.DataFrame(transactions_json);

    df = initial_df_quality_checks(df)

    df_v, df_i1 = validate_currency(df, allowed_currency, latest_file)
    df_v ,df_i2 = check_invalid_transaction_date(df_v, date_fmt, latest_file)
    df_v ,df_i3 = handle_duplicates(df_v, latest_file)
    df_v = convert_dtypes(df_v, date_fmt)

    df_i = pd.concat([df_i1, df_i2, df_i3])

    filename_without_ext = latest_file.replace(".json", "")
    
    df_v.to_csv(os.path.join(final_data_path, f"validated_{filename_without_ext}.csv"), index=False)
    df_i.to_csv(os.path.join(removed_data_path, f"removed_rows_{filename_without_ext}.csv"), index=False)
        
    return df_v



def create_customer_df(df):

    customers_df = df[["customerId", "customerName"]]
    customers_df = customers_df.drop_duplicates(subset=["customerId", "customerName"])
    customers_df = customers_df.reset_index(drop = True)
    customers_df["createdOn"] = pd.to_datetime(datetime.now(),format="%Y-%m-%d")

    filename_without_ext = latest_file.replace(".json", "")

    customers_df.to_csv(os.path.join(final_data_path,f"customers_only_{filename_without_ext}.csv"), index=False)


def create_transactions_df(df):
    
    transactions_df = df[["customerId", "transactionId", "transactionDate", "currency","amount"]]
    transactions_df["createdOn"] = pd.to_datetime(datetime.now(),format="%Y-%m-%d")

    filename_without_ext = latest_file.replace(".json", "")

    transactions_df.to_csv(os.path.join(final_data_path,f"transactions_only_{filename_without_ext}.csv"), index=False)


def run_all():    

    df_v = run_all_validation_checks(allowed_currency, latest_file, date_fmt, removed_data_path, final_data_path)
    create_customer_df(df_v)
    create_transactions_df(df_v)

run_all()