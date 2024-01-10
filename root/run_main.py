import data_validation
import data_to_postgredb
from helper_utils import get_latest_filename
from config import *


latest_file = (get_latest_filename(raw_data_path, date_fmt))
latest_file_date = latest_file.replace(".json", "").split("_")[1:]
latest_file_date = "_".join(latest_file_date)
# print(latest_file_date)



data_validation.run_all()
data_to_postgredb.customer_SQLtable_update(latest_file_date)
data_to_postgredb.transaction_SQLtable_update(latest_file_date)