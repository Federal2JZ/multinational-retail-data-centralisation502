import pandas as pd
from datetime import datetime
import numpy as np


class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, user_data):
        # Replace 'NULL' values with NaN
        user_data.replace('NULL', np.nan, inplace=True)

        # Convert date_of_birth and join_date columns to datetime format
        date_columns = ['date_of_birth', 'join_date']
        user_data[date_columns] = user_data[date_columns].apply(pd.to_datetime, errors='coerce')

        # Drop rows with incorrect or missing values in key columns
        key_columns = ['first_name', 'last_name', 'user_uuid']
        user_data.dropna(subset=key_columns, inplace=True)

        # Remove rows where date_of_birth is in the future
        user_data = user_data[user_data['date_of_birth'] <= pd.to_datetime('today')]

        return user_data