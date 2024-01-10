import pandas as pd
import numpy as np


class DataCleaning:
    def __init__(self, dataframe=None):
        self.dataframe = dataframe

    def clean_user_data(self):
        if self.dataframe is None:
            raise ValueError("DataFrame not provided to DataCleaning instance")

        # Replace 'NULL' values with NaN
        self.dataframe = self.dataframe.replace('NULL', np.nan)

        # Convert date columns to datetime format
        date_columns = ['date_of_birth', 'join_date']
        self.dataframe[date_columns] = self.dataframe[date_columns].apply(pd.to_datetime, errors='coerce')

        # Drop rows with incorrect or missing values in key columns
        key_columns = ['first_name', 'last_name', 'user_uuid']
        self.dataframe = self.dataframe.dropna(subset=key_columns)

        # Remove rows where date_of_birth is in the future
        self.dataframe = self.dataframe[self.dataframe['date_of_birth'] <= pd.Timestamp.now()]

        return self.dataframe

    def clean_card_data(self):
        if self.dataframe is None:
            raise ValueError("DataFrame not provided to DataCleaning instance")

        # Remove rows with 'NULL' in card_number
        cleaned_data = self.dataframe[self.dataframe['card_number'] != 'NULL'].copy()

        # Convert expiry_date to datetime format with specified format
        cleaned_data.loc[:, 'expiry_date'] = pd.to_datetime(cleaned_data['expiry_date'], format='%m/%y', errors='coerce')

        # Validate date_payment_confirmed using the validate function
        cleaned_data.loc[:, 'date_payment_confirmed'] = pd.to_datetime(cleaned_data['date_payment_confirmed'], errors='coerce')

        # Drop rows with invalid date_payment_confirmed
        cleaned_data = cleaned_data.dropna(subset=['date_payment_confirmed'])

        return cleaned_data