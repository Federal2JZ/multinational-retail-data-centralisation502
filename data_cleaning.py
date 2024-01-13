import pandas as pd
import numpy as np
import re

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
    
    def clean_store_data(self, stores_data):
        # Drop duplicates if necessary
        cleaned_data = stores_data.drop_duplicates()

        # Remove the "lat" column because its duplicate and empty
        cleaned_data = cleaned_data.drop(columns=['lat'])

        # Replace "NULL" strings with NaN
        cleaned_data.replace('NULL', np.nan, inplace=True)

        # Drop rows where all columns have NaN or empty string values
        cleaned_data.dropna(how='all', inplace=True)
        cleaned_data.replace('', np.nan, inplace=True)
        cleaned_data.dropna(how='all', inplace=True)

        # Define a regular expression pattern for random alphanumeric strings and apply the pattern check to each cell in the DataFrame
        random_pattern = re.compile(r'^[A-Za-z]{3}-[0-9]{2}-[A-Za-z]{3}$')
        random_mask = cleaned_data.apply(lambda x: x.map(lambda val: bool(random_pattern.match(str(val)))))

        # Drop rows where all columns have random alphanumeric strings
        cleaned_data = cleaned_data[~random_mask.all(axis=1)]

        # Convert 'latitude' to numeric, replacing non-numeric values with NaN
        cleaned_data['latitude'] = pd.to_numeric(cleaned_data['latitude'], errors='coerce')

        # Convert 'opening_date' to datetime, handling different date formats
        cleaned_data['opening_date'] = pd.to_datetime(cleaned_data['opening_date'], errors='coerce')

        # Convert 'longitude' to numeric, handling non-numeric values
        cleaned_data['longitude'] = pd.to_numeric(cleaned_data['longitude'], errors='coerce')

        # Specify data types for each column
        data_types = {
            "address": "object",
            "longitude": "float64",
            "locality": "object",
            "store_code": "object",
            "staff_numbers": "object",
            "opening_date": "datetime64[ns]",
            "store_type": "object",
            "latitude": "float64",
            "country_code": "object",
            "continent": "object",
        }
        
        # Convert columns to their specified data types
        cleaned_data = cleaned_data.astype(data_types)

        return cleaned_data