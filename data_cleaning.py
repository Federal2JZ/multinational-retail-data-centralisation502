import pandas as pd
import numpy as np


class DataCleaning:
    """
    A class for cleaning various types of data.
    """

    def __init__(self, dataframe=None):
        """
        Initialize the DataCleaning instance.

        Parameters:
        - dataframe: The pandas DataFrame to be cleaned.
        """
        self.dataframe = dataframe

    # Clean legacy user data
    def clean_user_data(self, legacy_users_table):
        """
        Clean legacy user data.

        Parameters:
        - legacy_users_table: The DataFrame containing legacy user data.

        Returns:
        - cleaned_users_table: The cleaned DataFrame of user data.
        """
        legacy_users_table = legacy_users_table.copy()  # not to get warning SettingWithCopyWarning

        # Replace 'NULL' values with NaN
        legacy_users_table.replace('NULL', np.NaN, inplace=True)

        # Drop rows with missing values in key columns
        legacy_users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)

        # Convert date columns to datetime format
        legacy_users_table['date_of_birth'] = pd.to_datetime(legacy_users_table['date_of_birth'], errors='ignore')
        legacy_users_table['join_date'] = pd.to_datetime(legacy_users_table['join_date'], errors='coerce')

        # Drop rows with missing values in the 'join_date' column
        legacy_users_table = legacy_users_table.dropna(subset=['join_date'])

        # Clean 'phone_number' column
        legacy_users_table['phone_number'] = legacy_users_table['phone_number'].str.replace('/W', '')

        # Remove duplicate entries based on 'email_address'
        legacy_users_table = legacy_users_table.drop_duplicates(subset=['email_address'])

        # Drop the first column (index column)
        legacy_users_table.drop(legacy_users_table.columns[0], axis=1, inplace=True)

        return legacy_users_table 
    
    # Clean card data
    def clean_card_data(self, card_data_table):
        """
        Clean card data.

        Parameters:
        - card_data_table: The DataFrame containing card data.

        Returns:
        - cleaned_card_data: The cleaned DataFrame of card data.
        """
        # Replace 'NULL' values with NaN
        card_data_table.replace('NULL', np.NaN, inplace=True)

        # Drop rows with missing values in the 'card_number' column
        card_data_table.dropna(subset=['card_number'], how='any', axis=0, inplace=True)

        # Remove rows where 'card_number' contains letters or '?'
        card_data_table = card_data_table[~card_data_table['card_number'].str.contains('[a-zA-Z?]', na=False)]

        return card_data_table
    
    # Clean store data
    def clean_store_data(self, store_data):
        """
        Clean store data.

        Parameters:
        - store_data: The DataFrame containing store data.

        Returns:
        - cleaned_store_data: The cleaned DataFrame of store data.
        """
        # Reset index to avoid duplicate indices after manipulation
        store_data = store_data.reset_index(drop=True)

        # Replace 'NULL' values with NaN
        store_data.replace('NULL', np.NaN, inplace=True)

        # Convert 'opening_date' column to datetime format
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors ='coerce')

        store_data.loc[[31, 179, 248, 341, 375], 'staff_numbers'] = [78, 30, 80, 97, 39] # had to find values where it was just a string of letters and replaced them

        # Convert 'staff_numbers' to numeric, drop rows with missing values
        store_data['staff_numbers'] = pd.to_numeric(store_data['staff_numbers'], errors='coerce')
        store_data.dropna(subset=['staff_numbers'], axis=0, inplace=True)

        # Clean 'continent' column
        store_data['continent'] = store_data['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')

        return store_data
    
    # Convert product weights
    def convert_product_weights(self, products_df):
        """
        Convert product weights to a unified format.

        Parameters:
        - products_df: The DataFrame containing product data.

        Returns:
        - converted_products_df: The DataFrame with weights converted to a unified format.
        """
        def convert_weight(weight):
            try:
                unit_factors = {'g': 1, 'ml': 0.001, 'k': 1000}
                return float(weight) if isinstance(weight, (int, float)) else float(weight[:-1]) * unit_factors.get(weight[-1], 1)
            except (ValueError, TypeError):
                return None

        products_df['weight'] = products_df['weight'].apply(convert_weight)
        return products_df
    
    # Clean products data
    def clean_products_data(self, data):
        """
        Clean products data.

        Parameters:
        - data: The DataFrame containing products data.

        Returns:
        - cleaned_data: The cleaned DataFrame of products data.
        """
        # Replace 'NULL' values with NaN
        data.replace('NULL', np.NaN, inplace=True)

        # Convert 'date_added' column to datetime format
        data['date_added'] = pd.to_datetime(data['date_added'], errors='coerce')

        # Drop rows with missing values in 'date_added'
        data.dropna(subset=['date_added'], how='any', axis=0, inplace=True)

        # Convert 'weight' column to string
        data['weight'] = data['weight'].astype(str)

        # Clean 'weight' column by removing spaces after dots
        data['weight'] = data['weight'].apply(lambda x: x.replace(' .', ''))

        # Extract numeric values from 'weight' column where it contains 'x'
        temp_cols = data.loc[data.weight.str.contains('x'), 'weight'].str.split('x', expand=True)
        numeric_cols = temp_cols.apply(lambda x: pd.to_numeric(x.str.extract('(\d+\.?\d*)', expand=False)), axis=1)
        final_weight = numeric_cols.prod(axis=1)
        data.loc[data.weight.str.contains('x'), 'weight'] = final_weight

        # Lowercase and strip whitespace in 'weight' column
        data['weight'] = data['weight'].apply(lambda x: str(x).lower().strip())

         # Drop the first column (index column)
        data.drop(data.columns[0], axis=1, inplace=True)

        return data
    
    # Clean date times data
    def clean_date_times_data(self, data):
        """
        Clean date times data.

        Parameters:
        - data: A dictionary containing date time data.

        Returns:
        - cleaned_data: The DataFrame of cleaned date time data.
        """
        # Convert dictionary to a DataFrame
        data = pd.DataFrame.from_dict(data)

        # Convert 'year' column to numeric, drop rows with missing values
        data['year'] = pd.to_numeric(data['year'], errors='coerce')
        data.dropna(subset=['year'], how='any', axis=0, inplace=True)

        return data
    
    # Clean orders data
    def clean_orders_data(self, data):
        """
        Clean orders data.

        Parameters:
        - data: The DataFrame containing orders data.

        Returns:
        - cleaned_data: The DataFrame of cleaned orders data.
        """
        # Drop unnecessary columns
        data.drop("level_0", axis=1, inplace=True) 
        data.drop("1", axis=1, inplace=True) 
        data.drop(data.columns[0], axis=1, inplace=True)
        data.drop('first_name', axis=1, inplace=True)
        data.drop('last_name', axis=1, inplace=True)
        return data