import pandas as pd
import tabula as tb
import requests
import boto3
import os


class DataExtractor():
    """
    A class for extracting data from various sources.
    """

    def __init__(self, database_connector_instance=None, table_name=None):
        """
        Initialize the DataExtractor.

        Parameters:
        - database_connector_instance: An instance of the database connector.
        - table_name: The name of the table from which data will be extracted.
        """
        self.database_connector_instance = database_connector_instance
        self.table_name = table_name

    def read_rds_table(self, db_connector, table_name):
        """
        Read data from a table in a relational database.

        Parameters:
        - db_connector: An instance of the database connector.
        - table_name: The name of the table to read from.

        Returns:
        - df: A pandas DataFrame containing the data from the specified table.
        """
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
    
    def retrieve_pdf_data(self, link):
        """
        Retrieve data from a PDF file.

        Parameters:
        - link: The URL or path to the PDF file.

        Returns:
        - df_pdf: A pandas DataFrame containing the extracted data from the PDF.
        """
        pdf_data = tb.read_pdf(link,pages = 'all')
        df_pdf = pd.concat(pdf_data)
        return df_pdf
    
    def list_number_of_stores(self, endpoint, headers):
        """
        Get the number of stores from an API endpoint.

        Parameters:
        - endpoint: The URL of the API endpoint.
        - headers: A dictionary of headers to be included in the request.

        Returns:
        - result: The number of stores retrieved from the API.
        """
        response = requests.get(endpoint, headers=headers)
        result = response.json().get('number_stores')
        return result
    
    def retrieve_stores_data(self, retrieve_store_endpoint, headers, num_stores):
        """
        Retrieve data for multiple stores from an API endpoint.

        Parameters:
        - retrieve_store_endpoint: The base URL of the API endpoint for retrieving store data.
        - headers: A dictionary of headers to be included in the request.
        - num_stores: The number of stores to retrieve data for.

        Returns:
        - stores_df: A pandas DataFrame containing the data for all the stores.
        """
        stores_data = []

        for store_number in range(1, num_stores + 1):
            store_endpoint = f"{retrieve_store_endpoint}/{store_number}"

            try:
                response = requests.get(store_endpoint, headers=headers)

                if response.status_code == 200:
                    store_info = response.json()
                    stores_data.append(store_info)
                else:
                    print(f"Error: Unable to retrieve store {store_number}. Status code: {response.status_code}")

            except Exception as e:
                print(f"Error: {e}")

        stores_df = pd.DataFrame(stores_data)
        return stores_df
    
    def fetch_json_data(self, json_url):
        """
        Fetch JSON data from a URL.

        Parameters:
        - json_url: URL of the JSON data.

        Returns:
        dict: JSON data.
        """
        response = requests.get(json_url)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch JSON data.")
            return None
    
    def extract_from_s3(self, s3_address):
        """
        Extract data from a CSV file stored in an S3 bucket.

        Parameters:
        - s3_address: The address of the CSV file in S3 (e.g., 's3://bucket_name/path/to/file.csv').

        Returns:
        - products_data: A pandas DataFrame containing the extracted data from the CSV file.
        """
        s3_client = boto3.client('s3')
        products_data = None

        # Parse the S3 address
        bucket_name, key = s3_address.split('//')[1].split('/', 1)
        
        # Download the file from S3
        local_file_name = 'products.csv'
        s3_client.download_file(bucket_name, key, local_file_name)
        
        # Read the CSV into a DataFrame
        products_data = pd.read_csv(local_file_name)

        # Clean up: Close the local file
        products_data.to_csv(local_file_name, index=False)  # Save the cleaned DataFrame
        products_data = pd.read_csv(local_file_name)  # Re-read the cleaned DataFrame
        # Remove the local file
        os.remove(local_file_name)

        return products_data