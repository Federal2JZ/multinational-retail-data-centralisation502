import pandas as pd
import tabula as tb
import requests
import boto3
import os


class DataExtractor():
    def __init__(self, database_connector_instance=None, table_name=None):
        self.database_connector_instance = database_connector_instance
        self.table_name = table_name

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
    
    def retrieve_pdf_data(self, link):
        pdf_data = tb.read_pdf(link,pages = 'all')
        df_pdf = pd.concat(pdf_data)
        return df_pdf
    
    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        result = response.json().get('number_stores')
        return result
    
    def retrieve_stores_data(self, retrieve_store_endpoint, headers, num_stores):
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
    
    def extract_from_s3(self, s3_address):
        s3_client = boto3.client('s3')
        products_data = None

        # Parse the S3 address
        bucket_name, key = s3_address.split('//')[1].split('/', 1)
        
        # Download the file from S3
        local_file_name = 'products.csv'
        s3_client.download_file(bucket_name, key, local_file_name)
        
        # Read the CSV into a DataFrame
        products_data = pd.read_csv(local_file_name)

            # # Print the shape of the DataFrame
            # print("Products Data Shape:", products_data.shape)

            # # Print information about the DataFrame after cleaning
            # print("Products Data Information:")
            # print(products_data.info())

        # Clean up: Close the local file
        products_data.to_csv(local_file_name, index=False)  # Save the cleaned DataFrame
        products_data = pd.read_csv(local_file_name)  # Re-read the cleaned DataFrame
        # Remove the local file
        os.remove(local_file_name)

        return products_data