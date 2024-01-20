from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import requests
import json


def dim_users():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()

    # Read legacy_users data from the database
    legacy_users_table_name = "legacy_users"
    legacy_users_data = data_extractor.read_rds_table(db_connector, legacy_users_table_name)

    # Initialize DataCleaning instance with the legacy_users_data
    data_cleaning = DataCleaning()
    cleaned_legacy_users_data = data_cleaning.clean_user_data(legacy_users_data)

    # Upload cleaned legacy_users data to the database
    db_connector.upload_to_db(cleaned_legacy_users_data, table_name='dim_users')


def dim_card_details():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()

    # Extract data from a PDF file using retrieve_pdf_data method
    pdf_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    # Clean card details data
    data_cleaning = DataCleaning()
    cleaned_card_details_data = data_cleaning.clean_card_data(pdf_data)

    # Upload cleaned card details data to the database
    db_connector.upload_to_db(cleaned_card_details_data, 'dim_card_details')

def dim_stores():
    data_extractor = DataExtractor()
    database_connector = DatabaseConnector()
    data_cleaning = DataCleaning()

    # API details
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
    api_key = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    # List the number of stores
    num_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, api_key)

    if num_stores is not None:
        print(f"Number of stores: {num_stores}")

        # Retrieve store data
        stores_data = data_extractor.retrieve_stores_data(retrieve_store_endpoint, api_key, num_stores)

        # Clean store data
        cleaned_store_data = data_cleaning.clean_store_data(stores_data)

        # Upload cleaned store data to the database
        database_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

def dim_products():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    # Extract data from S3
    products_data = data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
    products_data = data_cleaning.convert_product_weights(products_data)

    # Data Cleaning - Convert Product Weights
    products_data = data_cleaning.convert_product_weights(products_data)

    # Data Cleaning - Clean Products Data
    products_data = data_cleaning.clean_products_data(products_data)

    # Upload cleaned products data to the database
    db_connector.upload_to_db(products_data, 'dim_products')

    
def dim_orders():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()

    # List all tables in the database
    tables = db_connector.list_db_tables()
    print("Tables in the database:", tables)

    # Extract orders data
    orders_table_name = 'orders_table'
    orders_data = data_extractor.read_rds_table(db_connector, orders_table_name)

    # Initialize DataCleaning instance with orders_data
    data_cleaning = DataCleaning(orders_data)

    # Clean orders data
    cleaned_orders_data = data_cleaning.clean_orders_data(orders_data)

    # Upload cleaned orders data to the database
    target_table_name = 'orders_table'
    db_connector.upload_to_db(cleaned_orders_data, target_table_name)

def dim_date_times():
    db_connector = DatabaseConnector()
    data_cleaning = DataCleaning()

    # Fetch data from the JSON file using requests
    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    response = requests.get(json_url)
    date_details_data = json.loads(response.text)

    # Clean date details data
    cleaned_date_details_data = data_cleaning.clean_date_times_data(date_details_data)

    # Upload cleaned date details data to the database
    db_connector.upload_to_db(cleaned_date_details_data, 'dim_date_times')

if __name__ == "__main__":
    dim_stores()
    dim_card_details()
    dim_users()
    dim_products()
    dim_orders()
    dim_date_times()