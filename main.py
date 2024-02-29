from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import requests
import json


def initialize_components():
    """
    Initialize the database connector, data extractor, and data cleaner.

    Returns:
    - db_connector: An instance of the DatabaseConnector.
    - data_extractor: An instance of the DataExtractor.
    - data_cleaning: An instance of the DataCleaning.
    """
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()
    return db_connector, data_extractor, data_cleaning

def dim_users():
    """
    Extracts, cleans, and uploads user data to the database.

    Reads legacy user data from the database, cleans it to remove inconsistencies,
    and uploads the cleaned user data to the 'dim_users' table in the database.
    """
    # Read legacy_users data from the database
    legacy_users_table_name = "legacy_users"
    legacy_users_data = data_extractor.read_rds_table(db_connector, legacy_users_table_name)

    # Initialize DataCleaning instance with the legacy_users_data
    data_cleaning = DataCleaning()
    cleaned_legacy_users_data = data_cleaning.clean_user_data(legacy_users_data)

    # Upload cleaned legacy_users data to the database
    db_connector.upload_to_db(cleaned_legacy_users_data, table_name='dim_users')


def dim_card_details():
    """
    Extracts, cleans, and uploads card details data to the database.

    Extracts card details data from a PDF file, cleans it to remove inconsistencies,
    and uploads the cleaned card details data to the 'dim_card_details' table in the database.
    """
    # Extract data from a PDF file using retrieve_pdf_data method
    pdf_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    # Clean card details data
    data_cleaning = DataCleaning()
    cleaned_card_details_data = data_cleaning.clean_card_data(pdf_data)

    # Upload cleaned card details data to the database
    db_connector.upload_to_db(cleaned_card_details_data, 'dim_card_details')

def dim_stores():
    """
    Extracts, cleans, and uploads store data to the database.

    Retrieves store data from an API, cleans it to remove inconsistencies,
    and uploads the cleaned store data to the 'dim_store_details' table in the database.
    """
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
        db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

def dim_products():
    """
    Extracts, cleans, and uploads product data to the database.

    Extracts product data from an S3 bucket, cleans it to remove inconsistencies,
    and uploads the cleaned product data to the 'dim_products' table in the database.
    """
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
    """
    Extracts, cleans, and uploads order data to the database.

    Reads order data from the database, cleans it to remove inconsistencies,
    and uploads the cleaned order data to the 'orders_table' table in the database.
    """
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
    """
    Extracts, cleans, and uploads date time data to the database.

    Fetches date time data from a JSON file, cleans it to remove inconsistencies,
    and uploads the cleaned date time data to the 'dim_date_times' table in the database.
    """
    # Fetch data from the JSON file using requests
    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    response = requests.get(json_url)
    date_details_data = json.loads(response.text)

    # Clean date details data
    cleaned_date_details_data = data_cleaning.clean_date_times_data(date_details_data)

    # Upload cleaned date details data to the database
    db_connector.upload_to_db(cleaned_date_details_data, 'dim_date_times')

if __name__ == "__main__":
    db_connector, data_extractor, data_cleaning = initialize_components()
    dim_stores()
    dim_card_details()
    dim_users()
    dim_products()
    dim_orders()
    dim_date_times()