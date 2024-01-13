from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


def dim_users():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()

    # Read legacy_users data from the database
    legacy_users_table_name = "legacy_users"
    legacy_users_data = data_extractor.read_rds_table(db_connector, legacy_users_table_name)

    # Initialize DataCleaning instance with the legacy_users_data
    data_cleaning = DataCleaning(legacy_users_data)

    # Clean legacy_users data by calling the clean_user_data method
    cleaned_legacy_users_data = data_cleaning.clean_user_data()

    # Upload cleaned legacy_users data to the database
    db_connector.upload_to_db(cleaned_legacy_users_data, table_name='dim_users')


def dim_card_details():
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()

    # Extract data from a PDF file using retrieve_pdf_data method
    pdf_data = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    # Clean card details data
    data_cleaning = DataCleaning(pdf_data)
    cleaned_card_details_data = data_cleaning.clean_card_data()

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

if __name__ == "__main__":
    dim_stores()
    dim_card_details()
    dim_users()