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

if __name__ == "__main__":
    dim_card_details()
    dim_users()