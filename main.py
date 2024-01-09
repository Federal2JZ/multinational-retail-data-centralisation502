from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


def dim_users():
    db_connector = DatabaseConnector()
    data_cleaning = DataCleaning()
    data_extractor = DataExtractor()

    # Read legacy_users data from the database
    legacy_users_table_name = "legacy_users"
    legacy_users_data = data_extractor.read_rds_table(db_connector, legacy_users_table_name)

    # Clean legacy_users data
    cleaned_legacy_users_data = data_cleaning.clean_user_data(legacy_users_data)

    # Upload cleaned legacy_users data to the database
    db_connector.upload_to_db(cleaned_legacy_users_data, table_name='dim_users')

if __name__ == "__main__":
    dim_users()