from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Step 2: Create instances
db_connector = DatabaseConnector("sales_data", "postgres", "admin123", "localhost")
data_extractor = DataExtractor()
data_cleaner = DataCleaning()

# Step 3: Connect to the database
db_connector.connect()

# Step 4: List tables in the database
tables = db_connector.list_db_tables()
print("Tables in the database:", tables)

# Step 5: Read user data from the RDS database
user_data = data_extractor.read_rds_table(db_connector, "dim_users")

# Step 6: Clean user data
cleaned_user_data = data_cleaner.clean_user_data(user_data)

# Step 7: Upload cleaned user data to the database
db_connector.upload_to_db(cleaned_user_data, "dim_users")

# Step 8: Close the database connection
db_connector.close_connection()