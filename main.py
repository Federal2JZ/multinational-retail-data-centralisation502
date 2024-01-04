from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import psycopg2


host = 'localhost'
port = '5432'
database = 'sales_data'
user = 'postgres'
password = 'admin123'

# Construct the connection string
connection_string = f"host={host} port={port} dbname={database} user={user} password={password}"

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(connection_string)

    # Create a cursor object for executing SQL queries
    cursor = connection.cursor()

    # Example: Execute a SQL query
    cursor.execute("SELECT * FROM dim_users;")
    
    # Fetch the result
    result = cursor.fetchall()
    print("Query Result:")
    for row in result:
        print(row)

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()