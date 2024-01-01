import psycopg2
import yaml
from sqlalchemy import create_engine


class DatabaseConnector:
    def __init__(self, dbname, user, password, host):
        # Constructor to initialize database connection parameters
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.connection = None
        self.cursor = None

    def connect(self):
        # Method to establish a connection to the PostgreSQL database
        self.connection = psycopg2.connect(
            dbname=self.dbname, user=self.user, password=self.password, host=self.host
        )
        self.cursor = self.connection.cursor()

    def close_connection(self):
        # Method to close the database connection
        if self.connection:
            self.connection.close()
        if self.cursor:
            self.cursor.close()

    def execute_query(self, query):
        # Method to execute a query on the database
        pass

    def read_db_creds(self, file_path="db_creds.yaml"):
        # Method to read database credentials from a YAML file
        with open(file_path, "r") as file:
            return yaml.safe_load(file)
        
    def init_db_engine(self):
        # Method to initialize and return a SQLAlchemy database engine
        creds = self.read_db_creds()
        return create_engine(
            f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        )
    
    def list_db_tables(self):
        pass

    def upload_to_db(self, dataframe, table_name):
        # Method to upload a Pandas DataFrame to a database table
        dataframe.to_sql(table_name, con=self.init_db_engine(), if_exists='replace', index=False)



if __name__ == "__main__":
    pass