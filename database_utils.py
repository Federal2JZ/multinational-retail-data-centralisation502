import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine, inspect


class DatabaseConnector():
    """
    A class for connecting to and interacting with a PostgreSQL database.
    """

    def __init__(self):
        """
        Initialize the DatabaseConnector.
        """
        self.file_name = "db_creds.yaml"

    def read_db_creds(self):
        """
        Read database credentials from a YAML file.

        Returns:
        - data: A dictionary containing the database credentials.
        """
        with open(self.file_name) as f:
            data = yaml.load(f, Loader=SafeLoader)
            return data

    def init_db_engine(self):
        """
        Initialize a SQLAlchemy database engine using the credentials from the YAML file.

        Returns:
        - engine: A SQLAlchemy engine object connected to the specified PostgreSQL database.
        """
        data_2 = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{data_2['RDS_USER']}:{data_2['RDS_PASSWORD']}@{data_2['RDS_HOST']}:{data_2['RDS_PORT']}/{data_2['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        """
        List the tables in the connected PostgreSQL database.

        Returns:
        - table_names: A list of table names in the database.
        """
        inspector = inspect(self.init_db_engine())
        return inspector.get_table_names()

    def upload_to_db(self,dataframe,table_name):
        """
        Upload a pandas DataFrame to a specified table in the PostgreSQL database.

        Parameters:
        - dataframe: The pandas DataFrame to upload.
        - table_name: The name of the table to upload the DataFrame to.
        """
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'admin123'
        DATABASE = 'sales_data'
        PORT = 5432
        engine_2 = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        dataframe.to_sql(table_name,engine_2,if_exists = 'replace')