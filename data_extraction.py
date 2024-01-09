import pandas as pd
import tabula as tb
import requests


class DataExtractor():
    def __init__(self, database_connector_instance = None, table_name = None):
        self.database_connector_instance = database_connector_instance
        self.table_name = table_name

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df