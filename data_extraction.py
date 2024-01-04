import pandas as pd


class DataExtractor:
    def __init__(self):
        pass
    
    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql_query(query, engine)
        return df