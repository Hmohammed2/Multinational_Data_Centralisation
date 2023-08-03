import database_utils
import pandas as pd
from sqlalchemy.orm import Session

class DataExtractor:
    def __init__(self) -> None:
        # create new instance of Databaseconnector class
        self.new_connector = database_utils.DatabaseConnector()

    def read_rds_table(self, tablename:str):
        with self.new_connector.init_db_engine().connect() as conn:
            df = pd.read_sql(tablename, conn)
            print(df)
        return df

    def list_tables(self):
        self.new_connector.list_db_tables()
        


inst = DataExtractor()
inst.list_tables()
inst.read_rds_table('legacy_users')