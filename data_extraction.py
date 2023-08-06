import database_utils
import pandas as pd

class DataExtractor:
    def __init__(self) -> None:
        # create new instance of Databaseconnector class
        self.new_connector = database_utils.DatabaseConnector()

    def read_rds_table(self, tablename:str):
        with self.new_connector.init_db_engine('config/db_creds').connect() as conn:
            df = pd.read_sql(tablename, conn)
            print(df.head(5))
        return df

        