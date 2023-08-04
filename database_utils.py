import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:

    def __init__(self) -> None:
        self.__HOST = None
        self.__PASSWORD = None
        self.__USER = None
        self.__DATABASE = None
        self.__PORT = None

    def read_db_creds(self, yamlconfig):
        
        with open(f'{yamlconfig}.yaml', 'r',) as f :
            output = yaml.safe_load(f)
        
        return output
    
    def init_db_engine(self, filename:str):
        output = self.read_db_creds(filename)

        try:
            self.__HOST = output['RDS_HOST']
            self.__PASSWORD = output['RDS_PASSWORD']
            self.__USER = output['RDS_USER']
            self.__DATABASE = output['RDS_DATABASE']
            self.__PORT = output['RDS_PORT']

            print('Data successfully read!')
        except:
            pass

        conn = create_engine(f'postgresql+pg8000://{self.__USER}:{self.__PASSWORD}@{self.__HOST}:{self.__PORT}/{self.__DATABASE}')
        
        return conn
    
    def list_db_tables(self):
        '''
        list tables within the database
        '''
        connector = self.init_db_engine('config/db_creds')
        
        try:
            with connector.connect() as conn:
                inspector = inspect(conn)
                schemas = inspector.get_schema_names()
                for schema in schemas:
                    for table_name in inspector.get_table_names(schema=schema):
                        print(f'Table name: {table_name}')
        except:
            print('Error! Cannot connect to database')

    def upload_to_db(self, dataframe, tablename:str):
        '''
        upload into the local database. Raise error if 'dataframe' object is not a pandas dataframe
        '''
        local_conn = self.init_db_engine('config/mysql_creds')

        if isinstance(dataframe, pd.DataFrame):
            with local_conn.connect() as con:
                dataframe.to_sql('dim_users', con)
        else:
            raise TypeError


        
