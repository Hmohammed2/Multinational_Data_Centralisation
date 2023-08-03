import yaml
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import MetaData

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
    
    def init_db_engine(self):
        output = self.read_db_creds('config/db_creds')

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
        connector = self.init_db_engine()
        
        try:
            with connector.connect() as conn:
                inspector = inspect(conn)
                schemas = inspector.get_schema_names()
                for schema in schemas:
                    print("schema: %s" % schema)
                    for table_name in inspector.get_table_names(schema=schema):
                        for column in inspector.get_columns(table_name, schema=schema):
                            print("Column: %s" % column)
        except:
            print('Error!')

new_connector = DatabaseConnector()

new_connector.list_db_tables()