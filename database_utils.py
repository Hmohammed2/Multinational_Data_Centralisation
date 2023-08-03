import yaml
from sqlalchemy import create_engine

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

        conn = create_engine(f'mysql+pymysql://{self.__USER}:{self.__PASSWORD}@{self.__HOST}:{self.__PORT}/{self.__DATABASE}')
        
        return conn

DatabaseConnector().init_db_engine()