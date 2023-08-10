
import data_extraction
import datetime
import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self) -> None:
        self.obj = data_extraction.DataExtractor()
    
    @staticmethod
    def convert_datetime(item):
        #print(item)
        try:
            converted_date = pd.to_datetime(item, errors='coerce')
            return converted_date
        except Exception as ex:
            print('Exception:', ex)
        return item
    
    def clean_user_data(self):

        '''
        Method for cleaning user data. Remove missing values, standardise date format and wrong information applied
        '''
        
        df = self.obj.read_rds_table('legacy_users')

        # standardize date values
        df['date_of_birth'] = df['date_of_birth'].apply(self.convert_datetime)
        df['join_date'] = df['join_date'].apply(self.convert_datetime)

        # Replace 'NULL' string with np.nan. 

        df = df.replace('NULL', np.nan)

        # Check for missing values after having applied function

        print(df.isna().sum())

        # drop rows with nan values. This will also affect the incorrect string values as the date values are blank in those columns.

        df.dropna(inplace=True)

        #  Verify if there are still missing values

        print(df.isna().sum())

        
        
obj = DataCleaning()
obj.clean_user_data()