
import data_extraction
import datetime

class DataCleaning:
    def __init__(self) -> None:
        self.obj = data_extraction.DataExtractor()
    
    @staticmethod
    def convert_datetime(item):
        #print(item)
        
        try:
            return datetime.datetime.strptime(item, "%Y-%m-%d")
        except Exception as ex:
            print('Exception:', ex)
            try:
                return datetime.datetime.strptime(item, "%Y-%d-%m")
            except Exception as ex:
                print('Exception:', ex)

        print('not converted:', item)    
        return item

    def clean_user_data(self):
        
        df = self.obj.read_rds_table('legacy_users')

        # check for null values
        print(df['date_of_birth'].apply(self.convert_datetime))

        
obj = DataCleaning()
obj.clean_user_data()