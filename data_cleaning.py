
import data_extraction

class DataCleaning:
    def __init__(self) -> None:
        self.obj = data_extraction.DataExtractor()

    def clean_user_data(self):
        
        df = self.obj.read_rds_table('legacy_users')

        # check for null values
        print(df.isnull().values.any())

        
obj = DataCleaning()
obj.clean_user_data()