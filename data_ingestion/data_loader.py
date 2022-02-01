import pandas as pd

class Data_getter:

    def __init__(self, file_object, logger_object):
        self.training_file = "Training_FileFromDB/InputFile.csv"
        self.file_object = file_object
        self.logger_object  = logger_object

    def get_data(self):
        """
            Method Name : get_data
            Description : This method reads the data from source
            Output      : A Pandas DataFrame.
            On failure  : raise Exception

            Written By  : Kuntinath Noraje
            Version     : 1.0
            Reversion   : None

        """
        self.logger_object.log(self.file_object,"Entered the get data method of the Data_Getter class")
        try:
            self.data = pd.read_csv(self.training_file) # reading the data file
            self.logger_object.log(self.file_object,"Data load successful. Exit the get data method of the Data_getter class")
            return self.data

        except Exception as e:
            self.logger_object.log(self.logger_object,
                                   "Exception occured in the get data method of the data gettear class. Exception message : %s" +str(e))
            self.logger_object.log(self.file_object,
                                   "Data load unsuccessful. Exit the get_data Method of the data_getter class")
            raise Exception()