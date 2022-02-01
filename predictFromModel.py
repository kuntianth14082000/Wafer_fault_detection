import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation

class prediction:

    def __init__(self,path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer =logger.App_Logger()
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path)

    def predictionFromModel(self):
        try:
            self.pred_data_val.deletePredicitionFile()  # Delete the existingprediction from last run!!
            self.log_writer.log(self.file_object,"Start of prediction!!")
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data = data_getter.get_data()

            preprocessor = preprocessing.Preprocessor(self.file_object,self.log_writer)
            is_null_present = preprocessor.is_null_present(data)
            if(is_null_present):
                data = preprocessor.impute_missing_values(data)

            cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)
            data = preprocessor.remove_columns(data,cols_to_drop)

        except Exception as e:
            pass
