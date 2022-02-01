from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTransform_Training.DataTransformation import dataTransform
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from application_logging import logger


class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.file_object =open("Training_Logs/Training_main_log.txt","a+")
        self.dataTransform = dataTransform()
        self.dBOperation = dBOperation()
        self.log_writer =logger.App_Logger()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, 'Start of validation on files!!')

            #Extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, Column_Names, noofcolumns = self.raw_data.valuesFromSchema()

            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)

            # Validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)

            # validate if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object,"Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object,"Starting Data Transformation!!")
            # Replace blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()
            self.log_writer.log(self.file_object,"DataTransform Complete!!")

            self.log_writer.log(self.file_object,"Creating Training Database and table on the basis of given schema!!")
            # create database with given name, if present open the connection! create table with columns given in schema
            self.dBOperation.createTableDb('Training',Column_Names)
            self.log_writer.log(self.file_object,"Table creation completed!!")
            self.log_writer.log(self.file_object,"Insertion of data into table started!!!!")

            #Insert csv file in the table
            self.dBOperation.insertIntoTableGoodData('Training')
            self.log_writer.log(self.file_object,"Insert in table completed!!")
            self.log_writer.log(self.file_object,"Deleting Good Data Folser!!")

            # Delete the good data folder after After loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object,"Good data Folder Deleted!!!")
            self.log_writer.log(self.file_object,"Moving Bad files to Archive and Deleting Bad data folder!!!")

            # Move the bad files to Archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object,"Bad files moved to archive!! Bad folder deleted!!")
            self.log_writer.log(self.file_object,"validation operation completed!!")
            self.log_writer.log(self.file_object,"Extracting csv files from table")
            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('Training')
            self.file_object.close()



        except Exception as e:
            raise e

