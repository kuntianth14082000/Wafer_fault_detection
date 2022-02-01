import sqlite3
from datetime import datetime
import shutil
from os import listdir
import os
import csv
from application_logging.logger import App_Logger


class dBOperation:
    """
        This class shall be used for all the SQL operations.

        Written By  : Kuntinath Noraje
        Version     : 1.0
        Revisions   : None
    """
    def __init__(self):
        self.path = 'Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()

    def dataBaseConnection(self,DatabaseName):
        """
        Method Name : dataBaseConnection
        Description : The methods createas the database with the given name and if database already exists then opens connection to the DB.
        Output      : Connection to the DB
        On Failure  : Raise ConnectionError

        Written By  : Kuntinath Noraje
        Version     : 1.0
        Revisions   : None
        """
        try:
            conn = sqlite3.connect(self.path+DatabaseName+'.db')
            file = open("Training_Logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"Opened %s database successfully" %DatabaseName)
            file.close()

        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt","a+")
            self.logger.log(file,"Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self,DatabaseName,column_names):
        """
            Method Name : createTableDb
            Description : This method creates a table in given database which will be used insert the Good data after raw data validation.
            Output      : None
            On Failure  : Raise Exception

            Written By  : Kuntinath Noraje
            Version     : 1.0
            Revisions   : None
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()

                file =open("Training_Logs/DbTableCreateLog.txt","a+")
                self.logger.log(file,"Table created successufully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", "a+")
                self.logger.log(file,"Closed %s database successfully" %DatabaseName)
                file.close()

            else:
                for key in column_names.keys():
                    type =column_names[key]

                    #in try block we will check if table exist, if yes then add columns to the table
                    # else incatch block we will create the table

                    try:
                        conn.execute("ALTER TABLE Good_Raw_Data ADD COLUMN '{column_name}'{data_type}".format(column_name=key,data_type=type))
                    except:
                        conn.execute("CREATE TABLE Good_Raw_Data ({column_name} {data_type})".format(column_name=key,data_type=type))

                conn.close()

                file = open("Training_Logs/DbTableCreateLog.txt","a+")
                self.logger.log(file,"Table Created Successfully!!")
                file.close()
                file = open("Training_Logs/DataBaseConnectionLog.txt","a+")
                self.logger.log(file,"Closed %s database successfully" %DatabaseName)
                file.close()


        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self,Database):
        """
                                Method Name : insertIntoTableGoodData
                                Description : This method inserts the Good data files from the Good_Raw folder into
                                              the above created folder.
                                Output      : None
                                On Failure  : Raise Exception

                                Written By  : Kuntinath Noraje
                                Vesrion     : 1.0
                                Revision    : None
        """
        conn = self.dataBaseConnection(Database)
        goodFilepath = self.goodFilePath
        badFilePath  = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilepath)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        for file in onlyfiles:
            try:
                with open(goodFilepath + '/' + file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in(line[1]):
                            try:
                                print(6)
                                conn.execute("INSERT INTO Good_Raw_Data values ({values})".format(values=(list_)))
                                self.logger.log(log_file,"%s : file loaded successfully!!" %file)
                                print(7)
                                conn.commit()
                            except Exception as e:
                                raise e
            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s" %e)
                shutil.move(goodFilepath + '/' + file, badFilePath)
                self.logger.log(log_file,"File moved successfully %s" %file)
                log_file.close()
                conn.close()
        conn.close()
        log_file.close()

    def selectingDatafromtableintocsv(self,Database):
        """
                                Method Name : selectingDatafromtableintocsv
                                Description : This method exports the data in GoodData Table as a CSV file. in a givenlocation.
                                               above created.
                                Output      : None
                                On Failure  : Raise Exception

                                Written By  : Kuntinath Noraje
                                Vesrsion    : 1.0
                                Revision    : None
        """
        self.fileFromDb = "Training_FileFromDB/"
        self.fileName   = "InputFile.csv"
        log_file = open("Training_Logs/ExportToCsv.txt","a+")
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT * FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()
            # Get the headers of the csv file
            headers =[i[0] for i in cursor.description]

            # make the csv output Directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open csv file for writing
            csvFile =csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',',lineterminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')

            # add the headers and data to the csv file
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file,"File Exported Successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file,"File Exporting Failed. Error : %s" %e)
            log_file.close()