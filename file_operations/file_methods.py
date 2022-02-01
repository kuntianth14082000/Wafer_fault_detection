import pickle
import shutil
import os

class File_operations:
    """
            This class shall be used to save the model after training
            and load the saved model for prediction.

            Written By  : Kuntinath Noraje
            Version     : 1.0
            Revision    : None
    """
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory = 'models/'

    def save_model(self,model,filename):
        """
                Method Name : save_model
                Description : save the model to file directory
                Ouput       : File gets Saved
                On Failure  : Raise Exception

                Written By  : Kuntinath Noraje
                Version     : 1.0
                Revision    : None
        """
        self.logger_object.log(self.file_object,"Entered the save_model method of the file_operation class")
        try:
            path = os.path.join(self.model_directory,filename)  #create seperate directory for each cluster
            if os.path.isdir(path): # Remove previosly Existing model for each clusters
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path + '/' + filename + '.sav','wb') as f:
                pickle.dump(model,f)    # save the model to file
            self.logger_object.log(self.file_object,"Model file " + filename +'saved. Exited the save_model method of the model_finder class.')
            return 'success'


        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Model File ' + filename + ' could not be saved. Exited the save_model method of the Model_Finder class')
            raise Exception()