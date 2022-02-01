"""
    This is the Entry for Training the Machine Learnig Model.

    Written By  : Kuntinath Noraje.
    Version     : 1.0
    Revision    : None
"""

# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from application_logging import logger
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods

class trainModel:

    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt","a+")

    def trainingModel(self):
        # Logging the start of training
        self.log_writer.log(self.file_object,"Start of training!!")
        try:
            # Getting the data from source
            data_getter = data_loader.Data_getter(self.file_object,self.log_writer)
            data = data_getter.get_data()

            """Doing the data preprocessing"""

            preprocessor = preprocessing.Preprocessor(self.file_object,self.log_writer)
            data = preprocessor.remove_columns(data,['Wafer']) # remove the unnamed column as it doesn't contribute to prediction.

            # create seperate features and labels
            X,Y = preprocessor.separate_label_feature(data,label_column_name='Output')

            # Check if missing values are present in the dataset
            is_null_present = preprocessor.is_null_present(X)


            # if missing values are there,replace them appropriately.
            if(is_null_present):
                X = preprocessor.impute_missing_values(X) # missing values imputetion

            """ Applying the clustering approach """

            kmeans = clustering.KMeansClustering(self.file_object,self.log_writer)
            number_of_clusters = kmeans.elbow_plot(X)  #  using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            X = kmeans.create_clusters(X,number_of_clusters)

            # create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels'] = Y

            # Getting the unique characters from the dataset
            list_of_clusters = X['Cluster'].unique()

            """Parsing all the clusters and Looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data =X[X['Cluster']==i]    # Filter the data for one cluster

                # Prepare the feature and Label column
                cluster_features = cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label = cluster_data['Labels']

                # spliting the data into training and test set for each cluster one by one
                x_train,x_test,y_train,y_test = train_test_split(cluster_features,cluster_label, test_size= 1/3, random_state=355)

                model_finder =tuner.Model_Finder(self.file_object,self.log_writer) # Object initilization.

                # getting the best model for each of the clusters.
                best_model_name,best_model = model_finder.get_best_model(x_train,y_train,x_test,y_test)

                # saving the best model to the directory
                file_op = file_methods.File_operations(self.file_object,self.log_writer)
                save_model = file_op.save_model(best_model,best_model_name+str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception

