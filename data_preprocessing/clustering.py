import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods


class KMeansClustering:
    """
        This class shall be used to divide data into clusters before training.

        Written By  : Kuntinath Noraje
        Version     : 1.0
        Revision    : None
    """

    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def elbow_plot(self,data):
        """
                            Method Name : elbow_plot
                            Description : This method saves the plot to decide the optimum number of clusters to the file.
                            Output      :   A picture saved to the directory.
                            On failure  : Raise Exception.

                            Written By  : Kuntinath Noraje
                            Version     : 1.0
                            Revision    : None
        """
        self.logger_object.log(self.file_object,"Entered the Elbow plot method. Of the clustering class")
        wcss = []   # initializing an empty list.
        try:
            for i in range(1,11):
                kmeans = KMeans(n_clusters = i, init = 'k-means++', random_state = 42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss)
            plt.title("The Elbow Method")
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            #plt.show()
            plt.savefig("preprocessing_data/k_means_Elbow.PNG")
            #finding the value of optimum cluster programmatically
            self.kn = KneeLocator(range(1,11),wcss,curve='convex',direction='decreasing')
            self.logger_object.log(self.file_object,"The optimum number of cluster is :"+str(self.kn.knee)+" Exited the elbow_plot method of the KMeansClustering class")
            return self.kn.knee

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in elbow_plot method of the KMeansClustering class. Exception message: '+ str(e))
            self.logger_object.log(self.file_object,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()

    def create_clusters(self,data,number_of_clusters):
        """
                Methos name : create_clusters
                Description : Create a new dataframe consisting of the cluster information.
                Output      : A dataframe with cluster column
                On failure  : Raise Exception

                Written By  : Kuntinath Noraje
                Version     : 1.0
                Revision    : None
        """
        self.logger_object.log(self.file_object,"Enter the create cluster method of the KMeansClustering Class")
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters,init='k-means++',random_state=42)
            self.y_kmeans =  self.kmeans.fit_predict(data) # Divide data into clusters
            self.file_op = file_methods.File_operations(self.file_object,self.logger_object)
            self.save_model = self.file_op.save_model(self.kmeans,'KMenas') # Saving the model to directory
                                                                                # Passing 'Model' as the functions need three parameters

            self.data['Cluster'] = self.y_kmeans # create a new cloumn in the dataset for storing the cluster information
            self.logger_object.log(self.file_object,"Successfully created" + str(self.kn.knee)+'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,"Exceptiom occured in create_clusters method of the KMeansClstering class. Exception message: " + str(e))
            self.logger_object.log(self.file_object,"Fitting the data to clusters failed. Exited the create_clusters method for the KMeansClustering class")
            raise Exception()