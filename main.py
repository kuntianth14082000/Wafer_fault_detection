from wsgiref import simple_server
from flask import Flask, request, render_template
from flask import Response
import os
import json
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
from training_Validation_Insertion import train_validation
from trainingModel import trainModel
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['GET','POST'])
@cross_origin()
def predictRouteClient():
    try:
        folder_path = "Prediction_Batch_files"
        if folder_path is not None:
            path = folder_path
            pred_val = pred_validation(path) #object initialization
            pred_val.prediction_validation() #calling the prediction_validation function
            pred = prediction(path) #object initialization

            return path
    except Exception as e:
        print(e)


@app.route("/train", methods=['GET','POST'])        # change line 126 in index.html to /train
@cross_origin()
def trainRouteClient():
    try:
        #if request.json['folderPath'] is not None:
            #path = request.json['folderPath']
            #return path
        folder_path = "Training_Batch_Files"
        if folder_path is not None:
            path = folder_path

            # train_valObj = train_validation(path) # object initialization
            # train_valObj.train_validation() # calling the training_validation function

            # trainModelObj = trainModel() # object initialization
            # trainModelObj.trainingModel() # training the model for the files in the table

    except ValueError:
        return Response("Error Occurred! %s" % ValueError)

    except KeyError:
        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:
        return Response("Error Occurred! %s" % e)

    return Response("Training successfull!!")

port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    host = '0.0.0.0'
    #port = 5000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
