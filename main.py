from wsgiref import simple_server
from flask import Flask, Request, render_template, request
from TransformandValidatate.Training_Validation_Insertion import TrainValidateInsert as TrainValInsert
from flask import Response
import os
import json

from flask_cors import CORS, cross_origin

app = Flask(__name__)

CORS(app)


@app.route('/', methods=['POST','GET'])
@cross_origin()
def home():
    return render_template('home.html')


@app.route('/train', methods=['POST'])
@cross_origin()
def train():
    if request.method == 'POST':
        path = request.form.get('train')
        print("path %s"%str(path))
        train_validate_insert_obj=TrainValInsert(path ,'schema_training.json')
        train_validate_insert_obj.validate_transform_insert()

    return render_template('home.html',validate_transform_data="Validation !!!  transformation !!!  insertion!!!")





port=int(os.getenv("PORT",5000))
# print(port)
if __name__ == '__main__':
    host = '127.0.0.1'
    #port = 8000
    https = simple_server.make_server(host, port, app)
    #app.debug = True
    #app.run(debug=True)
    print("serving on {}:{}".format(host, port))
    https.serve_forever()
