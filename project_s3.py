import os

from flask import Flask, render_template, request, send_from_directory, send_file, make_response
import config
from io import BytesIO
from utils.S3_utils import S3_Utils

app = Flask(__name__)
app.config.from_object(config)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.static_folder, 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
def index():
    s3_utils = S3_Utils()
    buckets = s3_utils.get_bucket_names()
    current_directory = os.getcwd()
    return render_template('index.html', buckets = buckets, current_directory = current_directory)

@app.route('/<bucket>')
def bucket(bucket):    
    s3_utils = S3_Utils()
    object_key = s3_utils.list_objects(bucket)
    current_directory = os.getcwd()
    key_dict = {}
    for key_name in object_key:        
        key_dict[key_name] = s3_utils.download_url(key_name)
    return render_template('objects.html', request=request ,buckets = object_key, current_directory = current_directory, url = key_dict)

@app.route('/<bucket>/<object_key>')
def object(bucket, object_key):
    s3_utils = S3_Utils()
    object_details = s3_utils.get_object_details(bucket, object_key)
    return render_template('object_details.html', response=object_details)

@app.route('/<bucket>/<object_key>', methods=['POST'])
def download(bucket, object_key):    
    s3_utils = S3_Utils()
    bucket_name = bucket
    object_key = object_key

    content, result = s3_utils.download_object(bucket_name, object_key)
    if result == False:
        return content

    response = make_response(content)
    response.headers.set('Content-Disposition', 'attachment', filename=object_key)
    return response
    
@app.route('/test')
def test():
    s3_1 = S3_Utils()
    s3_2 = S3_Utils()
    s3_3 = S3_Utils()

    return "ID 1 = " + str(id(s3_1)) + "<br>ID 2 = " + str(id(s3_2)) + "<br>ID 3 = " + str(id(s3_3))
if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
