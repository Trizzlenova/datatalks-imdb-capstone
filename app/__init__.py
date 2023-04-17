from flask import Flask
from .flows import etl_web_to_gcs

def create_app():
    app = Flask(__name__)

    etl_web_to_gcs()

    return app