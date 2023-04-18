from flask import Flask
from .flows import etl_web_to_gcs, etl_gcs_to_bq

def create_app():
    app = Flask(__name__)

    # etl_web_to_gcs()
    etl_gcs_to_bq('title.crew')

    return app