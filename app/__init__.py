from flask import Flask
from .flows.web_to_local import *


@flow(log_prints = True)
def create_app():
    app = Flask(__name__)

    web_to_local()

    return app