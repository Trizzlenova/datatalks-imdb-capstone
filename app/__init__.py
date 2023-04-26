from flask import Flask
from .flows import etl_web_to_bq

def run_etl_cycle():
    print('ETL Starting...')
    etl_web_to_bq()
    print('ETL Complete!')