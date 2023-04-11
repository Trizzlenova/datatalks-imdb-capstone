import os
import requests
from pyarrow import csv

response = requests.get('https://datasets.imdbws.com/title.crew.tsv.gz')