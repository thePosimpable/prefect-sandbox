import pandas as pd
import requests
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('../.env')
load_dotenv(dotenv_path = dotenv_path)

BACKEND_URL = os.getenv('BACKEND_URL')
API_KEY = os.getenv('API_KEY')


URL = f"{BACKEND_URL}/drsmodule/deliveryreceipts"
headers = {'Content-Type': 'application/json', 'Authorization': API_KEY}

PARAMS = {
    'searchBy': 'created_at',
    'searchByValue': '2022-01-03',
    'orderBy':'drno',
    'orderDir': 'asc'
}

r = requests.get(url = URL, headers = headers, params = PARAMS)
data = r.json()

df = pd.DataFrame(data['drs'])
print(df)