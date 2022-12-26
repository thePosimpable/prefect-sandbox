import requests
import os
from prefect import flow, task, get_run_logger
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path('../.env')
load_dotenv(dotenv_path = dotenv_path)

API_KEY = os.getenv('API_KEY')
BACKEND_HOST = os.getenv('BACKEND_URL')

@task
def call_api(date: str):
    URL = f"{BACKEND_HOST}/drsmodule/deliveryreceipts"

    base_query_params = {
        'searchBy': 'created_at',
        'orderBy':'drno',
        'orderDir': 'asc',
        'searchByValue': date
    }

    headers = {'Content-Type': 'application/json', 'Authorization': API_KEY}
    response = requests.get(url = URL, headers = headers, params = base_query_params)
    return response

@task
def test_func(res):
    print(res.url)
    print(res.json()['querycount'])

@flow
def api_flow():
    logger = get_run_logger()

    # dates = ['2022-01-03', '2022-01-04', '2022-01-05', '2022-01-06']
    dates = ['2022-01-04', '2022-01-05', '2022-01-06']

    call_api_task = [call_api('2022-01-03'), *call_api.map(dates)]
    
    test_func.map(call_api_task)

api_flow()