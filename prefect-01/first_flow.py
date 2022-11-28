'''
    Load from API store as CSVs
'''

import requests
import os
import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

BACKEND_URL = os.getenv('BACKEND_URL')
API_KEY = os.getenv('API_KEY')

@task
def call_api(URL: str, API_KEY: str, query_params: dict):
    headers = {'Content-Type': 'application/json', 'Authorization': API_KEY}
    response = requests.get(url = URL, headers = headers, params = query_params)

    print(response.status_code)
    return response.json()

@task
def parse_json(response):
    customer_drs = response["drs"]
    return customer_drs

@task
def convert_to_df(data):
    return pd.DataFrame(data)

@task
def store_to_csv(df: pd.DataFrame, query_date):
    if not os.path.isdir("./csvs/"):
        os.makedirs("./csvs/")

    df.to_csv(f"./csvs/{query_date}.csv")

@flow
def api_flow(url, API_KEY, query_params):
    response_json = call_api(url, API_KEY, query_params)
    customer_drs = parse_json(response_json)
    df = convert_to_df(customer_drs)
    store_to_csv(df, query_params['searchByValue'])

URL = f"{BACKEND_URL}/drsmodule/deliveryreceipts"

query_params = {
    'searchBy': 'created_at',
    'searchByValue': '2022-01-04',
    'orderBy':'drno',
    'orderDir': 'asc'
}

api_flow(URL, API_KEY, query_params)