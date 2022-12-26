import requests
from prefect import flow, task
from datetime import date, timedelta

@task
def call_api(URL: str, API_KEY: str, query_params: dict):
    headers = {'Content-Type': 'application/json', 'Authorization': API_KEY}
    response = requests.get(url = URL, headers = headers, params = query_params)
    return response.json()

@task
def parse_fact(response):
    fact = response["fact"]
    print(fact)
    return fact

@flow
def api_flow(url, API_KEY, query_params):
    query_dates = pd.date_range(date(2018, 1, 1), date(2022, 11, 28) - timedelta(days=1), freq='d').strftime('%Y-%m-%d')

    for query_date in query_dates:
        params = {**query_params} 
        params['searchByValue'] = query_date
        response_json = call_api(url, API_KEY, params)
        customer_drs = parse_json(response_json)
        df = convert_to_df(customer_drs)
        store_to_csv(df, params['searchByValue'])



base_query_params = {
    'searchBy': 'created_at',
    'orderBy':'drno',
    'orderDir': 'asc'
}

api_flow(URL, API_KEY, base_query_params)
