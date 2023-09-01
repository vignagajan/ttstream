import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'vignagajan',
    'start_date': datetime(2023, 9, 1, 10, 00)
}

# def stream_stocks(tickers):
import requests
import json
import random
import time

import requests


def get_yahoo_data(tickers):
    
    cookie = None

    user_agent_key = "User-Agent"
    user_agent_value = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"

    headers = {user_agent_key: user_agent_value}
    response = requests.get(
        "https://fc.yahoo.com", headers=headers, allow_redirects=True
    )

    if not response.cookies:
        raise Exception("Failed to obtain Yahoo auth cookie.")

    cookie = list(response.cookies)[0]

    response = requests.get(
        "https://query2.finance.yahoo.com/v8/finance/chart/"+tickers,
        headers=headers,
        cookies={cookie.name: cookie.value},
        allow_redirects=True,
    )
    data = response.json()

    if data is None:
        raise Exception("Failed to retrieve Yahoo data.")

    return data



    


