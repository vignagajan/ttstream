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
import time
import random

def get_yahoo_data(tickers,period="1d",interval="1m"):
    
    import yfinance as yf
    # create ticker for Apple Stock
    ticker = yf.Ticker(tickers)
    # get data of the most recent date
    data = ticker.history(period=period,interval=interval)

    return data.to_json()

print(get_yahoo_data("AAPL"))

    


