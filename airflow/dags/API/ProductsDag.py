from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.functions import *
import json
import os
from datetime import datetime

import pandas as pd

import requests
load_dotenv()


# Define a simple Python function
def ingest():
    url = "http://127.0.0.1:8000/get-json"

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)
    data=response.text

    # Convert JSON to Pandas DataFrame
    datadf = pd.read_json(data)

    datadf.head()


    # Simulate order_items table (to check product_id existence)
    # order_items = pd.DataFrame({"product_id": [101, 102, 103]})

    # Check if product_id exists in order_items
    # datadf["valid_product"] = datadf["product_id"].isin(order_items["product_id"])
    datadf["valid_product"]=True
    # Ensure price is numeric and non-negative
    datadf["valid_price"] = datadf["price"].apply(lambda x: isinstance(x, (int, float)) and x >= 0)

    # Filter out invalid rows
    datadf_cleaned = datadf[(datadf["valid_product"]) & (datadf["valid_price"])]
    datadf_cleaned['part_dt']=pd.to_datetime('today').strftime('%Y-%m-%d')

    # print(datadf_cleaned.head())

    load_to_bigquery(datadf_cleaned,project_id='original-future-449709-n8', dataset_id='NeoCommerceOms', table_name='product', partition_col='part_dt', partition_type='DAY')


# Define default arguments for the DAG
default_args = {
    "owner": "abhinav",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "schedule_interval":None,
    'email': ['abhhinavdataengineer6599@gmail.com'],
    "email_on_failure": False
}

# Define the DAG
with DAG(
    dag_id="Products",
    default_args=default_args,
    schedule=None,
    description="Ingest tables from RDMBS",
    catchup=False,
    tags=["abhinav"],
) as dag:

    # Define the task
    Products_Ingestion = PythonOperator(
        task_id="Products-Ingestion",
        python_callable=ingest,
    )

    # Set task dependencies (only one task in this case)
    Products_Ingestion