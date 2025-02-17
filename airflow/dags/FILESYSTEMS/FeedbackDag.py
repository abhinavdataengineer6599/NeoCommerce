from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.functions import *
import json
import os

import pandas as pd

import requests
load_dotenv()

from google.cloud import storage
import io
import re

def get_filenames(last_load_date):
    """Generate a list of filenames based on the last load date."""
    today = datetime.today()
    if last_load_date is None or (today - last_load_date).days > 7:
        start_date = today - timedelta(days=7)
    else:
        start_date = last_load_date + timedelta(days=1)
    
    filenames = [f"feedback_{date.year}_{date.month:02d}_{date.day:02d}.csv" 
                 for date in pd.date_range(start=start_date, end=today)]
    return filenames

def read_csv_from_gcs(bucket_name, last_load_date, folder_path=""):  
    """Read CSV files from Google Cloud Storage into a pandas DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    filenames = get_filenames(last_load_date)
    print(filenames)
    dfs = []
    for filename in filenames:

        blob_path = f"{folder_path}/{filename}" if folder_path else filename
        print('filename ' ,filename)
        print('blob_path ' ,blob_path)
        blob = bucket.blob(blob_path)
        
        if blob.exists():
            print('blob exists..')
            data = blob.download_as_text()

            df = pd.read_csv(io.StringIO(data))
            df['filename']=filename
            
            
            dfs.append(df)
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        raise FileNotFoundError("No relevant files found in the given date range.")


def clean_text(text):
    """Remove special characters and emojis from text."""
    return re.sub(r'[^\w\s,.]', '', text) if isinstance(text, str) else "No feedback provided"



# Define a simple Python function
def ingest():
    print('Hello Airflow..')

    bucket_name = "neocommerce"  # Change this to your GCS bucket
    folder_path = "feedbacks"  # Change this if files are inside a folder
    
    CONFIG_FILE =  os.getenv('CONFIG_FILE')
    
    PROJECTID='original-future-449709-n8'
    DATASETID='NeoCommerceOms'
    TABLENAME='feedback'

    with open(CONFIG_FILE, "r") as f:
        config=json.load(f)

    last_load_date=config['tables']['feedback']['last_load_timestamp']
    if last_load_date !='':
        last_load_date=datetime.strptime(last_load_date, "%Y-%m-%d")
    else:
        last_load_date=None
    
    print('last_load_timestamp -', last_load_date)

    df = read_csv_from_gcs(bucket_name, last_load_date, folder_path)


    # Data Quality Checks
    df.drop_duplicates(subset=["order_id"], keep="first", inplace=True)
    df["feedback_text"] = df["feedback_text"].apply(clean_text)
    df["rating"] = df["rating"].apply(lambda x: x if 1 <= x <= 5 else None)

    # Transformations
    df["feedback_date"] = pd.to_datetime(df["feedback_date"], errors="coerce").dt.strftime("%Y-%m-%d")


    df['part_dt'] = pd.to_datetime('today').strftime('%Y-%m-%d')
    # print(df.head())

    config['tables']['feedback']['last_load_timestamp']=pd.to_datetime('today').strftime('%Y-%m-%d')

    try:
        load_to_bigquery(df,project_id=PROJECTID, dataset_id=DATASETID, table_name=TABLENAME, partition_col='part_dt', partition_type='DAY')
    except Exception as ex:
        print("TABLE LOAD FAILED ",ex)
        raise ex

    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)


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
    dag_id="Feedback",
    default_args=default_args,
    schedule=None,
    description="Ingest tables from Files",
    catchup=False,
    tags=["abhinav"],
) as dag:

    # Define the task
    Feedback_Ingestion = PythonOperator(
        task_id="Feedback-Ingestion",
        python_callable=ingest,
    )

    # Set task dependencies (only one task in this case)
    Feedback_Ingestion