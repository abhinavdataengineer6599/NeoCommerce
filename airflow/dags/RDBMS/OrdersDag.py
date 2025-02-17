from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.functions import *
import json
import os
from datetime import datetime

load_dotenv()


# Define a simple Python function
def ingest():
    print("Hello, Airfow!")

    conn=connect_db('OrderManagementSystem')
    CONFIG_FILE =  os.getenv('CONFIG_FILE')
    
    PROJECTID='original-future-449709-n8'
    DATASETID='NeoCommerceOms'
    TABLENAME='orders'

    with open(CONFIG_FILE, "r") as f:
        config=json.load(f)

    orders_ts=config['tables']['orders']['last_load_timestamp']
    print('last_load_timestamp -',config['tables']['orders']['last_load_timestamp'])
    

    if orders_ts !='':
        orders_qry=f""" SELECT * FROM orders WHERE order_id is not null and customer_id is not null and cast(order_date as date) > cast({orders_ts} as date) """
    else:
        orders_qry=f""" SELECT * FROM orders WHERE order_id is not null and customer_id is not null """

    order_items_qry=f""" SELECT * FROM order_items WHERE order_id is not null and product_id is not null """

    if conn is not None:
        orders_df = pd.read_sql_query(orders_qry, conn)
        order_items_df = pd.read_sql_query(order_items_qry, conn)
        
        # Validate the date format (Correct format: YYYY-MM-DD HH:MM:SS)
        orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Filter out invalid records (keep only valid dates)
        orders_df = orders_df.dropna(subset=['order_date'])

        orders_df['order_date'] = orders_df['order_date'].dt.tz_localize(None)
        orders_df['status'] = orders_df['status'].str.upper()

        df=order_items_df.copy()
        # Calculate the total amount by summing quantity * price for each order_id in order_items
        df['calculated_total'] = order_items_df['quantity'] * order_items_df['price']

        # Group by order_id and sum the calculated totals
        calculated_totals_df = df.groupby('order_id')['calculated_total'].sum().reset_index()

        # print(calculated_totals.head())

        # print(orders_df.head())

        # print(order_items_df.head())
        # Merge the orders table with the calculated totals
        df_merged = pd.merge(orders_df, calculated_totals_df, on='order_id')

        # Compare total_amount with the calculated total
        df_merged['is_total_correct'] = df_merged['total_amount'] == df_merged['calculated_total']

        # commented bcs of low data quality
        # filtered_df = df_merged[df_merged['is_total_correct'] == 'True']

        final_df = pd.merge(df_merged, order_items_df, on='order_id', how='inner')
        final_df['part_dt'] = final_df['order_date'].dt.date 

        final_df['part_dt']=pd.to_datetime('today').strftime('%Y-%m-%d')

        config['tables']['orders']['last_load_timestamp']=final_df['order_date'].max().strftime('%Y-%m-%d')

        try:
            load_to_bigquery(final_df, project_id=PROJECTID, dataset_id=DATASETID, table_name=TABLENAME, partition_col='part_dt', partition_type='DAY')
        except Exception as ex:
            print("TABLE LOAD FAILED ",ex)
            raise ex

        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        
        conn.close()

        print('Connection Closed...')
        # print(final_df.head()) 
        # return final_df
    else:
        raise Exception('conn is None')


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
    dag_id="Orders",
    default_args=default_args,
    schedule=None,
    description="Ingest tables from RDMBS",
    catchup=False,
    tags=["abhinav"],
) as dag:

    # Define the task
    RDBMS_Ingestion = PythonOperator(
        task_id="Orders-Ingestion",
        python_callable=ingest,
    )

    # Set task dependencies (only one task in this case)
    RDBMS_Ingestion

