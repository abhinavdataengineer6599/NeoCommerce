import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

from google.cloud import bigquery

def infer_bq_schema(df):
    """
    Infer BigQuery schema from a pandas DataFrame.
    
    Args:
    - df (pd.DataFrame): DataFrame to infer schema from.
    
    Returns:
    - schema (list): List of BigQuery SchemaField objects.
    """
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'STRING',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    schema = []
    for col, dtype in df.dtypes.items():
        bq_type = type_mapping.get(str(dtype), 'STRING')  # Default to STRING if unknown
        schema.append(bigquery.SchemaField(col, bq_type))
    
    return schema


def load_to_bigquery(df, project_id, dataset_id, table_name, partition_col, partition_type='DAY'):
    """
    Load a pandas DataFrame into a partitioned BigQuery table, ensuring idempotency for daily loads.
    
    Args:
    - df (DataFrame): Pandas DataFrame to upload.
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_name (str): Destination table name.
    - partition_col (str): Column to partition by (must be DATE or TIMESTAMP).
    - partition_type (str): Type of partitioning ('DAY' or 'MONTH'). Default is 'DAY'.
    """
    
    # Ensure partition column is in the correct format
    df[partition_col] = pd.to_datetime(df[partition_col])  # Convert to TIMESTAMP
    
    # Create BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Define the full table ID
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Infer schema dynamically
    schema = infer_bq_schema(df)

    # Define partitioning configuration
    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY if partition_type.upper() == 'DAY' else bigquery.TimePartitioningType.MONTH,
        field=partition_col
    )

    # Create table configuration
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = time_partitioning

    # Create table if it doesn't exist
    try:
        client.create_table(table)
        print(f"Created partitioned table {table_id} (partitioned by {partition_col} - {partition_type})")
    except Exception as e:
        print(f"Table {table_id} already exists or error occurred: {e}")
    
    # Delete existing data for the same partition (partition_col) before inserting new data
    partition_date = df[partition_col].dt.date.unique()[0]  # Get the date of the partition
    delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE DATE({partition_col}) = '{partition_date}'
    """
    client.query(delete_query).result()  # Execute the delete query

    # Define the load job config with WRITE_TRUNCATE mode (overwrites affected partitions)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Appends data after truncating the affected partition
        time_partitioning=time_partitioning  # Ensure partitioning is applied
    )

    # Load the dataframe into BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Data successfully loaded into {table_id} with partitioning on {partition_col} ({partition_type})")

'''
def load_to_bigquery(df, project_id, dataset_id, table_name, partition_col, partition_type='DAY'):
    """
    Load a pandas DataFrame into a partitioned BigQuery table, automatically inferring schema.
    
    Args:
    - df (DataFrame): Pandas DataFrame to upload.
    - project_id (str): Google Cloud project ID.
    - dataset_id (str): BigQuery dataset ID.
    - table_name (str): Destination table name.
    - partition_col (str): Column to partition by (must be DATE or TIMESTAMP).
    - partition_type (str): Type of partitioning ('DAY' or 'MONTH'). Default is 'DAY'.
    """
    
    # Ensure partition column is in the correct format
    df[partition_col] = pd.to_datetime(df[partition_col])  # Convert to TIMESTAMP
    
    # Create BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Define the full table ID
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Infer schema dynamically
    schema = infer_bq_schema(df)

    # Define partitioning configuration
    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY if partition_type.upper() == 'DAY' else bigquery.TimePartitioningType.MONTH,
        field=partition_col
    )

    # Create table configuration
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = time_partitioning

    # Create table if it doesn't exist
    try:
        client.create_table(table)
        print(f"Created partitioned table {table_id} (partitioned by {partition_col} - {partition_type})")
    except Exception as e:
        print(f"Table {table_id} already exists or error occurred: {e}")

    # Define the load job config with WRITE_TRUNCATE mode (overwrites affected partitions)
    job_config = bigquery.LoadJobConfig(
        # write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite affected partitions only
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=time_partitioning  # Ensure partitioning is applied
    )

    # Load the dataframe into BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Data successfully loaded into {table_id} with partitioning on {partition_col} ({partition_type})")

'''

def connect_db(dbname):
    DB_HOST =  os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD =  os.getenv('DB_PASSWORD')
    DB_NAME = dbname #"OrderManagementSystem" 
    
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print("Connected successfully!")
        return connection
    except mysql.connector.Error as err:
        print("Error:", err)
