from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

def ingest_data():
    # Simulate ingestion to S3
    s3 = boto3.client('s3')
    s3.upload_file('data/sample.csv', 'my-bucket', 'iceberg/sample.csv')

with DAG('iceberg_ingestion', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    ingest = PythonOperator(task_id='ingest_to_s3', python_callable=ingest_data)
