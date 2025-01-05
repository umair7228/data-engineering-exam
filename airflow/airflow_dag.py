from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import csv
import io
import boto3
from datetime import timedelta

# Define the DAG
dag = DAG(
    's3_data_transformation',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='S3 data transformation DAG',
    schedule_interval=None,  # Triggered manually or via an SQS event notification
    start_date=days_ago(1),
    catchup=False,
)

# Function to transform data from JSON to CSV
def transform_playlist_data(raw_data):
    """
    Transforms playlist data into CSV format.
    """
    try:
        # Parse raw JSON content into Python dictionary
        data = json.loads(raw_data)
        
        # Extract playlist details
        playlists = data.get("items", [])
        
        # Prepare CSV header
        csv_data = "Name,Tracks\n"
        
        # Append rows
        for playlist in playlists:
            name = playlist.get("name", "Unknown")
            tracks = playlist.get("tracks", {}).get("total", 0)
            csv_data += f"{name},{tracks}\n"
        
        return csv_data
    except Exception as e:
        print(f"Error transforming data: {e}")
        raise

# Function to download, transform and upload data to S3 using boto3
def transform_and_load_data(source_bucket, target_bucket, target_prefix='output/'):
    s3_client = boto3.client('s3')

    try:
        # List objects in the source bucket
        response = s3_client.list_objects_v2(Bucket=source_bucket)

        if 'Contents' not in response:
            print("No objects found in the source bucket.")
            return
        
        # Iterate through all objects in the source bucket
        for obj in response['Contents']:
            source_key = obj['Key']

            # Download the file from the source bucket
            response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            file_content = response['Body'].read().decode('utf-8')  # Read and decode file content
            
            # Transform the JSON data into CSV format
            transformed_data = transform_playlist_data(file_content)
            
            # Define the target file key (add a prefix to organize data in the target bucket)
            target_key = f"{target_prefix}{source_key.split('/')[-1].replace('.json', '.csv')}"
            
            # Upload the transformed data to the target S3 bucket
            s3_client.put_object(
                Bucket=target_bucket,
                Key=target_key,
                Body=transformed_data.encode('utf-8'),
                ContentType='text/csv'
            )
            print(f"Successfully uploaded transformed data to {target_key}")

    except Exception as e:
        print(f"Error occurred: {e}")
        raise

# Define the task to trigger data transformation
transform_task = PythonOperator(
    task_id='transform_and_load_data',
    python_callable=transform_and_load_data,
    op_args=[
        's3-lambda-raw-data',  # Source bucket name
        'transform-data-um',  # Target bucket name
    ],
    dag=dag,
)

# Set the task dependencies
transform_task