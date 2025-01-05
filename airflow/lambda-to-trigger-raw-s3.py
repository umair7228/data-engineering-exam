import json
import boto3
import uuid
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')

# Your S3 bucket name
BUCKET_NAME = 's3-lambda-raw-data'

# Lambda function handler
def lambda_handler(event, context):
    # The event contains the Spotify data from CloudWatch
    print("Received event:", json.dumps(event, indent=2))

    # Extract the data (you will need to modify this based on the structure of your event)
    # For this example, it's assumed the data is inside the "detail" field.
    spotify_data = event['detail']

    # Generate a unique filename based on the current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"spotify_data_{timestamp}_{uuid.uuid4().hex}.json"

    # Store the Spotify data in the S3 bucket as a JSON file
    try:
        response = s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(spotify_data),
            ContentType='application/json'
        )
        print(f"Data successfully saved to S3: {response}")
        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully saved to S3!')
        }
    except Exception as e:
        print(f"Error saving data to S3: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error saving data to S3.')
        }