import json
import boto3
import urllib.parse

# Initialize the S3 client
s3_client = boto3.client('s3')

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

def lambda_handler(event, context):
    # Extract information from the event
    record = event['Records'][0]
    
    # Bucket name and file details from the event
    source_bucket_name = record['s3']['bucket']['name']
    file_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
    
    # Define the new file path in the target bucket (new bucket for transformed data)
    target_bucket_name = 'transform-data-um'  # Replace with the target bucket name
    output_key = f"{file_key.split('/')[-1].replace('.json', '.csv')}"  # Change extension to CSV
    
    try:
        # Download the file from the source bucket
        response = s3_client.get_object(Bucket=source_bucket_name, Key=file_key)
        file_content = response['Body'].read().decode("utf-8")  # Read and decode file content
        
        # Transform the JSON data into CSV format
        transformed_data = transform_playlist_data(file_content)
        
        # Upload the transformed data to the target bucket
        s3_client.put_object(
            Bucket=target_bucket_name,
            Key=output_key,
            Body=transformed_data.encode("utf-8"),
            ContentType='text/csv'
        )
        
        print(f"Transformed file {file_key} saved to {target_bucket_name}/{output_key}")
        
        # Optionally remove the 'output/' folder from the source bucket by deleting its objects
        # List objects in the 'output/' folder and delete them
        list_objects_response = s3_client.list_objects_v2(
            Bucket=source_bucket_name,
            Prefix='output/'  # Target the 'output/' folder
        )
        
        if 'Contents' in list_objects_response:
            delete_objects = {
                'Objects': [{'Key': obj['Key']} for obj in list_objects_response['Contents']]
            }
            s3_client.delete_objects(
                Bucket=source_bucket_name,
                Delete=delete_objects
            )
            print("Removed objects in 'output/' folder from the source bucket")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"File transformed and saved successfully to {target_bucket_name}/{output_key}")
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file {file_key}: {str(e)}")
        }