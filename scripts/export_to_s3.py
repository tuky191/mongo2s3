import os
import pymongo
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time

# Set up your DocumentDB connection using environment variables
mongodb_uri = os.environ['MONGODB_URI']
mongodb_username = os.environ['MONGODB_USERNAME']
mongodb_password = os.environ['MONGODB_PASSWORD']
database_name = os.environ['DATABASE_NAME']
collection_name = os.environ['COLLECTION_NAME']

client = pymongo.MongoClient(mongodb_uri, username=mongodb_username, password=mongodb_password, ssl=True)
db = client[database_name]
collection = db[collection_name]

# Set up your S3 connection using environment variables
s3 = boto3.client('s3')
bucket_name = os.environ['S3_BUCKET_NAME']
s3_prefix = os.environ.get('S3_PREFIX', '')  # Optional, for organizing files

# Define the size of each chunk to export (in documents)
chunk_size = int(os.environ.get('CHUNK_SIZE', 1000))  # Default to 1000 if not set
checkpoint_file = 'checkpoint.txt'
max_retries = int(os.environ.get('MAX_RETRIES', 5))  # Default to 5 if not set

def export_to_s3(file_path, chunk_number):
    file_name = f'{s3_prefix}documentdb_export_chunk_{chunk_number}.parquet'
    s3.upload_file(file_path, bucket_name, file_name)
    print(f'Uploaded {file_name} to S3')

def get_last_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return f.read().strip()
    return None

def save_checkpoint(last_timestamp):
    with open(checkpoint_file, 'w') as f:
        f.write(last_timestamp)

def get_cursor(last_timestamp=None):
    retries = 0
    while retries < max_retries:
        try:
            if last_timestamp:
                return collection.find({'timestamp': {'$gte': last_timestamp}}).sort('timestamp', pymongo.ASCENDING).limit(chunk_size)
            return collection.find().sort('timestamp', pymongo.ASCENDING).limit(chunk_size)
        except Exception as e:
            print(f'Cursor timeout, retrying ({retries + 1}/{max_retries})... Error: {e}')
            retries += 1
            time.sleep(5)  # Wait before retrying
    raise Exception('Max retries reached, unable to continue.')

last_timestamp = get_last_checkpoint()
cursor = get_cursor(last_timestamp)
chunk = []

try:
    while True:
        chunk = list(cursor)
        if not chunk:
            break

        df = pd.DataFrame(chunk)
        table = pa.Table.from_pandas(df)
        file_path = f'/tmp/temp_file_{last_timestamp}.parquet'
        pq.write_table(table, file_path)
        export_to_s3(file_path, last_timestamp)
        os.remove(file_path)  # Clean up the local file

        last_timestamp = chunk[-1]['timestamp'].isoformat()
        save_checkpoint(last_timestamp)

        cursor = get_cursor(last_timestamp)
except Exception as e:
    print(f'Error: {e}')
finally:
    cursor.close()

print('Export completed.')
