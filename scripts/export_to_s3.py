import os
import pymongo
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from tqdm import tqdm
from pprint import pprint
import sys
from datetime import datetime
from dateutil.parser import isoparse
import json

# Set up your DocumentDB connection using environment variables or default values
mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://foundation:PASSWORD@foundation-indexed-918816454019.us-east-1.docdb-elastic.amazonaws.com/?tls=true&tlsCAFile=/app/SFSRootCAG2.pem&tlsAllowInvalidHostnames=true&authMechanism=DEFAULT&authSource=foundation')

# Construct the MongoDB client with the required parameters
client = pymongo.MongoClient(mongodb_uri)

database_name = os.getenv('DATABASE_NAME', 'foundation')
collection_name = os.getenv('COLLECTION_NAME', 'txs-index-address')

db = client[database_name]
collection = db[collection_name]

# Set up your S3 connection using environment variables or default values
s3 = boto3.client('s3')
bucket_name = os.getenv('S3_BUCKET_NAME', 'your_s3_bucket_name')
s3_prefix = collection_name  # Set S3 prefix to the collection name
checkpoint_key = f'{s3_prefix}/checkpoint.txt'  # Place checkpoint.txt in the s3prefix folder

# Define the size of each chunk to export (in documents)
chunk_size = int(os.getenv('CHUNK_SIZE', 250000))  # Default to 250000 if not set
max_retries = int(os.getenv('MAX_RETRIES', 5))  # Default to 5 if not set

def export_to_s3(file_path, chunk_number):
    file_name = f'{s3_prefix}/export/documentdb_chunk_{chunk_number}.parquet'
    s3.upload_file(file_path, bucket_name, file_name)

def get_last_checkpoint():
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=checkpoint_key)
        checkpoint_data = obj['Body'].read().decode('utf-8').strip()
        if checkpoint_data:
            last_timestamp, processed_docs_count = checkpoint_data.split(',')
            return last_timestamp, int(processed_docs_count)
        return None, 0
    except s3.exceptions.NoSuchKey:
        return None, 0

def save_checkpoint(last_timestamp, processed_docs_count):
    s3.put_object(Bucket=bucket_name, Key=checkpoint_key, Body=f"{last_timestamp},{processed_docs_count}")

def get_cursor(last_timestamp=None):
    retries = 0
    while retries < max_retries:
        try:
            if last_timestamp:
                return collection.find({'timestamp': {'$gte': isoparse(last_timestamp)}}).sort('timestamp', pymongo.ASCENDING).limit(chunk_size)
            return collection.find().sort('timestamp', pymongo.ASCENDING).limit(chunk_size)
        except Exception as e:
            print(f'Cursor timeout, retrying ({retries + 1}/{max_retries})... Error: {e}')
            retries += 1
            time.sleep(5)  # Wait before retrying
    raise Exception('Max retries reached, unable to continue.')

last_timestamp, processed_docs_count = get_last_checkpoint()
cursor = get_cursor(last_timestamp)
chunk = list(cursor)
initial_run = True

# Handle initial run where last_timestamp is None
if last_timestamp is None:
    try:
        if chunk:
            last_timestamp = chunk[0]['timestamp'].isoformat()
        else:
            print("No documents found in the collection.")
            cursor.close()
            exit(0)
    except Exception as e:
        print(f'Error during initial fetch: {e}')
        cursor.close()
        exit(1)

# Use approximate count
total_docs = collection.estimated_document_count()

def serialize_document(doc):
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc

try:
    with tqdm(total=total_docs, desc="Exporting data", initial=processed_docs_count) as pbar:
        while True:
            if initial_run:
                initial_run = False
            else:
                chunk = list(cursor)
            
            # Transform the documents
            transformed_chunk = [{'_id': doc['_id'], '_doc': json.dumps(serialize_document(doc))} for doc in chunk]
            
            df = pd.DataFrame(transformed_chunk)
            table = pa.Table.from_pandas(df)
            file_path = f'/tmp/temp_file_{processed_docs_count}.parquet'
            pq.write_table(table, file_path)
            export_to_s3(file_path, processed_docs_count)
            os.remove(file_path)  # Clean up the local file
            last_timestamp = chunk[-1]['timestamp']
            processed_docs_count += len(chunk)
            save_checkpoint(last_timestamp, processed_docs_count)
            pbar.update(len(chunk))
            if len(chunk) < chunk_size:
                break
            cursor = get_cursor(last_timestamp)
except Exception as e:
    print(f'Error: {e}')
finally:
    cursor.close()

print('Export completed.')