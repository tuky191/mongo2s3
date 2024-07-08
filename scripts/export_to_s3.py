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
import threading

# Set up your DocumentDB connection using environment variables or default values
mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://user:PASSWORD@localhost/?tls=true&tlsCAFile=/app/SFSRootCAG2.pem&tlsAllowInvalidHostnames=true&authMechanism=DEFAULT')
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

# Define the file size limit for each chunk (in bytes)
file_size_limit = int(os.getenv('FILE_SIZE_LIMIT', 100 * 1024 * 1024))  # Default to 100 MB if not set
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

def open_new_file(processed_docs_count):
    filename = f"/tmp/temp_file_{processed_docs_count}.parquet"
    return filename

def offload_file(filename):
    def offload():
        print(f"Offloading {filename}")
        # Implement your offloading logic here (e.g., upload to S3)
        # Simulating S3 upload
        export_to_s3(filename, processed_docs_count)
        os.remove(filename)
    
    threading.Thread(target=offload).start()

last_timestamp, processed_docs_count = get_last_checkpoint()
query = {} if last_timestamp is None else {'timestamp': {'$gte': isoparse(last_timestamp)}}
cursor = collection.find(query, no_cursor_timeout=True).sort('timestamp', pymongo.ASCENDING)

# Use approximate count
total_docs = collection.estimated_document_count()

def serialize_document(doc):
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc

try:
    with tqdm(total=total_docs, desc="Exporting data", initial=processed_docs_count) as pbar:
        current_size = 0
        documents = []

        for document in cursor:
            # Transform and serialize the document
            serialized_doc = serialize_document(document)
            documents.append(serialized_doc)
            current_size += len(json.dumps(serialized_doc))

            # Update the last processed timestamp
            last_timestamp = document['timestamp'].isoformat()

            # Check if the current file size limit is reached
            if current_size >= file_size_limit:
                file_path = open_new_file(processed_docs_count)
                
                # Write to Parquet
                df = pd.DataFrame(documents)
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_path)
                
                offload_file(file_path)
                
                processed_docs_count += 1
                save_checkpoint(last_timestamp, processed_docs_count)
                
                documents = []
                current_size = 0

            pbar.update(1)

        # Handle the remaining documents
        if documents:
            file_path = open_new_file(processed_docs_count)
            
            # Write to Parquet
            df = pd.DataFrame(documents)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)
            
            offload_file(file_path)
            
            processed_docs_count += 1
            save_checkpoint(last_timestamp, processed_docs_count)

except Exception as e:
    print(f'Error: {e}')
    sys.exit(1)
finally:
    cursor.close()

print('Export completed.')
sys.exit(0)
