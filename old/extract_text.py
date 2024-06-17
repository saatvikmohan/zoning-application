import os
import boto3
from textextract import start_document_text_detection, get_document_text_detection
from dotenv import load_dotenv
import boto3
import json
from dotenv import load_dotenv
from openai import OpenAI
from textextract import start_document_text_detection, get_document_text_detection
from decimal import Decimal
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from datetime import datetime
import json
# Load environment variables from .env file
load_dotenv()

def store_to_dynamodb(fields):
    dynamodb = boto3.resource('dynamodb')
    # table = dynamodb.Table('nashville-zoning-2')
    table = dynamodb.Table('test')
    
    # Convert float values to Decimal
    for key, value in fields.items():
        if isinstance(value, float):
            fields[key] = Decimal(str(value))
    
    # Create a unique partition key by combining Project Name and Date
    partition_key = f"{fields['Project Name']}_{fields['Date']}"
    fields['id'] = partition_key
    
    table.put_item(Item=fields)

def extract_text_from_pdf(bucket, document):
    job_id = start_document_text_detection(bucket, document)
    if job_id:
        responses = get_document_text_detection(job_id)
        text = ''
        for response in responses:
            for item in response["Blocks"]:
                if item["BlockType"] == "LINE":
                    text += item["Text"] + "\n"
        return text
    return ""

def process_individual_items(bucket, start_index=0):
    s3 = boto3.client('s3')
    prefix = 'nashville/staff-reports-individual-pdfs/'
    output_prefix = 'nashville/staff-reports-individual-txt-files/'

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        print(f"No files found in {bucket}/{prefix}")
        return

    counter = 0
    for obj in response['Contents']:
        if counter < start_index:
            counter += 1
            continue

        document = obj['Key']
        if document.endswith('/'):  # Skip directories
            continue
        print(f"Processing document {counter}: {document}")
        text = extract_text_from_pdf(bucket, document)
        if text:
            output_key = output_prefix + document.split('/')[-1].replace('.pdf', '.txt')
            s3.put_object(Bucket=bucket, Key=output_key, Body=text)
            print(f"Text extracted and stored at: {output_key}")
        else:
            print(f"No text extracted from PDF: {document}")

        counter += 1

if __name__ == "__main__":
    bucket_name = 'zoning-project'
    start_index = 0
    process_individual_items(bucket_name, start_index)