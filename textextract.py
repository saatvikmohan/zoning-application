import json
import re
from dotenv import load_dotenv
load_dotenv()

import boto3
from botocore.exceptions import ClientError
import time
import os

EXTRACT_PATH = os.getenv('EXTRACT_PATH')

def start_document_text_detection(bucket, document):
    """Start an asynchronous text detection job"""
    textract = boto3.client('textract')
    try:
        response = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document}}
        )
        return response['JobId']
    except ClientError as e:
        print(f"An error occurred: {e}")
        return None

def get_document_text_detection(job_id):
    """Get the results of the asynchronous text detection job"""
    textract = boto3.client('textract')
    next_token = None
    pages = []

    while True:
        try:
            params = {'JobId': job_id}
            if next_token:
                params['NextToken'] = next_token

            response = textract.get_document_text_detection(**params)
            status = response['JobStatus']
            print("Job status: ", status)

            if status == 'SUCCEEDED':
                pages.append(response)
                next_token = response.get('NextToken')
                while next_token:
                    response = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
                    pages.append(response)
                    next_token = response.get('NextToken')
                break
            elif status == 'IN_PROGRESS':
                time.sleep(5)
            elif status == 'FAILED':
                print("Job failed")
                break
            else:
                next_token = response.get('NextToken')
        except ClientError as e:
            print(f"An error occurred: {e}")
            break

    return pages

def extract_text_from_responses(responses):
    """Extract text from Textract responses"""
    text = ''
    for response in responses:
        blocks = response.get('Blocks', [])
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text += block['Text'] + '\n'
    return text

def upload_to_s3(bucket, key, content):
    """Upload a string as a file to S3"""
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket, Key=f"{EXTRACT_PATH}/{key}", Body=content)
        print(f"File uploaded to s3://{bucket}/{EXTRACT_PATH}/{key}")
    except ClientError as e:
        print(f"An error occurred: {e}")

def split_text_into_sections(text):
    """Split text into sections based on headings"""
    sections = re.split(r'(?m)^\s*[A-Z\s]+\s*$', text)
    sections = [section.strip() for section in sections if section.strip()]
    return sections

def process_pdf(bucket_name, s3_key):
    """Process a PDF file: extract text, split into sections, and upload to S3"""
    job_id = start_document_text_detection(bucket_name, s3_key)
    if job_id:
        responses = get_document_text_detection(job_id)
        extracted_text = extract_text_from_responses(responses)
        
        # Save full text
        text_s3_key = s3_key.replace('.pdf', '.txt')
        upload_to_s3(bucket_name, text_s3_key, extracted_text)
        
        # Split text into sections and save each section
        sections = split_text_into_sections(extracted_text)
        for i, section in enumerate(sections):
            section_s3_key = s3_key.replace('.pdf', f'_section_{i}.txt')
            upload_to_s3(bucket_name, section_s3_key, section)
        
        # Save metadata
        metadata = {
            'sections': [f"{s3_key.replace('.pdf', f'_section_{i}.txt')}" for i in range(len(sections))]
        }
        metadata_s3_key = s3_key.replace('.pdf', '_metadata.json')
        upload_to_s3(bucket_name, metadata_s3_key, json.dumps(metadata))
        print(f"Extracted text and metadata saved for {s3_key}")
    else:
        print("Failed to start text detection job")

if __name__ == "__main__":
    bucket_name = os.getenv('S3_BUCKET_NAME')
    file_name = os.getenv('PDF_FILE')
    
    process_pdf(bucket_name, file_name)