import os
import boto3
from textextract import start_document_text_detection, get_document_text_detection
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

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

def process_document(bucket, document, output_prefix):
    s3 = boto3.client('s3')
    text = extract_text_from_pdf(bucket, document)
    if text:
        output_key = output_prefix + document.split('/')[-1].replace('.pdf', '.txt')
        s3.put_object(Bucket=bucket, Key=output_key, Body=text)
        print(f"Text extracted and stored at: {output_key}")
    else:
        print(f"No text extracted from PDF: {document}")


def process_individual_items(bucket, start_index=0):
    s3 = boto3.client('s3')
    prefix = 'nashville/staff-reports-individual-pdfs/'
    output_prefix = 'nashville/staff-reports-individual-txt-files/'

    documents = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' in response:
            documents.extend([obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/')])

        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break

    # Ensure documents are processed starting from the specified index
    documents_to_process = documents[start_index:]
    counter = 0
    max_workers = 8  # Adjust based on your system and requirements
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_document, bucket, doc, output_prefix): doc for doc in documents_to_process}
        for future in as_completed(futures):
            doc = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing document {doc}: {e}")
            counter += 1
            print(f"Processed document {counter}: {doc}")


if __name__ == "__main__":
    bucket_name = 'zoning-project'
    start_index = 0
    process_individual_items(bucket_name, start_index)