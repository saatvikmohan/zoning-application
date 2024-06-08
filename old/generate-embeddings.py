#fix to avoid adding 050924DraftMinutes.json

from langchain.embeddings import OpenAIEmbeddings
import json
import boto3
import re
from io import StringIO
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')


def generate_embeddings(texts):
    """Generate embeddings for a list of texts using Langchain"""
    api_key = os.getenv('OPENAI_API_KEY')
    embeddings_model = OpenAIEmbeddings(model="text-embedding-ada-002", api_key=api_key)
    embeddings = [embeddings_model.embed_query(text) for text in texts]
    return embeddings

def read_text_from_s3(bucket_name, s3_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    text = obj['Body'].read().decode('utf-8')
    return text

def write_embeddings_to_s3(bucket_name, s3_key, embeddings):
    s3 = boto3.client('s3')
    embeddings_str = json.dumps(embeddings)
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=embeddings_str)

def list_txt_files_in_s3(bucket_name, extract_path):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=extract_path)
    txt_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.txt')]
    return txt_files

if __name__ == "__main__":
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    EXTRACT_PATH = os.getenv('EXTRACT_PATH')
    EMBEDDING_PATH = os.getenv('EMBEDDING_PATH')
    
    txt_files = list_txt_files_in_s3(S3_BUCKET_NAME, EXTRACT_PATH)
    
    for txt_file in txt_files:
        # Exclude files that do not contain "_section_" in their names
        if "_section_" not in txt_file:
            continue
        
        text = read_text_from_s3(S3_BUCKET_NAME, txt_file)
        
        # Generate embeddings for the entire text file instead of splitting into sections
        embeddings = generate_embeddings([text])  # Pass the entire text as a single element list
        
        embedding_file = os.path.join(EMBEDDING_PATH, os.path.basename(txt_file).replace('.txt', '.json'))
        write_embeddings_to_s3(S3_BUCKET_NAME, embedding_file, embeddings)
        print(f"Embeddings generated and saved to s3://{S3_BUCKET_NAME}/{embedding_file}")