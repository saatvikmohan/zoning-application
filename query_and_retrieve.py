import logging
import boto3
import os
import json
from dotenv import load_dotenv
from langchain.embeddings import OpenAIEmbeddings
import pinecone
from pinecone import Pinecone, ServerlessSpec
from openai import OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS and Pinecone configurations
aws_bucket_name = os.getenv('S3_BUCKET_NAME', 'nashville-minutes')
openai_api_key = os.getenv('OPENAI_API_KEY')
pinecone_api_key = os.getenv('PINECONE_API_KEY')
pinecone_index_name = os.getenv('PINECONE_INDEX_NAME', 'test-minutes-doc')
pinecone_environment = os.getenv('PINECONE_ENVIRONMENT', 'default')

# Initialize clients
s3 = boto3.client('s3')

client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)

def init_pinecone():
    return Pinecone(api_key=pinecone_api_key)

def search_similar_documents(query, top_k=10):
    pc = init_pinecone()
    index = pc.Index(pinecone_index_name)
    
    # Create embedding for the query using Langchain
    embeddings_model = OpenAIEmbeddings(model="text-embedding-ada-002", api_key=openai_api_key)
    query_embeddings = embeddings_model.embed_query(query)
    
    # Query Pinecone with the query embedding
    response = index.query(namespace="", vector=query_embeddings, top_k=top_k, include_values=True, include_metadata=True)
    logger.info(f"Search response: {response}")
    return response['matches']

def fetch_section_from_s3(bucket_name, section_id, path):
    # Normalize the section_id format
    if "section" not in section_id:
        parts = section_id.split('-')
        if len(parts) == 2:
            section_id = f"{parts[0]}_section_{parts[1]}"
    else:
        # Handle cases where section_id might already include "section" but in a different format
        section_id = section_id.replace('-', '_')
    
    section_s3_key = f"{path}/{section_id}.txt"
    logger.info(f"Fetching from S3 with key: {section_s3_key}")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=section_s3_key)
        content = response['Body'].read().decode('utf-8')
        return content
    except s3.exceptions.NoSuchKey:
        logger.error(f"No such key: {section_s3_key}. Please check if the file exists in the S3 bucket.")
        return None
    except Exception as e:
        logger.error(f"An error occurred while fetching the file: {e}")
        return None

def generate_answer(query, section_content):
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": f"Answer the following question based on the provided content:\n\nContent: {section_content}\n\nQuestion: {query}\n\nAnswer:"}
        ],
        max_tokens=200
    )
    return response.choices[0].message.content.strip()

def query_and_answer(query):
    matches = search_similar_documents(query)
    results = []

    for match in matches:
        section_id = match['id']
        logger.info(f"Section ID: {section_id}, Score: {match['score']}")
        section_content = fetch_section_from_s3(aws_bucket_name, section_id, 'extracted-txt-files')
        if section_content:
            answer = generate_answer(query, section_content)
            results.append((section_id, answer))
    return results

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Document processing pipeline")
    parser.add_argument('--query', type=str, required=True, help="Query to search and answer")
    
    args = parser.parse_args()
    results = query_and_answer(args.query)
    for section_id, answer in results:
        print(f"Section ID: {section_id}\nAnswer: {answer}\n")
