import pinecone
from pinecone import ServerlessSpec
import json
import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')

def init_pinecone(api_key):
    """Initialize Pinecone"""
    return pinecone.Pinecone(api_key=api_key)

def create_index(pc, index_name, dimension):
    """Create Pinecone index"""
    if index_name not in pc.list_indexes().names():
        spec = ServerlessSpec(
            cloud='aws',  # Specify the cloud provider
            region='us-east-1'  # Specify the region
        )
        pc.create_index(
            name=index_name,
            dimension=dimension,
            metric='euclidean',  # Assuming 'euclidean' as the metric
            spec=spec
        )
    else:
        # Ensure the existing index has the correct dimension
        index_info = pc.describe_index(index_name)
        if index_info['dimension'] != dimension:
            raise ValueError(f"Existing index dimension {index_info['dimension']} does not match required dimension {dimension}")
    index = pc.Index(index_name)
    return index

def upsert_embeddings(index, embeddings, document_id):
    """Upsert embeddings into Pinecone index"""
    # Flatten the embeddings if they are in an extra list
    if isinstance(embeddings, list) and len(embeddings) == 1 and isinstance(embeddings[0], list):
        embeddings = embeddings[0]
    index.upsert(vectors=[(document_id, embeddings)])
    print(f"Embeddings for document {document_id} upserted to Pinecone")

def list_s3_files(bucket_name, prefix):
    """List all files in an S3 bucket with a given prefix"""
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [content['Key'] for content in response.get('Contents', [])]

def download_s3_file(bucket_name, key, download_path):
    """Download a file from S3"""
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, key, download_path)

if __name__ == "__main__":
    api_key = os.getenv('PINECONE_API_KEY')
    index_name = os.getenv('PINECONE_INDEX_NAME', 'test-minutes')
    bucket_name = os.getenv('S3_BUCKET_NAME', 'nashville-minutes')
    embedding_path = os.getenv('EMBEDDING_PATH', 'embedding-files')

    pc = init_pinecone(api_key)
    
    # List all embedding files in the S3 bucket
    embedding_files = list_s3_files(bucket_name, embedding_path)
    
    for file_key in embedding_files:
        local_file_path = os.path.join('/tmp', os.path.basename(file_key))
        download_s3_file(bucket_name, file_key, local_file_path)
        
        with open(local_file_path, 'r') as f:
            embeddings = json.load(f)
        
        # Check if embeddings need flattening and calculate dimension correctly
        if isinstance(embeddings, list) and len(embeddings) == 1 and isinstance(embeddings[0], list):
            embeddings = embeddings[0]  # Flatten the embeddings
        dimension = len(embeddings)  # Calculate dimension after potential flattening

        index = create_index(pc, index_name, dimension=dimension)
        
        # Upsert the embeddings
        document_id = os.path.splitext(os.path.basename(file_key))[0]
        upsert_embeddings(index, embeddings, document_id)