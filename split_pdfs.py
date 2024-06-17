import os
import boto3
import PyPDF2
from PyPDF2 import PdfReader, PdfWriter
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def split_pdf_to_individual_items(bucket, prefix):
    s3 = boto3.client('s3')
    local_pdf_dir = '/tmp/original_pdfs'
    os.makedirs(local_pdf_dir, exist_ok=True)

    # List all files in the specified S3 path
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        print(f"No files found in {bucket}/{prefix}")
        return

    for obj in response['Contents']:
        document = obj['Key']
        if document.endswith('/'):  # Skip directories
            continue
        local_pdf_path = os.path.join(local_pdf_dir, os.path.basename(document))
        print(f"Downloading {document} from {bucket}")
        s3.download_file(bucket, document, local_pdf_path)

        # Read the PDF
        reader = PdfReader(local_pdf_path)
        num_pages = len(reader.pages)

        item_start_page = None
        item_number = 0

        for page_num in range(num_pages):
            page = reader.pages[page_num]
            text = page.extract_text()

            if "Item #" in text:
                if item_start_page is not None:
                    # Save the previous item PDF
                    save_item_pdf(reader, item_start_page, page_num - 1, bucket, item_number, document)
                    item_number += 1

                # Mark the start of the new item
                item_start_page = page_num

        # Save the last item PDF
        if item_start_page is not None:
            save_item_pdf(reader, item_start_page, num_pages - 1, bucket, item_number, document)

def save_item_pdf(reader, start_page, end_page, bucket, item_number, document_name):
    writer = PdfWriter()
    for page_num in range(start_page, end_page + 1):
        writer.add_page(reader.pages[page_num])

    item_pdf_path = f'/tmp/item_{item_number}.pdf'
    with open(item_pdf_path, 'wb') as f:
        writer.write(f)

    # Upload to S3 with new key format
    item_s3_key = f"nashville/staff-reports-individual-pdfs/{os.path.basename(document_name)}_item_{item_number}.pdf"
    s3 = boto3.client('s3')
    print(f"Uploading {item_pdf_path} to {bucket}/{item_s3_key}")
    s3.upload_file(item_pdf_path, bucket, item_s3_key)
    os.remove(item_pdf_path)

if __name__ == "__main__":
    bucket_name = 'zoning-project'
    prefix = 'nashville/staff-reports/'

    split_pdf_to_individual_items(bucket_name, prefix)

