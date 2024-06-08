import os
import boto3
import PyPDF2
from PyPDF2 import PdfReader, PdfWriter
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


def split_pdf_to_individual_items(bucket, prefix):
    s3 = boto3.client('s3')
    local_pdf_dir = '/tmp/original_pdfs'
    os.makedirs(local_pdf_dir, exist_ok=True)

    # List all files in the specified S3 path
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    print(response)
    if 'Contents' not in response:
        print(f"No files found in {bucket}/{prefix}")
        return []

    item_pdf_paths = []
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
                    item_pdf_path = save_item_pdf(reader, item_start_page, page_num - 1, bucket, item_number)
                    item_pdf_paths.append(item_pdf_path)
                    item_number += 1

                # Mark the start of the new item
                item_start_page = page_num

        # Save the last item PDF
        if item_start_page is not None:
            item_pdf_path = save_item_pdf(reader, item_start_page, num_pages - 1, bucket, item_number)
            item_pdf_paths.append(item_pdf_path)

    return item_pdf_paths


def save_item_pdf(reader, start_page, end_page, bucket, item_number):
    writer = PdfWriter()
    for page_num in range(start_page, end_page + 1):
        writer.add_page(reader.pages[page_num])

    item_pdf_path = f'/tmp/item_{item_number}.pdf'
    with open(item_pdf_path, 'wb') as f:
        writer.write(f)

    # Upload to S3
    item_s3_key = f"nashville/{os.getenv('INDIVIDUAL_REQUEST_PDF_PATHS')}/item_{item_number}.pdf"
    s3 = boto3.client('s3')
    print(f"Uploading {item_pdf_path} to {bucket}/{item_s3_key}")
    s3.upload_file(item_pdf_path, bucket, item_s3_key)
    os.remove(item_pdf_path)
    return item_s3_key

def extract_text_from_pdf(bucket, document):
    textract = boto3.client('textract')
    response = textract.detect_document_text(
        Document={'S3Object': {'Bucket': bucket, 'Name': document}}
    )
    text = ''
    for item in response["Blocks"]:
        if item["BlockType"] == "LINE":
            text += item["Text"] + "\n"
    return text

import openai
import json

def extract_fields_with_gpt(text, pdf_link):
    openai.api_key = os.getenv("OPENAI_API_KEY")
    prompt = (
        "Extract the following fields from the text:\n"
        "Applicant, Project Name, Council District, School District, Council Person, "
        "Requested by, Date, Staff Recommendation Description, Staff Recommendation, "
        "Existing Zoning Type, Proposed Zoning Type, Location, Location Description, "
        "Community Character Policy, Community Character Policy ID, Community Plan, "
        "Number units proposed, Number of acres, Number of units currently, Request Type.\n\n"
        "Here is an example:\n\n"
        "Example:\n"
        "Input Text:\n"
        "Item #1 Major Plan Amendment 2023CP-003-005\n"
        "Project Name: Bordeaux-Whites Creek-Haynes Trinity Community Plan Amendment\n"
        "Council District: 02 - Toombs\n"
        "School District: 01 - Gentry\n"
        "Requested by: Metro Planning Department, applicant; various owners.\n"
        "Deferrals: This item was deferred from the April 25, 2024, and May 9, 2024, Planning Commission meetings. No public hearing was held.\n"
        "Staff Reviewer: Clark\n"
        "Staff Recommendation: Defer to the June 13, 2024, Planning Commission meeting.\n"
        "Applicant Request: Amend Bordeaux-Whites Creek-Haynes Trinity Community Plan to change the community character policy.\n"
        "Major Plan Amendment: A request to amend the Bordeaux-Whites Creek-Haynes Trinity Community Plan by changing the policy from Urban Neighborhood Evolving (T4 NE) to Urban Neighborhood Center (T4 NC) for properties located at the southwest corner of Cliff Drive and Buena Vista Pike, zoned R8 (One and Two-Family Residential) (5.55 acres).\n"
        "Staff Recommendation: Staff recommends deferral to the June 13, 2024, Planning Commission meeting.\n\n"
        "Extracted Fields:\n"
        "{\n"
        "  \"Applicant\": \"Metro Planning Department, applicant; various owners\",\n"
        "  \"Project Name\": \"Bordeaux-Whites Creek-Haynes Trinity Community Plan Amendment\",\n"
        "  \"Council District\": \"02\",\n"
        "  \"School District\": \"01\",\n"
        "  \"Council Person\": \"Toombs\",\n"
        "  \"Requested by\": \"Metro Planning Department\",\n"
        "  \"Date\": \"May 23, 2024\",\n"
        "  \"Staff Recommendation Description\": \"Staff recommends deferral to the June 13, 2024, Planning Commission meeting.\",\n"
        "  \"Staff Recommendation\": \"defer\",\n"
        "  \"Existing Zoning Type\": \"R8\",\n"
        "  \"Proposed Zoning Type\": \"T4 NC\",\n"
        "  \"Location\": \"southwest corner of Cliff Drive and Buena Vista Pike\",\n"
        "  \"Location Description\": \"Properties located at the southwest corner of Cliff Drive and Buena Vista Pike, zoned R8 (One and Two-Family Residential) (5.55 acres).\",\n"
        "  \"Community Character Policy\": \"Urban Neighborhood Evolving (T4 NE) to Urban Neighborhood Center (T4 NC)\",\n"
        "  \"Community Character Policy ID\": \"T4 NC\",\n"
        "  \"Community Plan\": \"Bordeaux-Whites Creek-Haynes Trinity Community Plan\",\n"
        "  \"Number units proposed\": \"N/A\",\n"
        "  \"Number of acres\": \"5.55\",\n"
        "  \"Number of units currently\": \"N/A\",\n"
        "  \"Request Type\": \"major plan amendment\",\n"
        "  \"pdf_link\": \"s3://your-bucket-name/PDF_FILES_ORIGINAL/item_1.pdf\"\n"
        "}\n\n"
        "Text:\n" + text
    )
    response = openai.Completion.create(
        model="gpt-3.5-turbo",
        prompt=prompt,
        max_tokens=1500,
        n=1,
        stop=None,
        temperature=0.7,
    )
    fields = response.choices[0].text.strip()
    fields_json = json.loads(fields)
    fields_json["pdf_link"] = pdf_link
    return fields_json

def upload_to_s3(bucket, key, content):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=content)

def process_individual_items(bucket, pdf_keys):
    for pdf_key in pdf_keys[:10]:  # Limit to 10 items for now
        text = extract_text_from_pdf(bucket, pdf_key)
        pdf_link = f"s3://{bucket}/{pdf_key}"
        fields = extract_fields_with_gpt(text, pdf_link)
        print(json.dumps(fields, indent=2))

if __name__ == "__main__":
    bucket_name = 'zoning-project'
    prefix = 'nashville/original_pdfs/'

    item_pdf_paths = split_pdf_to_individual_items(bucket_name, prefix)

    # Process the individual item PDFs
    process_individual_items(bucket_name, item_pdf_paths)
