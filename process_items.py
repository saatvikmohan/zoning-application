import os
import boto3
import json
from dotenv import load_dotenv
from openai import OpenAI
from textextract import start_document_text_detection, get_document_text_detection
from decimal import Decimal
# Load environment variables from .env file
load_dotenv()

def store_to_dynamodb(fields):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('test')
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

from datetime import datetime
import json

def extract_fields_with_gpt(text, pdf_link):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    instructions = (
        "Extract the following fields from the text and ensure the types are correct:\n"
        "Request Type (either SP Amendment, Zone Change, Preliminary SP, Final Plat, Concept Plan, or Major Plan Amendment), "
        "Applicant, Project Name, Council District (as an integer), School District (as an integer), Council Person, "
        "Requested by, Date (as DateType), Staff Recommendation Description, Staff Recommendation (as 'defer', 'approve', or 'conditional'), "
        "Existing Zoning Type (code only), Proposed Zoning Type (code only), Location (single street or address), "
        "Location Description, Community Character Policy, Community Character Policy ID, Community Plan, "
        "Number of units proposed (as float), Number of acres (as float), Number of units currently (as float), "
        "Here are some examples:\n\n"
        "Example 1:\n"
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
        "  \"Request Type\": \"major plan amendment\",\n"
        "  \"Applicant\": \"Metro Planning Department, applicant; various owners\",\n"
        "  \"Project Name\": \"Bordeaux-Whites Creek-Haynes Trinity Community Plan Amendment\",\n"
        "  \"Council District\": 2,\n"
        "  \"School District\": 1,\n"
        "  \"Council Person\": \"Toombs\",\n"
        "  \"Requested by\": \"Metro Planning Department\",\n"
        "  \"Date\": \"May 23, 2024\",\n"
        "  \"Staff Recommendation Description\": \"Staff recommends deferral to the June 13, 2024, Planning Commission meeting.\n"
        "  \"Staff Recommendation\": \"defer\",\n"
        "  \"Existing Zoning Type ID\": \"R8\",\n"
        "  \"Proposed Zoning Type ID\": \"T4 NC\",\n"
        "  \"Location\": \"Cliff Drive\",\n"
        "  \"Location Description\": \"southwest corner of Cliff Drive and Buena Vista Pike, 5.55 acres\",\n"
        "  \"Community Character Policy\": \"Urban Neighborhood Evolving (T4 NE) to Urban Neighborhood Center (T4 NC)\",\n"
        "  \"Community Character Policy ID\": \"T4 NC\",\n"
        "  \"Community Plan\": \"Bordeaux-Whites Creek-Haynes Trinity Community Plan\",\n"
        "  \"Number of units proposed\": \"N/A\",\n"
        "  \"Number of acres\": \"5.55\",\n"
        "  \"Number of units currently\": \"N/A\",\n"
        "}\n\n"
        "Input Text:\n"
        f"{text}\n\n"
        "Extracted Fields:\n"
    )

    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that extracts specific fields from text and ensures the types are correct."
            },
            {
                "role": "user",
                "content": instructions,
            }
        ],
        max_tokens=500,
        model="gpt-3.5-turbo",
    )
    response_text = response.choices[0].message.content.strip()
    print("Response JSON:", response_text)  # Debugging line to inspect the JSON before parsing

    # Check if the response_text is not empty and appears to be JSON
    if response_text:
        try:
            extracted_fields = json.loads(response_text)
        except json.JSONDecodeError as e:
            print("JSON decoding failed:", e)
            print("Faulty JSON content:", response_text)
            return None  # Return None or handle as appropriate
    else:
        print("Received empty or non-JSON response")
        return None  # Return None or handle as appropriate


    extracted_fields = json.loads(response.choices[0].message.content.strip())
    
    # Convert 'N/A' and empty strings to None
    for key, value in extracted_fields.items():
        if value == 'N/A' or value == '':
            extracted_fields[key] = None

    # Convert types with checks for None values and 'N/A'
    extracted_fields['Council District'] = str(int(extracted_fields['Council District']))
    extracted_fields['School District'] = str(int(extracted_fields['School District']))
    date_str = extracted_fields['Date']
    extracted_fields['Date'] = datetime.strptime(date_str, '%B %d, %Y').date() if date_str not in ('N/A', None) else None
    
    # Convert date to string format for DynamoDB
    if extracted_fields['Date'] is not None:
        extracted_fields['Date'] = extracted_fields['Date'].strftime('%Y-%m-%d')
    
    # Convert numeric fields to string, handling 'N/A' and None
    num_units_proposed = extracted_fields.get('Number of units proposed', '0')
    extracted_fields['Number of units proposed'] = str(num_units_proposed) if num_units_proposed not in ('N/A', None) else None
    
    num_acres = extracted_fields.get('Number of acres', '0')
    extracted_fields['Number of acres'] = str(num_acres) if num_acres not in ('N/A', None) else None
    
    num_units_current = extracted_fields.get('Number of units currently', '0')
    extracted_fields['Number of units currently'] = str(num_units_current) if num_units_current not in ('N/A', None) else None
    
    extracted_fields['pdf_link'] = pdf_link

    return extracted_fields

def process_individual_items(bucket):
    s3 = boto3.client('s3')
    prefix = 'nashville/individual-request-pdfs/'

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in response:
        print(f"No files found in {bucket}/{prefix}")
        return

    counter = 0
    for obj in response['Contents']:
        counter = counter + 1
        document = obj['Key']
        if document.endswith('/'):  # Skip directories
            continue
        text = extract_text_from_pdf(bucket, document)
        if text:
            pdf_link = f"https://{bucket}.s3.amazonaws.com/{document}"
            fields = extract_fields_with_gpt(text, pdf_link)
            if fields is not None:
                store_to_dynamodb(fields)
            else:
                print(f"Failed to extract fields or decode JSON for PDF: {pdf_link}")
        else:
            print(f"No text extracted from PDF: {pdf_link}")

if __name__ == "__main__":
    bucket_name = 'zoning-project'
    process_individual_items(bucket_name)