import os
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

def safe_geocode(geolocator, address, attempt=1, max_attempts=3):
    try:
        return geolocator.geocode(address, timeout=10)  # Increased timeout
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        if attempt <= max_attempts:
            print(f"Geocoding timeout, retrying {attempt}/{max_attempts}")
            return safe_geocode(geolocator, address, attempt + 1, max_attempts)
        else:
            print("Geocoding failed after max retries")
            return None


def extract_fields_with_gpt(text, pdf_link):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    instructions = (
        "Extract the following fields from the text and ensure the types are correct:\n"
        "Request Type (either SP Amendment, Zone Change, Preliminary SP, Final Plat, Concept Plan, or Major Plan Amendment), "
        "Applicant, Project Name, Council District (as an integer), School District (as an integer), Council Person, "
        "Requested by, Date (in YYYY-MM-DD format), Staff Recommendation Description, Staff Recommendation (as 'defer', 'approve', or 'conditional'), "
        "Existing Zoning Type ID (code only), Proposed Zoning Type ID (code only), Addresses (list of addresses without descriptors like 'unnumbered' - if the street number is not available, just put the street name), "
        "Location Description, Community Character Policy, Community Character Policy ID, Community Plan, "
        "Number of units proposed (as float), Number of acres (as float), Number of units currently (as float), "
        "Parcel ID (list of numbers only, remove any descriptors like 'Part of Parcel(s)'), Map ID (string), Summary (based on the analysis), "
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
        "Major Plan Amendment: A request to amend the Bordeaux-Whites Creek-Haynes Trinity Community Plan by changing the policy from Urban Neighborhood Evolving (T4 NE) to Urban Neighborhood Center (T4 NC) for properties located at the southwest corner of 110 Cliff Drive and Buena Vista Pike, zoned R8 (One and Two-Family Residential) (5.55 acres).\n"
        "2024Z-058PR-001\nMap 119-05, Parcel 292-294, part of 291\n11, South Nashville\n16 (Ginny Welsch)\n"
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
        "  \"Date\": \"2024-05-23\",\n"
        "  \"Staff Recommendation Description\": \"Staff recommends deferral to the June 13, 2024, Planning Commission meeting.\n"
        "  \"Staff Recommendation\": \"defer\",\n"
        "  \"Existing Zoning Type ID\": \"R8\",\n"
        "  \"Proposed Zoning Type ID\": \"T4 NC\",\n"
        "  \"Addresses\": [\"110 Cliff Drive\", \"Buena Vista Pike\"],\n"
        "  \"Location Description\": \"southwest corner of Cliff Drive and Buena Vista Pike, 5.55 acres\",\n"
        "  \"Community Character Policy\": \"Urban Neighborhood Evolving (T4 NE) to Urban Neighborhood Center (T4 NC)\",\n"
        "  \"Community Character Policy ID\": \"T4 NC\",\n"
        "  \"Community Plan\": \"Bordeaux-Whites Creek-Haynes Trinity Community Plan\",\n"
        "  \"Number of units proposed\": \"N/A\",\n"
        "  \"Number of acres\": \"5.55\",\n"
        "  \"Number of units currently\": \"N/A\",\n"
        "  \"Parcel ID\": [\"291\", \"292\", \"293\", \"294\"],\n"
        "  \"Map ID\": \"119-05\",\n"
        "  \"Summary\": \"The application proposes a multi-family residential development along the corridor and provides improved pedestrian facilities along Central Pike and a portion of South New Hope Road. The plan includes some characteristics of T3 CM policy areas such as providing housing along a corridor and framing the Central Pike Corridor with the northernmost building, which is encouraged by the policy. Absent the building along Central Pike, the plan fails to meet the goals and is not consistent with the Central Pike Supplemental Policy Area (SPA).\"\n"
        "}\n\n"
        "Input Text:\n"
        f"{text}\n\n"
        "Extracted Fields:\n"
    )
    retry_count = 3
    for attempt in range(retry_count):
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
            temperature=0,
        )
        response_text = response.choices[0].message.content.strip()
        print("Response JSON:", response_text)  # Debugging line to inspect the JSON before parsing

        if response_text.startswith("```") and response_text.endswith("```"):
            response_text = response_text[3:-3].strip()

        try:
            extracted_fields = json.loads(response_text)
            # Convert 'N/A' and empty strings to None
            for key, value in extracted_fields.items():
                if value == 'N/A' or value == '':
                    extracted_fields[key] = None

            # Convert 'Council District' to integer, set to None if not possible
            # Convert 'Council District' to integer, set to None if not possible
            try:
                if isinstance(extracted_fields['Council District'], (str, int)):
                    extracted_fields['Council District'] = int(extracted_fields['Council District'])
                else:
                    extracted_fields['Council District'] = None
            except (ValueError, TypeError, Exception):
                extracted_fields['Council District'] = None

            # Convert 'School District' to integer, set to None if not possible
            try:
                if isinstance(extracted_fields['School District'], (str, int)):
                    extracted_fields['School District'] = int(extracted_fields['School District'])
                else:
                    extracted_fields['School District'] = None
            except (ValueError, TypeError, Exception):
                extracted_fields['School District'] = None

            # Ensure numeric fields are converted to float
            numeric_fields = ['Number of units proposed', 'Number of acres', 'Number of units currently']
            for field in numeric_fields:
                try:
                    if extracted_fields[field] is not None:
                        extracted_fields[field] = float(extracted_fields[field])
                except (ValueError, TypeError, Exception):
                    extracted_fields[field] = None

            extracted_fields['pdf_link'] = pdf_link
            print("PDF link:", pdf_link)

            geolocator = Nominatim(user_agent="zoning-application")
            addresses = extracted_fields.get("Addresses", [])
            if addresses:
                # Assuming the first address is the primary one for simplicity
                primary_address = addresses[0] + ", Nashville, TN"
                location = safe_geocode(geolocator, primary_address)
                if location:
                    print("Location:", location)
                    extracted_fields['latitude'] = location.latitude
                    extracted_fields['longitude'] = location.longitude
                else:
                    extracted_fields['latitude'] = None
                    extracted_fields['longitude'] = None
            else:
                extracted_fields['latitude'] = None
                extracted_fields['longitude'] = None

            return extracted_fields
        except json.JSONDecodeError as e:
            print(f"Attempt {attempt + 1} failed: JSON decoding error:", e)
            print("Faulty JSON content:", response_text)
            if attempt == retry_count - 1:
                print("Max retries reached. Failed to decode JSON.")
                return None

    return None


def process_individual_items(bucket, start_index=0):
    s3 = boto3.client('s3')
    prefix = 'nashville/staff-reports-individual-pdfs/'

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

        text = extract_text_from_pdf(bucket, document)
        if text:
            pdf_link = f"https://{bucket}.s3.amazonaws.com/{document}"
            print(f"Document Index: {counter}")
            fields = extract_fields_with_gpt(text, pdf_link)
            if fields is not None:
                store_to_dynamodb(fields)
            else:
                print(f"Failed to extract fields or decode JSON for PDF: {pdf_link}")
        else:
            print(f"No text extracted from PDF: {pdf_link}")

        counter += 1

if __name__ == "__main__":
    bucket_name = 'zoning-project'
    start_index = 0  # Start from the 632nd item
    process_individual_items(bucket_name, start_index)

