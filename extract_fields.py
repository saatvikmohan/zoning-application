import os
import boto3
import json
from openai import OpenAI
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from decimal import Decimal
from dotenv import load_dotenv
import concurrent.futures

import os
import boto3
import json
from dotenv import load_dotenv
from textextract import start_document_text_detection, get_document_text_detection
from decimal import Decimal
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from datetime import datetime
import json
from PIL import Image
import pdf2image
import io
import csv


# Load environment variables from .env file
load_dotenv()

def pdf_to_image(pdf_body):
    """ Convert PDF file to images """
    images = pdf2image.convert_from_bytes(pdf_body.read())
    return images

def store_to_dynamodb(fields):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('test2')
    # table = dynamodb.Table('nashville-zoning-db')
    
    # Convert float values to Decimal
    for key, value in fields.items():
        if isinstance(value, float):
            fields[key] = Decimal(str(value))
    
    # Create a unique partition key by combining Project Name and Date
    partition_key = f"{fields['Project Name']}_{fields['Date']}"
    fields['id'] = partition_key
    
    table.put_item(Item=fields)


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


import base64
import io
import os
import json

def extract_fields_with_gpt(images, pdf_link, counter):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    instructions = (
        "Extract the following fields from the image and output in JSON format.\n"
        "Request Type (either SP Amendment, Zone Change, Preliminary SP, Final Plat, Concept Plan, or Major Plan Amendment), "
        "Applicant, Project Name, Council District (as an integer), School District (as an integer), Council Person, School District Representative, "
        "Requested by, Date (in YYYY-MM-DD format), Staff Recommendation Description, Staff Recommendation (as 'defer', 'approve', or 'conditional'), "
        "Existing Zoning Type ID (list of codes only), Proposed Zoning Type ID (list of codes only), Addresses (list of addresses without descriptors like 'unnumbered' - if the street number is not available, just put the street name), "
        "Location Description, Community Character Policy, Community Character Policy ID (list of codes only), Community Plan, "
        "Number of units proposed (as float), Number of acres (as float), Number of units currently (as float), "
        "Parcel ID (list of numbers that are usually 3 digits, remove any descriptors like 'Part of Parcel(s)'), Map ID (list of strings - usually numbers separated by a dash), Summary (based on the analysis. Keep it brief - under 50 words). Note: Parcel ID and Map ID are usually found in a format like \"Map 069-12, Parcel 048\"\n"
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
        "  \"School District Representative\": \"Gentry\",\n"
        "  \"Requested by\": \"Metro Planning Department\",\n"
        "  \"Date\": \"2024-05-23\",\n"
        "  \"Staff Recommendation Description\": \"Staff recommends deferral to the June 13, 2024, Planning Commission meeting.\n"
        "  \"Staff Recommendation\": \"defer\",\n"
        "  \"Existing Zoning Type ID\": [\"R8\"],\n"
        "  \"Proposed Zoning Type ID\": [\"T4 NC\"],\n"
        "  \"Addresses\": [\"110 Cliff Drive\", \"Buena Vista Pike\"],\n"
        "  \"Location Description\": \"southwest corner of Cliff Drive and Buena Vista Pike, 5.55 acres\",\n"
        "  \"Community Character Policy\": \"Urban Neighborhood Evolving (T4 NE) to Urban Neighborhood Center (T4 NC)\",\n"
        "  \"Community Character Policy ID\": [\"T4 NC\"],\n"
        "  \"Community Plan\": \"Bordeaux-Whites Creek-Haynes Trinity Community Plan\",\n"
        "  \"Number of units proposed\": \"N/A\",\n"
        "  \"Number of acres\": \"5.55\",\n"
        "  \"Number of units currently\": \"N/A\",\n"
        "  \"Parcel ID\": [\"291\", \"292\", \"293\", \"294\"],\n"
        "  \"Map ID\": [\"119-05\"],\n"
        "  \"Summary\": \"The application proposes a multi-family residential development along the corridor and provides improved pedestrian facilities along Central Pike and a portion of South New Hope Road.\"\n"
        "}\n"
    )

    retry_count = 3
    for attempt in range(retry_count):
        # Convert images to base64 strings
        image_data = []
        for image in images:
            buffered = io.BytesIO()
            try:
                # Ensure the image is in JPEG format
                if image.format not in ['JPEG', 'PNG', 'GIF', 'WEBP']:
                    image = image.convert('RGB')
                    image.save(buffered, format="JPEG")
                else:
                    image.save(buffered, format=image.format)

                # Check image size
                if buffered.tell() > 20 * 1024 * 1024:  # 20 MB
                    print("Image size exceeds 20 MB, skipping this image.")
                    continue

                img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')
                image_data.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{img_str}"
                    }
                })
            finally:
                buffered.close()

        if not image_data:
            print("No valid images to process.")
            return None

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant designed to output JSON."},
                    {"role": "user", "content": instructions},
                    {"role": "user", "content": image_data, "detail": "low"}
                ],
                response_format={"type": "json_object"},
                max_tokens=500,
            )

            response_text = response.choices[0].message.content
            print("Response JSON:", response_text)  # Debugging line to inspect the JSON before parsing

            # Truncate the summary if it's too long
            max_summary_length = 300  # Set the maximum length for the summary
            if 'Summary' in response_text:
                summary_start = response_text.find('"Summary":')
                if summary_start != -1:
                    summary_start += len('"Summary":')
                    summary_end = response_text.find('}', summary_start)
                    if summary_end != -1:
                        summary_content = response_text[summary_start:summary_end]
                        if len(summary_content) > max_summary_length:
                            truncated_summary = summary_content[:max_summary_length] + '...'
                            response_text = response_text[:summary_start] + truncated_summary + response_text[summary_end:]

            try:
                extracted_fields = json.loads(response_text)
                # Remove double quotes around string values if they exist on both sides
                for key, value in extracted_fields.items():
                    if isinstance(value, str) and value.startswith('"') and value.endswith('"'):
                        extracted_fields[key] = value[1:-1]
                # Convert 'N/A' and empty strings to None
                for key, value in extracted_fields.items():
                    if (value == 'N/A' or value == '' or value == 'null' or value == 'None' or 
                        value == ["N/A"] or value == [] or 
                        (isinstance(value, str) and value.strip() == '') or 
                        (isinstance(value, list) and all(v == 'N/A' for v in value))):
                        extracted_fields[key] = None

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
                extracted_fields['Document number'] = counter
                print("PDF link:", pdf_link)

                return extracted_fields
            except json.JSONDecodeError as e:
                print(f"Attempt {attempt + 1} failed: JSON decoding error:", e)
                print("Faulty JSON content:", response_text)
                if attempt == retry_count - 1:
                    print("Max retries reached. Failed to decode JSON.")
                    return None

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: Invalid request error:", e)
            if attempt == retry_count - 1:
                print("Max retries reached. Invalid request error.")
                return None

    return None

import csv

def process_pdf_files(bucket, start_index=0):
    s3 = boto3.client('s3', region_name='us-west-2')
    prefix = 'nashville/staff-reports-individual-pdfs/'
    continuation_token = None
    counter = start_index

    # Load already processed links from already_done.csv
    already_done = set()
    try:
        with open('already_done.csv', mode='r') as file:
            reader = csv.reader(file)
            already_done = {rows[0] for rows in reader}
    except FileNotFoundError:
        print("already_done.csv not found. Proceeding without it.")

    while True:
        list_objects_params = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            list_objects_params['ContinuationToken'] = continuation_token

        response = s3.list_objects_v2(**list_objects_params)
        if 'Contents' not in response:
            print(f"No files found in {bucket}/{prefix}")
            break

        for obj in response['Contents']:
            if counter < start_index:
                counter += 1
                continue

            document = obj['Key']
            if document.endswith('/'):
                continue

            pdf_link = f"https://{bucket}.s3.amazonaws.com/{document}"
            if pdf_link in already_done:
                print(f"Skipping already processed PDF: {pdf_link}")
                counter += 1
                continue

            pdf_obj = s3.get_object(Bucket=bucket, Key=document)
            images = pdf_to_image(pdf_obj['Body'])
            fields = extract_fields_with_gpt(images, pdf_link=pdf_link, counter=counter)
            if fields:
                store_to_dynamodb(fields)
                # Add the processed link to already_done.csv
                with open('already_done.csv', mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([pdf_link])
            else:
                print(f"Failed to extract fields for PDF: {document}")

            counter += 1
            print(f"Processed {counter} PDF files")

        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break


if __name__ == "__main__":
    bucket_name = 'zoning-project'
    start_index = 0
    process_pdf_files(bucket_name, start_index)

