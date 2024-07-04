from dotenv import load_dotenv
load_dotenv()

import boto3

def find_object_index(bucket, prefix, target_filename):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    index = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'] == f"{prefix}{target_filename}":
                    return index
                index += 1

    return -1  # Return -1 if the file is not found

# Usage
bucket_name = 'zoning-project'
prefix = 'nashville/staff-reports-individual-pdfs/'
target_filename = '190328sr.pdf_item_21.pdf'
# target_filename = 'StaffReport041323.pdf_item_24.pdf'
file_index = find_object_index(bucket_name, prefix, target_filename)

if file_index != -1:
    print(f"Index of the file '{target_filename}': {file_index}")
else:
    print(f"File '{target_filename}' not found.")