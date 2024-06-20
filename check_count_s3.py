from dotenv import load_dotenv
load_dotenv()

import boto3



import boto3

def count_objects_in_prefix(bucket, prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    total_objects = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            total_objects += len(page['Contents'])

    return total_objects

# Usage
bucket_name = 'zoning-project'
prefix = 'nashville/staff-reports-individual-txt-files/'
number_of_objects = count_objects_in_prefix(bucket_name, prefix)
print(f"Number of objects: {number_of_objects}")