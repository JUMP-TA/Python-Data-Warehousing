import json
import boto3
import psycopg2
import csv

def lambda_handler(event, context):
    # Connect to S3
    s3 = boto3.client('s3')
    bucket_name = 'your-s3-bucket-name'
    
    # Connect to Redshift
    conn = psycopg2.connect(
        dbname='your-db-name',
        user='your-db-user',
        password='your-db-password',
        host='your-redshift-endpoint',
        port='5439'
    )
    cursor = conn.cursor()

    # Fetch the file from S3 (use event info)
    file_key = event['Records'][0]['s3']['object']['key']
    file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_data = file_obj['Body'].read().decode('utf-8').splitlines()

    # Parse CSV file
    reader = csv.reader(file_data)
    next(reader)  # Skip header
    
    # Insert data into Redshift - Separate the data into two tables
    for row in reader:
        # Insert into customers table
        cursor.execute("""
            INSERT INTO customers (customer_id, customer_name)
            VALUES (%s, %s)
            ON CONFLICT (customer_id) DO NOTHING;  -- Ensure no duplicates
        """, (row[0], row[1]))

        # Insert into orders table
        cursor.execute("""
            INSERT INTO orders (order_id, customer_id, order_amount, order_date)
            VALUES (%s, %s, %s, %s)
        """, (row[2], row[0], row[3], row[4]))

    conn.commit()
    cursor.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully loaded {file_key} into Redshift')
    }
