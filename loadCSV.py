import json
import boto3
import csv
import uuid

def write_to_dynamobd(file_path, file_name):
    dynamodb = boto3.resource("dynamodb")
    
    # create dynamodb table
    table = dynamodb.create_table(
        TableName = file_name,
        KeySchema = [{
            "AttributeName": "id", 
            "KeyType": "HASH"
        }],
        AttributeDefinitions = [{
            "AttributeName": "id",
            "AttributeType": "S"
        }],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    
    # Wait until the table exists
    table.meta.client.get_waiter('table_exists').wait(TableName=file_name)
    
    # iterate through csv file
    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file)
        col_names = None
        first_row = True
        
        for row in csv_reader:
            # first row contains column names
            if first_row:
                col_names = row
                print(col_names)
                first_row = False
                
            else:
                # create object to be commited to the table
                new_item = {"id": str(uuid.uuid4())}
                
                for i in range(0, len(col_names)):
                    new_item[col_names[i]] = row[i]
                
                # add item to table
                table.put_item(Item = new_item)
    return None
    

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    
    # get file info
    record = event["Records"][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    
    # download csv file locally
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
    s3.download_file(bucket, key, download_path)
    
    write_to_dynamobd(download_path, key)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Done writing CSV to DynamoDB')
    }