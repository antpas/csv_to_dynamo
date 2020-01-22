import json
import boto3
import os
import csv

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

bucket = os.environ['bucket']
key = os.environ['key']
tableName = os.environ['table']

def lambda_handler(event, context):
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    rows = obj['Body'].read().decode("utf-8").split('\n')
    headers = rows[0].split(',')
    rows.pop(0)
    table = dynamodb.Table(tableName)
    
    row_data = []
    entry = {}
    count = 0
    for line in rows:
        row_data.append(line.split(','))
        j = 0
        row_data[count][j] = int(row_data[count][j])
        for k in headers:
            entry[k] = row_data[count][j]
            j = j + 1
        count = count + 1
        table.put_item(
           Item=entry
        )
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded to DynamoDB Table')
    }
