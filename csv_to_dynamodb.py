import json
import boto3
import os
import csv
import codecs
import sys

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

bucket = os.environ['bucket']
key = os.environ['key']
tableName = os.environ['table']

def lambda_handler(event, context):
    

    #get() does not store in memory
    obj = s3.Object(bucket, key).get()['Body']
    table = dynamodb.Table(tableName)

    batch_size = 100
    batch = []
    count = 0

    #DictReader is a generator; not stored in memory
    for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
        if count >= batch_size:
            write_to_dynamo(batch)
            batch = []
            count = 0

        batch.append(row)
        count += 1
    
    if batch:
        write_to_dynamo(batch)
 
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded to DynamoDB Table')
    }

    
def write_to_dynamo(row):
    table = dynamodb.Table(tableName)

    with table.batch_writer() as batch:
        for i in range(len(row)):
            batch.put_item(
                Item=row[i]
            )
    
