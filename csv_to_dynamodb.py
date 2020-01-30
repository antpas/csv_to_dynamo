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
    
    #DictReader is a generator; not stored in memory
    for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
        table.put_item(
           Item=row
        )
 
    return {
        'statusCode': 200,
        'body': json.dumps('Uploaded to DynamoDB Table')
    }
