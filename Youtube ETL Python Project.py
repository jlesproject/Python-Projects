# install pip3 - pip install google-api-python-client oauth2client boto3

import json
import os
from googleapiclient.discovery import build
import boto3
from botocore.exceptions import NoCredentialsError

# AWS S3 Configuration
AWS_ACCESS_KEY = 'your_access_key'
AWS_SECRET_KEY = 'your_secret_key'
S3_BUCKET_NAME = 'your_s3_bucket'
S3_PREFIX = 'youtube_data/'

# YouTube Data API Configuration
YOUTUBE_API_KEY = 'your_youtube_api_key'
YOUTUBE_CHANNEL_ID = 'your_youtube_channel_id'

def extract_data(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    response = youtube.search().list(part='snippet', channelId=channel_id, maxResults=5).execute()
    return response['items']

def transform_data(raw_data):
    # Simplified transformation, you can modify this based on your use case
    transformed_data = [{'title': item['snippet']['title'], 'published_at': item['snippet']['publishedAt']} for item in raw_data]
    return transformed_data

def load_data_to_s3(data, aws_access_key, aws_secret_key, bucket_name, prefix):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    for item in data:
        object_key = f"{prefix}{item['title'].replace(' ', '_')}.json"
        s3.put_object(Body=json.dumps(item), Bucket=bucket_name, Key=object_key)

def main():
    raw_data = extract_data(YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID)
    transformed_data = transform_data(raw_data)
    load_data_to_s3(transformed_data, AWS_ACCESS_KEY, AWS_SECRET_KEY, S3_BUCKET_NAME, S3_PREFIX)

if __name__ == "__main__":
    main()
