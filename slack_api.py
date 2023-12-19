import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
secret_name = ""
region_name = ""
session = boto3.session.Session()
client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # Retrieve AWS S3 credentials
try:
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
except ClientError as e:
    raise e

aws_secret_s3 = get_secret_value_response['SecretString']
secret_data_s3 = json.loads(aws_secret_s3)
SLACK_TOKEN = secret_data_s3['your_slack_api_token']
# Set your Slack API token

def send_slack_message(message):
    # Set the channel where you want to send the message
    CHANNEL_ID = "your_channel_id"

    # Initialize Slack WebClient
    client = WebClient(token=SLACK_TOKEN)

    # Send a message to the specified channel
    try:
        response = client.chat_postMessage(
            channel=CHANNEL_ID,
            text=message
        )
        print("Message sent successfully:", response["ts"])
    except SlackApiError as e:
        print("Error sending message:", e.response["error"])

