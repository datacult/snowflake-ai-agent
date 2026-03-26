import os
import time
import boto3
from botocore.exceptions import ClientError

_TABLE_NAME  = 'SlackBotThreadStore'
_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days
_MAX_MESSAGES = 20                # cap at 20 messages (~10 exchanges)

_dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
_table    = _dynamodb.Table(_TABLE_NAME)


def get_history(thread_ts: str) -> list:
    """Return the conversation message history for a Slack thread, or [] if not found."""
    try:
        resp = _table.get_item(Key={'thread_ts': thread_ts})
        return resp.get('Item', {}).get('messages', [])
    except ClientError as e:
        print(f"DynamoDB get error: {e}")
        return []


def save_history(thread_ts: str, messages: list) -> None:
    """Persist updated conversation history with a 7-day TTL."""
    try:
        _table.put_item(Item={
            'thread_ts':  thread_ts,
            'messages':   messages[-_MAX_MESSAGES:],
            'expires_at': int(time.time()) + _TTL_SECONDS,
        })
    except ClientError as e:
        print(f"DynamoDB put error: {e}")
