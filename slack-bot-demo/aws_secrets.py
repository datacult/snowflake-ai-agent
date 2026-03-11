import os
import json
import boto3
from botocore.exceptions import ClientError


def load_secrets(secret_name: str, region: str = "us-east-1") -> None:
    """
    Fetch a JSON secret from AWS Secrets Manager and load every
    key/value pair into os.environ. Falls back to .env (via dotenv)
    if the AWS call fails — useful for local development.
    """
    try:
        client = boto3.client("secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response["SecretString"])
        os.environ.update(secrets)
        print(f"Secrets loaded from AWS Secrets Manager: {secret_name}")
    except ClientError as e:
        print(f"Could not load secrets from AWS ({e.response['Error']['Code']}), falling back to .env")
        from dotenv import load_dotenv
        load_dotenv()
