"""AWS credentials management."""

import os


def get_storage_options() -> dict:
    """Build storage options from environment variables."""
    # Support both AWS_ACCESS_KEY and AWS_ACCESS_KEY_ID
    access_key = os.environ.get("AWS_ACCESS_KEY") or os.environ["AWS_ACCESS_KEY_ID"]
    
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
