"""FinOps tools: seed floci, report costs, clean up idle resources.

Clients point at floci (http://localhost:4566, test/test, us-east-1) unless the env says otherwise.
Set AWS_ENDPOINT_URL to an empty string to use a real account through the normal boto3 credential chain.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3

MOCK_PATH = Path(__file__).resolve().parents[1] / "docs" / "contracts" / "finops_mock.json"
FLOCI = "http://localhost:4566"


def load_mock() -> dict:
    return json.loads(MOCK_PATH.read_text())


def client(service: str):
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    endpoint = os.environ.get("AWS_ENDPOINT_URL", FLOCI)
    if not endpoint:
        return boto3.client(service, region_name=region)
    return boto3.client(
        service,
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=region,
    )
