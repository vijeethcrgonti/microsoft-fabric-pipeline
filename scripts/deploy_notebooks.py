"""
deploy_notebooks.py  —  scripts/
Deploys PySpark notebooks to Microsoft Fabric workspace via Fabric REST API.
Supports create (first deploy) and update (subsequent deploys) operations.
Uses Service Principal authentication via MSAL.
"""

import argparse
import base64
import json
import logging
import os
import time
from pathlib import Path

import msal
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

NOTEBOOKS = [
    {"name": "bronze_to_silver", "path": "notebooks/bronze_to_silver.py"},
    {"name": "silver_to_gold", "path": "notebooks/silver_to_gold.py"},
    {"name": "utils", "path": "notebooks/utils.py"},
]


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=[FABRIC_SCOPE])
    if "access_token" not in result:
        raise RuntimeError(f"MSAL auth failed: {result.get('error_description')}")
    return result["access_token"]


def list_existing_notebooks(workspace_id: str, token: str) -> dict[str, str]:
    """Return {display_name: item_id} for existing notebooks in workspace."""
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return {item["displayName"]: item["id"] for item in resp.json().get("value", [])}


def encode_notebook(file_path: str) -> str:
    """Read notebook file and base64-encode for Fabric API payload."""
    content = Path(file_path).read_text(encoding="utf-8")
    # Wrap Python script as a Fabric notebook (ipynb-compatible)
    notebook_json = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "PySpark",
                "language": "python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
        },
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": content.splitlines(keepends=True),
            }
        ],
    }
    return base64.b64encode(json.dumps(notebook_json).encode()).decode()


def create_notebook(
    workspace_id: str, name: str, encoded_content: str, token: str
) -> str:
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": encoded_content,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()

    # Fabric API returns 202 Accepted with operation URL for long-running ops
    if resp.status_code == 202:
        operation_url = resp.headers.get("Location") or resp.headers.get(
            "x-ms-operation-id"
        )
        logger.info(f"Create operation in progress: {operation_url}")
        item_id = poll_operation(operation_url, token)
    else:
        item_id = resp.json().get("id", "")

    logger.info(f"Created notebook '{name}' → {item_id}")
    return item_id


def update_notebook(
    workspace_id: str, item_id: str, name: str, encoded_content: str, token: str
):
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/notebooks/{item_id}/updateDefinition"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": encoded_content,
                    "payloadType": "InlineBase64",
                }
            ],
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    logger.info(f"Updated notebook '{name}'")


def poll_operation(operation_url: str, token: str, max_wait: int = 120) -> str:
    """Poll a Fabric long-running operation until completion."""
    headers = {"Authorization": f"Bearer {token}"}
    elapsed = 0
    while elapsed < max_wait:
        resp = requests.get(operation_url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "").lower()
        if status == "succeeded":
            return data.get("createdItemId", "")
        elif status in {"failed", "cancelled"}:
            raise RuntimeError(f"Operation failed: {data}")
        logger.info(f"Operation status: {status}... waiting")
        time.sleep(5)
        elapsed += 5
    raise TimeoutError(f"Operation timed out after {max_wait}s")


def deploy_all(workspace_id: str, token: str, force_update: bool = False):
    existing = list_existing_notebooks(workspace_id, token)
    logger.info(f"Found {len(existing)} existing notebooks in workspace")

    for nb in NOTEBOOKS:
        name = nb["name"]
        path = nb["path"]

        if not Path(path).exists():
            logger.warning(f"Notebook file not found: {path} — skipping")
            continue

        encoded = encode_notebook(path)

        if name in existing and not force_update:
            logger.info(
                f"Notebook '{name}' already exists — skipping (use --force to update)"
            )
        elif name in existing:
            update_notebook(workspace_id, existing[name], name, encoded, token)
        else:
            create_notebook(workspace_id, name, encoded, token)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument(
        "--force", action="store_true", help="Force update existing notebooks"
    )
    args = parser.parse_args()

    tenant_id = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]

    token = get_access_token(tenant_id, client_id, client_secret)
    deploy_all(args.workspace_id, token, force_update=args.force)
    logger.info("Deployment complete")


if __name__ == "__main__":
    main()
