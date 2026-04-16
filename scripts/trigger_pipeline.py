"""
trigger_pipeline.py  —  scripts/
Triggers a Fabric Data Factory pipeline via REST API.
Polls run status until completion. Supports optional parameter injection.
"""

import argparse
import json
import logging
import os
import time

import msal
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=[FABRIC_SCOPE])
    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result.get('error_description')}")
    return result["access_token"]


def get_pipeline_id(workspace_id: str, pipeline_name: str, token: str) -> str:
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    for item in resp.json().get("value", []):
        if item["displayName"] == pipeline_name:
            return item["id"]
    raise ValueError(
        f"Pipeline '{pipeline_name}' not found in workspace {workspace_id}"
    )


def trigger_pipeline(
    workspace_id: str, pipeline_id: str, token: str, parameters: dict | None = None
) -> str:
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/jobs/instances?jobType=Pipeline"
    payload = {}
    if parameters:
        payload["executionData"] = {"parameters": parameters}

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
    )
    resp.raise_for_status()
    run_id = resp.headers.get("x-ms-operation-id") or resp.json().get("id")
    logger.info(f"Pipeline triggered. Run ID: {run_id}")
    return run_id


def poll_run_status(
    workspace_id: str, pipeline_id: str, run_id: str, token: str, timeout: int = 3600
) -> str:
    url = f"{FABRIC_API_BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/jobs/instances/{run_id}"
    headers = {"Authorization": f"Bearer {token}"}
    elapsed = 0
    poll_interval = 15

    while elapsed < timeout:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "Unknown")
        logger.info(f"[{elapsed}s] Run status: {status}")

        if status in {"Succeeded", "Completed"}:
            return "succeeded"
        elif status in {"Failed", "Cancelled"}:
            error = data.get("failureReason", "No details")
            raise RuntimeError(f"Pipeline run {status}: {error}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Pipeline run timed out after {timeout}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--pipeline", required=True, help="Pipeline display name")
    parser.add_argument(
        "--params", default=None, help="JSON string of pipeline parameters"
    )
    parser.add_argument("--wait", action="store_true", help="Wait for completion")
    args = parser.parse_args()

    token = get_token(
        os.environ["AZURE_TENANT_ID"],
        os.environ["AZURE_CLIENT_ID"],
        os.environ["AZURE_CLIENT_SECRET"],
    )

    parameters = json.loads(args.params) if args.params else None
    pipeline_id = get_pipeline_id(args.workspace_id, args.pipeline, token)
    run_id = trigger_pipeline(args.workspace_id, pipeline_id, token, parameters)

    if args.wait:
        result = poll_run_status(args.workspace_id, pipeline_id, run_id, token)
        logger.info(f"Pipeline finished with status: {result}")
    else:
        logger.info(f"Pipeline triggered (not waiting). Run ID: {run_id}")


if __name__ == "__main__":
    main()
