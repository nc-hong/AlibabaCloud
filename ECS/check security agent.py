#######################################################
 --access-key-id "XXXX" --access-key-secret "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY" --region cn-hongkong


#######################################################
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from typing import List, Dict, Any, Optional
import os

from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models

# Security Center (SAS) SDK
from alibabacloud_sas20181203.client import Client as SasClient
from alibabacloud_sas20181203 import models as sas_models

# ECS SDK
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models


def build_config(region_id: str, access_key_id: str, access_key_secret: str):
    """Create a Tea OpenAPI config for a given region and AK pair."""
    return open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id=region_id
    )


def create_sas_client(region_id: str, access_key_id: str, access_key_secret: str) -> SasClient:
    return SasClient(build_config(region_id, access_key_id, access_key_secret))


def create_ecs_client(region_id: str, access_key_id: str, access_key_secret: str) -> EcsClient:
    return EcsClient(build_config(region_id, access_key_id, access_key_secret))


def query_sas_assets(
    sas_client: SasClient,
    instance_id: Optional[str] = None,
    page_size: int = 100
) -> List[Dict[str, Any]]:
    """
    Call Security Center DescribeCloudCenterInstances to list ECS assets and their SAS client status.
    If instance_id is provided, it filters to that asset.
    """
    criteria = []
    if instance_id:
        # Filter by asset instance ID (ECS instance ID)
        criteria.append({"name": "assetInstanceId", "value": instance_id})

    request = sas_models.DescribeCloudCenterInstancesRequest(
        criteria=json.dumps(criteria) if criteria else None,
        machine_types="ecs",
        page_size=page_size,
        current_page=1,
        use_next_token=False,
        lang="en"
    )

    response = sas_client.describe_cloud_center_instances_with_options(
        request,
        util_models.RuntimeOptions()
    )

    if not response.body or not getattr(response.body, "instances", None):
        return []

    assets = []
    for item in response.body.instances or []:
        assets.append({
            "InstanceId": getattr(item, "instance_id", None) or getattr(item, "asset_instance_id", None),
            "Region": getattr(item, "region", None),
            "OsName": getattr(item, "os_name", None),
            "ClientStatus": getattr(item, "client_status", None),  # 'online' / 'offline' if agent present
            "InternetIp": getattr(item, "internet_ip", None),
            "VpcInstanceId": getattr(item, "vpc_instance_id", None),
            "Status": getattr(item, "status", None),
            # Some SAS responses include a name field; if present we’ll keep it:
            "SasName": getattr(item, "instance_name", None) or getattr(item, "asset_instance_name", None)
        })
    return assets


def query_cloud_assistant(
    ecs_client: EcsClient,
    region_id: str,
    instance_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    """
    Call ECS DescribeCloudAssistantStatus for the given instance IDs.
    Returns mapping: InstanceId -> {CloudAssistantStatus, CloudAssistantVersion, LastHeartbeatTime}
    """
    if not instance_ids:
        return {}

    req = ecs_models.DescribeCloudAssistantStatusRequest(
        region_id=region_id,
        instance_id=instance_ids  # up to 100 IDs
    )
    resp = ecs_client.describe_cloud_assistant_status_with_options(
        req,
        util_models.RuntimeOptions()
    )

    result = {}
    if resp.body and getattr(resp.body, "instance_cloud_assistant_status_set", None):
        statuses = resp.body.instance_cloud_assistant_status_set.instance_cloud_assistant_status
        for s in statuses or []:
            result[s.instance_id] = {
                "CloudAssistantStatus": s.cloud_assistant_status,   # 'true' or 'false'
                "CloudAssistantVersion": s.cloud_assistant_version, # empty if not installed/not running
                "LastHeartbeatTime": s.last_heartbeat_time
            }
    return result


def get_ecs_instance_names(
    ecs_client: EcsClient,
    region_id: str,
    instance_ids: List[str]
) -> Dict[str, str]:
    """
    Look up ECS instance names via DescribeInstances.
    Returns mapping: InstanceId -> InstanceName
    """
    if not instance_ids:
        return {}

    # DescribeInstances supports up to 100 InstanceIds per request
    # We’ll do a single batch here; add chunking if you pass more than 100.
    req = ecs_models.DescribeInstancesRequest(
        region_id=region_id,
        instance_ids=json.dumps(instance_ids)  # expects a JSON array string
    )
    resp = ecs_client.describe_instances_with_options(
        req,
        util_models.RuntimeOptions()
    )

    id_to_name: Dict[str, str] = {}
    if resp.body and getattr(resp.body, "instances", None) and getattr(resp.body.instances, "instance", None):
        for inst in resp.body.instances.instance or []:
            id_to_name[inst.instance_id] = inst.instance_name
    return id_to_name


def main():
    parser = argparse.ArgumentParser(
        description="Check whether Alibaba Cloud Security Center (SAS) agent is installed on ECS instances."
    )
    parser.add_argument("--region", required=True, help="Region ID, e.g., cn-hongkong")
    parser.add_argument("--access-key-id", required=False, help="AccessKey ID (or env ALIBABA_CLOUD_ACCESS_KEY_ID)")
    parser.add_argument("--access-key-secret", required=False, help="AccessKey Secret (or env ALIBABA_CLOUD_ACCESS_KEY_SECRET)")
    parser.add_argument("--instance-id", required=False, help="Optional ECS Instance ID to filter")
    parser.add_argument("--check-cloud-assistant", action="store_true", help="Also check Cloud Assistant Agent status")
    args = parser.parse_args()

    ak = args.access_key_id or os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    sk = args.access_key_secret or os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    if not ak or not sk:
        raise SystemExit("AccessKey not provided. Use --access-key-id/--access-key-secret or set env ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    sas_client = create_sas_client(args.region, ak, sk)
    ecs_client = create_ecs_client(args.region, ak, sk)

    assets = query_sas_assets(sas_client, instance_id=args.instance_id)
    if not assets:
        print("No ECS assets found in Security Center for the given criteria.")
        return

    # Map ECS instance IDs -> names
    ids = [a["InstanceId"] for a in assets if a.get("InstanceId")]
    id_to_name = get_ecs_instance_names(ecs_client, args.region, ids)

    # Optional Cloud Assistant status map
    ca_status_map = {}
    if args.check_cloud_assistant:
        ca_status_map = query_cloud_assistant(ecs_client, args.region, ids)

    print("ECS Security Center agent status:")
    print("--------------------------------------------------------------------------")
    for a in assets:
        iid = a.get("InstanceId")
        name = id_to_name.get(iid) or a.get("SasName") or "-"
        client_status = a.get("ClientStatus")  # 'online' / 'offline' if agent installed
        installed = "YES" if client_status in ("online", "offline") else "NO"
        detail = client_status if client_status else "not installed"
        line = (
            f"{iid} | name={name} | region={a.get('Region')} | os={a.get('OsName')} "
            f"| SAS agent installed={installed} ({detail})"
        )
        if args.check_cloud_assistant:
            ca = ca_status_map.get(iid, {})
            ca_flag = "installed" if str(ca.get("CloudAssistantStatus")).lower() == "true" else "not installed or not running"
            line += f" | CloudAssistant={ca_flag}"
        print(line)
    print("--------------------------------------------------------------------------")


if __name__ == "__main__":
    main()
