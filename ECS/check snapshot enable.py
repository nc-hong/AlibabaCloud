#############################################
#snapshot.py --region=cn-hangzhou
#
#############################################
import os
import time
import argparse
from typing import List, Dict

from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_tea_util.client import Client as TeaUtilClient


def make_ecs_client(region_id: str) -> EcsClient:
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    security_token = os.getenv("ALIBABA_CLOUD_SECURITY_TOKEN")  # optional (STS)

    if not access_key_id or not access_key_secret:
        raise RuntimeError(
            "Missing credentials: ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET env vars are required."
        )

    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
    )
    config.region_id = region_id
    # Explicit endpoint helps avoid RegionId inference issues
    config.endpoint = f"ecs.{region_id}.aliyuncs.com"

    if security_token:
        config.type = "sts"
        config.security_token = security_token

    return EcsClient(config)


def smoke_check_regions(ecs: EcsClient, region_id: str, debug: bool):
    """
    Quick check that the client works and that the target region exists.
    DescribeRegionsRequest takes no args in the ECS 20140526 Tea SDK.
    """
    try:
        req = ecs_models.DescribeRegionsRequest()  # <-- no region_id here
        resp = ecs.describe_regions(req)
        data = TeaUtilClient.to_map(resp.body)
        regions = data.get("Regions", {}).get("Region", []) or []
        available = {r.get("RegionId") for r in regions if r.get("RegionId")}
        print("=== Smoke test: DescribeRegions ===", flush=True)
        print(f"- Target region: {region_id}", flush=True)
        print(f"- Regions returned: {len(regions)}", flush=True)
        if debug:
            for r in regions[:10]:
                print(f"  • {r.get('RegionId')} ({r.get('LocalName')})", flush=True)
        if region_id not in available:
            print(f"⚠️ Target region '{region_id}' not found in DescribeRegions result. "
                  f"Proceeding, but double-check region spelling and your account permissions.", flush=True)
        print("=== End smoke test ===\n", flush=True)
    except Exception as e:
        print("❌ Smoke test failed: DescribeRegions error:", flush=True)
        print(str(e), flush=True)
        raise


def check_region_has_policies(ecs: EcsClient, region_id: str, debug: bool) -> List[Dict]:
    page_number = 1
    page_size = 50
    policies_all = []

    while True:
        req = ecs_models.DescribeAutoSnapshotPolicyExRequest(
            region_id=region_id,      # explicit
            page_number=page_number,
            page_size=page_size,
        )
        resp = ecs.describe_auto_snapshot_policy_ex(req)
        data = TeaUtilClient.to_map(resp.body)
        if debug:
            print(f"[DEBUG] DescribeAutoSnapshotPolicyEx page {page_number}: total={data.get('TotalCount')}", flush=True)
        policies = data.get("AutoSnapshotPolicies", {}).get("AutoSnapshotPolicy", []) or []
        for p in policies:
            policies_all.append({
                "AutoSnapshotPolicyId": p.get("AutoSnapshotPolicyId"),
                "RegionId": p.get("RegionId"),
                "PolicyName": p.get("PolicyName"),
                "RetentionDays": p.get("RetentionDays"),
                "DiskNums": p.get("DiskNums"),
                "RepeatWeekdays": p.get("RepeatWeekdays"),
                "TimePoints": p.get("TimePoints"),
                "EnableCrossRegionCopy": p.get("EnableCrossRegionCopy"),
                "TargetCopyRegions": p.get("TargetCopyRegions"),
                "Status": p.get("Status"),
            })

        total = data.get("TotalCount", 0)
        if page_number * page_size >= total or not policies:
            break
        page_number += 1
        time.sleep(0.05)

    return policies_all


def list_all_instances(ecs: EcsClient, region_id: str, debug: bool) -> List[Dict]:
    page_number = 1
    page_size = 100
    all_instances = []

    while True:
        req = ecs_models.DescribeInstancesRequest(
            region_id=region_id,      # explicit
            page_number=page_number,
            page_size=page_size,
        )
        resp = ecs.describe_instances(req)
        data = TeaUtilClient.to_map(resp.body)
        total = data.get("TotalCount", 0)
        instances = data.get("Instances", {}).get("Instance", []) or []
        if debug:
            print(f"[DEBUG] DescribeInstances page {page_number}: total={total}, returned={len(instances)}", flush=True)

        for inst in instances:
            all_instances.append({
                "InstanceId": inst.get("InstanceId"),
                "InstanceName": inst.get("InstanceName"),
                "Status": inst.get("Status"),
                "ZoneId": inst.get("ZoneId"),
                "RegionId": inst.get("RegionId"),
                "VpcId": inst.get("VpcId"),
                "VSwitchId": inst.get("VSwitchId"),
            })

        if page_number * page_size >= total or not instances:
            break
        page_number += 1
        time.sleep(0.05)

    return all_instances


def list_instance_disks_with_policy(ecs: EcsClient, region_id: str, instance_id: str, debug: bool) -> List[Dict]:
    page_number = 1
    page_size = 100
    disks_all = []

    while True:
        req = ecs_models.DescribeDisksRequest(
            region_id=region_id,      # explicit
            instance_id=instance_id,
            page_number=page_number,
            page_size=page_size,
        )
        resp = ecs.describe_disks(req)
        data = TeaUtilClient.to_map(resp.body)
        total = data.get("TotalCount", 0)
        disks = data.get("Disks", {}).get("Disk", []) or []
        if debug:
            print(f"[DEBUG] DescribeDisks page {page_number} for {instance_id}: total={total}, returned={len(disks)}", flush=True)

        for d in disks:
            disks_all.append({
                "DiskId": d.get("DiskId"),
                "Type": d.get("Type"),
                "Category": d.get("Category"),
                "Size": d.get("Size"),
                "Status": d.get("Status"),
                "AutoSnapshotPolicyId": d.get("AutoSnapshotPolicyId") or "",
            })

        if page_number * page_size >= total or not disks:
            break
        page_number += 1
        time.sleep(0.05)

    return disks_all


def check_all_ecs(region_id: str, only_without_policy: bool = False, debug: bool = False):
    print(f"=== Starting ECS snapshot policy check: region={region_id}, only_without_policy={only_without_policy}, debug={debug} ===", flush=True)
    try:
        ecs = make_ecs_client(region_id)
    except Exception as e:
        print("❌ Failed to create ECS client:", flush=True)
        print(str(e), flush=True)
        return

    # Smoke test
    try:
        smoke_check_regions(ecs, region_id, debug)
    except Exception:
        print("Aborting due to smoke test failure.", flush=True)
        return

    # Region policies
    try:
        policies = check_region_has_policies(ecs, region_id, debug)
        print(f"=== Region: {region_id} — Auto Snapshot Policies ({len(policies)}) ===", flush=True)
        if not policies:
            print("No auto snapshot policies found in this region.", flush=True)
        else:
            for p in policies:
                print(f"- {p['PolicyName']} (ID: {p['AutoSnapshotPolicyId']}, Status: {p.get('Status')}, "
                      f"RetentionDays: {p.get('RetentionDays')}, Disks bound: {p.get('DiskNums')})", flush=True)
    except Exception as e:
        print("❌ Failed to list auto snapshot policies:", flush=True)
        print(str(e), flush=True)

    # Instances
    try:
        instances = list_all_instances(ecs, region_id, debug)
    except Exception as e:
        print("❌ Failed to list instances:", flush=True)
        print(str(e), flush=True)
        return

    print(f"\n=== Found {len(instances)} ECS instance(s) in {region_id} ===", flush=True)
    if not instances:
        print("No ECS instances found. Ensure you selected the correct region and your credentials have access.", flush=True)
        print("=== Completed ===", flush=True)
        return

    # Per instance: check disks and bindings
    instances_with_policy = 0
    instances_without_policy = 0
    instances_no_disks = 0

    for inst in instances:
        instance_id = inst["InstanceId"]
        instance_name = inst.get("InstanceName") or ""
        try:
            disks = list_instance_disks_with_policy(ecs, region_id, instance_id, debug)
        except Exception as e:
            print(f"⚠️  Instance {instance_id} ({instance_name}) — error listing disks: {e}", flush=True)
            continue

        if not disks:
            instances_no_disks += 1
            if not only_without_policy:
                print(f"- Instance {instance_id} ({instance_name}) — no disks found.", flush=True)
            continue

        any_bound = any(d.get("AutoSnapshotPolicyId") for d in disks)
        if any_bound:
            instances_with_policy += 1
            if not only_without_policy:
                print(f"- Instance {instance_id} ({instance_name}) — ✅ at least one disk bound to a policy:", flush=True)
                for d in disks:
                    policy = d.get("AutoSnapshotPolicyId") or "(none)"
                    print(f"    • Disk {d['DiskId']} | Type: {d['Type']} | Category: {d['Category']} | "
                          f"Size: {d['Size']} GiB | Policy: {policy}", flush=True)
        else:
            instances_without_policy += 1
            print(f"- Instance {instance_id} ({instance_name}) — ⚠️ no disks bound to any auto snapshot policy:", flush=True)
            for d in disks:
                print(f"    • Disk {d['DiskId']} | Type: {d['Type']} | Category: {d['Category']} | "
                      f"Size: {d['Size']} GiB | Policy: (none)", flush=True)

    # Summary
    print("\n=== Summary ===", flush=True)
    print(f"Total instances:             {len(instances)}", flush=True)
    print(f"Instances with policy:       {instances_with_policy}", flush=True)
    print(f"Instances without policy:    {instances_without_policy}", flush=True)
    print(f"Instances with no disks:     {instances_no_disks}", flush=True)
    print("=== Completed ===", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check all ECS instances for Auto Snapshot Policy bindings (per disk)."
    )
    parser.add_argument("--region", default=os.getenv("ALICLOUD_REGION_ID", "cn-hongkong"),
                        help="Region ID, e.g. cn-hongkong")
    parser.add_argument("--only-without-policy", default=os.getenv("ONLY_WITHOUT_POLICY", "false"),
                        help="true/false: only display instances that have no disk bound to any policy")
    parser.add_argument("--debug", default=os.getenv("DEBUG", "false"),
                        help="true/false: show verbose API call info")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    only_without_policy = str(args.only_without_policy).lower().strip() in ("1", "true", "yes")
    debug = str(args.debug).lower().strip() in ("1", "true", "yes")
    check_all_ecs(region_id=args.region, only_without_policy=only_without_policy, debug=debug)
