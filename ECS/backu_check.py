
#!/usr/bin/env python3
"""
abc.py — Alibaba Cloud ECS snapshot audit (no STS)
- Uses a single AK/SK directly
- Lists snapshots in the last N hours across specified regions
- Saves a local JSON report
- Includes size info and verification status
- Instance attachment enrichment:
    * Per snapshot fields:
        - instance_id (first attached instance, if any)
        - instance_name (name of the first attached instance)
        - attached_instance_ids (list of all attached instance IDs)
        - attached_instance_names (list of all attached instance names)
"""

import os
import json
import datetime
import argparse
from typing import List, Dict, Any, Optional

# Alibaba Cloud SDK imports
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models

# --- CONFIGURATION ---
MASTER_ACCESS_KEY_ID = os.getenv("MASTER_ACCESS_KEY_ID", "YOUR_MASTER_AK_ID")
MASTER_ACCESS_KEY_SECRET = os.getenv("MASTER_ACCESS_KEY_SECRET", "YOUR_MASTER_AK_SECRET")

# Regions to audit (same account). Add/remove as needed or override via CLI.
REGIONS = [
    "cn-hangzhou",
    "cn-shanghai",
]

# Default lookback hours (can be overridden via CLI)
DEFAULT_LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))

# ---------------------- Core Functions ---------------------- #

def make_ecs_client(region_id: str) -> EcsClient:
    """
    Creates an ECS client for the given region using the direct AK/SK.
    Ensure region_id is present in config (required by API).
    """
    config = open_api_models.Config(
        access_key_id=MASTER_ACCESS_KEY_ID,
        access_key_secret=MASTER_ACCESS_KEY_SECRET,
        region_id=region_id,                               # required
        endpoint=f"ecs.{region_id}.aliyuncs.com",
    )
    return EcsClient(config)


def _parse_int(value: Optional[str]) -> Optional[int]:
    """
    Try to parse int from string or pass-through int; return None if not parseable.
    Alibaba APIs sometimes return numeric fields as strings in some SDKs/regions.
    """
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return value if isinstance(value, int) else None


# ---- Attachment + Instance-name helpers (with caches) ----

_disk_attach_cache: Dict[str, List[str]] = {}      # key: (region, disk_id) -> [instance_ids]
_instance_name_cache: Dict[str, str] = {}          # key: (region, instance_id) -> instance_name

def get_attached_instance_ids(ecs_client: EcsClient, region_id: str, disk_id: str) -> List[str]:
    """
    Lookup which instance(s) the disk is attached to using DescribeDisks.
    Handles multi-attach via 'attachments.attachment' in the response.
    """
    cache_key = f"{region_id}:{disk_id}"
    if cache_key in _disk_attach_cache:
        return _disk_attach_cache[cache_key]

    req = ecs_models.DescribeDisksRequest(
        region_id=region_id,
        disk_ids=f'["{disk_id}"]',   # JSON array as string
        page_size=10,
        page_number=1,
    )
    instance_ids: List[str] = []

    try:
        resp = ecs_client.describe_disks(req)
        disks = (resp.body.disks.disk or [])
        if disks:
            disk_obj = disks[0]
            attachments = getattr(disk_obj, "attachments", None)
            if attachments and getattr(attachments, "attachment", None):
                for att in attachments.attachment:
                    iid = getattr(att, "instance_id", None) or getattr(att, "InstanceId", None)
                    if iid:
                        instance_ids.append(iid)
            else:
                # Fallback: some SDK versions expose a direct 'instance_id' field
                iid_direct = getattr(disk_obj, "instance_id", None)
                if iid_direct:
                    instance_ids.append(iid_direct)
    except Exception:
        # Silent fallback — leave empty list if query fails
        pass

    # Deduplicate + sort (stable primary choice)
    instance_ids = sorted(list(set(instance_ids)))
    _disk_attach_cache[cache_key] = instance_ids
    return instance_ids


def get_instance_name(ecs_client: EcsClient, region_id: str, instance_id: str) -> Optional[str]:
    """
    Lookup instance name from DescribeInstances.
    """
    cache_key = f"{region_id}:{instance_id}"
    if cache_key in _instance_name_cache:
        return _instance_name_cache[cache_key]

    req = ecs_models.DescribeInstancesRequest(
        region_id=region_id,
        instance_ids=f'["{instance_id}"]',
        page_size=10,
        page_number=1,
    )

    try:
        resp = ecs_client.describe_instances(req)
        instances = (resp.body.instances.instance or [])
        if instances:
            name = getattr(instances[0], "instance_name", None)
            if name:
                _instance_name_cache[cache_key] = name
                return name
    except Exception:
        pass

    _instance_name_cache[cache_key] = None
    return None


def check_snapshots_in_region(region_id: str, lookback_hours: int) -> Dict[str, Any]:
    """
    Query snapshots in a region and return:
      - snapshots: list of snapshot dicts (with size fields, is_recent, and instance attachment info)
      - backup_verification: summary dict with success|fail based on recent snapshots
    """
    ecs_client = make_ecs_client(region_id)

    now = datetime.datetime.utcnow()
    cutoff_dt = now - datetime.timedelta(hours=lookback_hours)
    cutoff_iso = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    request = ecs_models.DescribeSnapshotsRequest(
        region_id=region_id,
        status="all",
        page_size=50,
        page_number=1,
    )

    snapshots_out: List[Dict[str, Any]] = []

    try:
        while True:
            response = ecs_client.describe_snapshots(request)
            body = response.body
            snapshots = body.snapshots.snapshot or []

            for snap in snapshots:
                # creation_time example: "YYYY-MM-DDTHH:MM:SSZ"
                creation_time = datetime.datetime.strptime(
                    snap.creation_time, "%Y-%m-%dT%H:%M:%SZ"
                )

                is_recent = creation_time > cutoff_dt

                # Size fields (may vary per SDK/region)
                source_disk_size_raw = getattr(snap, "source_disk_size", None)
                actual_snapshot_size_raw = getattr(snap, "actual_snapshot_size", None)

                source_disk_size_gb = _parse_int(source_disk_size_raw)
                actual_snapshot_size_gb = _parse_int(actual_snapshot_size_raw)

                # Resolve attached ECS instance(s) from source disk
                source_disk_id = getattr(snap, "source_disk_id", "")
                attached_instance_ids: List[str] = []
                attached_instance_names: List[Optional[str]] = []

                if source_disk_id:
                    attached_instance_ids = get_attached_instance_ids(ecs_client, region_id, source_disk_id)
                    for iid in attached_instance_ids:
                        iname = get_instance_name(ecs_client, region_id, iid)
                        attached_instance_names.append(iname)

                # Choose primary (first) if any
                primary_instance_id = attached_instance_ids[0] if attached_instance_ids else None
                primary_instance_name = attached_instance_names[0] if attached_instance_names else None

                snapshots_out.append({
                    "snapshot_id": snap.snapshot_id,
                    "status": snap.status,
                    "created_utc": snap.creation_time,
                    "source_disk_id": source_disk_id,
                    "source_disk_type": getattr(snap, "source_disk_type", ""),
                    "progress": getattr(snap, "progress", ""),
                    "product_code": getattr(snap, "product_code", ""),
                    "usage": getattr(snap, "usage", ""),
                    "source_disk_size_gb": source_disk_size_gb,
                    "actual_snapshot_size_gb": actual_snapshot_size_gb,
                    "is_recent": is_recent,

                    # >>> NEW fields directly on snapshot <<<
                    "instance_id": primary_instance_id,
                    "instance_name": primary_instance_name,
                    "attached_instance_ids": attached_instance_ids,
                    "attached_instance_names": attached_instance_names,
                })

            # Pagination (classic PageNumber/PageSize)
            total_count = body.total_count or 0
            page_size = request.page_size
            current_page = request.page_number
            max_page = (total_count + page_size - 1) // page_size

            if current_page >= max_page or len(snapshots) == 0:
                break

            request.page_number = current_page + 1

        # Region-level verification using is_recent
        recent_snapshot_count = sum(1 for s in snapshots_out if s.get("is_recent"))
        verification_result = "success" if recent_snapshot_count > 0 else "fail"

        return {
            "snapshots": snapshots_out,
            "backup_verification": {
                "result": verification_result,
                "recent_snapshot_count": recent_snapshot_count,
                "cutoff_utc": cutoff_iso,
                "lookback_hours": lookback_hours,
            },
        }

    except Exception as e:
        return {
            "snapshots": [{"error": f"{region_id}: {str(e)}"}],
            "backup_verification": {
                "result": "fail",
                "recent_snapshot_count": 0,
                "cutoff_utc": cutoff_iso,
                "lookback_hours": lookback_hours,
                "note": "Query error encountered.",
            },
        }


def build_report(regions: List[str], lookback_hours: int) -> Dict[str, Any]:
    """
    Builds a consolidated report for all specified regions in the current account.
    """
    started_at_utc = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    entries: List[Dict[str, Any]] = []

    for region in regions:
        print(f"[INFO] Checking region: {region}...")
        region_result = check_snapshots_in_region(region, lookback_hours)
        entries.append({
            "region": region,
            **region_result,  # includes snapshots + backup_verification
        })

    # Top-level summary
    total_recent = sum(e["backup_verification"]["recent_snapshot_count"] for e in entries)
    regions_success = sum(1 for e in entries if e["backup_verification"]["result"] == "success")

    return {
        "generated_at_utc": started_at_utc,
        "lookback_hours": lookback_hours,
        "account_access_key_id": MASK_AK(MASTER_ACCESS_KEY_ID),
        "regions_count": len(regions),
        "regions_with_recent_backups": regions_success,
        "total_recent_snapshots": total_recent,
        "entries": entries,
    }


def MASK_AK(ak: str) -> str:
    """
    Mask AccessKey for logging/report (privacy).
    """
    if not ak:
        return ""
    if len(ak) <= 8:
        return "***"
    return ak[:4] + "****" + ak[-4:]


def save_json_report(data: Dict[str, Any], output_path: str) -> str:
    """
    Save JSON report to disk.
    """
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return output_path


# ---------------------- CLI Entrypoint ---------------------- #

def parse_args():
    parser = argparse.ArgumentParser(
        description="Alibaba Cloud ECS Snapshot Audit (no STS) — JSON with size, verification, and instance fields"
    )
    parser.add_argument(
        "-o", "--output",
        default=f"./backup_report_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}Z.json",
        help="Output JSON file path (default: ./backup_report_YYYYMMDD_HHMMSSZ.json)",
    )
    parser.add_argument(
        "-l", "--lookback-hours",
        type=int,
        default=DEFAULT_LOOKBACK_HOURS,
        help=f"Lookback window in hours (default: {DEFAULT_LOOKBACK_HOURS})",
    )
    parser.add_argument(
        "-r", "--regions",
        nargs="*",
        default=REGIONS,
        help=f"Space-separated list of regions to check (default: {', '.join(REGIONS)})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Sanity checks
    if MASTER_ACCESS_KEY_ID.startswith("YOUR_") or MASTER_ACCESS_KEY_SECRET.startswith("YOUR_"):
        print("[WARN] MASTER_ACCESS_KEY_ID/SECRET look like placeholders. Set environment variables.")
    if not args.regions:
        print("[ERROR] No regions specified. Use --regions to provide at least one region.")
        return

    report = build_report(args.regions, args.lookback_hours)
    output_path = save_json_report(report, args.output)
    print(f"[OK] Report saved: {output_path}")


if __name__ == "__main__":
    main()
