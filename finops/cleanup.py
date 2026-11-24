"""Find and remove idle resources: stopped instances, unattached volumes, old snapshots, empty buckets.

Dry run by default; pass --apply to delete. Resources tagged keep=true (idle_rules in finops_mock.json)
are always skipped.

    python -m finops.cleanup [--apply]
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from botocore.exceptions import ClientError

import finops


def tag_dict(tags: list[dict] | None) -> dict:
    return {t["Key"]: t["Value"] for t in tags or []}


def _record(rtype: str, rid: str, tags: list[dict] | None, **extra) -> dict:
    tags_d = tag_dict(tags)
    return {"type": rtype, "id": rid, "name": tags_d.get("Name", rid), "tags": tags_d, **extra}


def collect(ec2, s3, now: datetime) -> list[dict]:
    """Live resources in the finops_mock.json resource shape, plus `name` (the Name tag or the id)."""
    out = []
    for res in ec2.describe_instances()["Reservations"]:
        for i in res["Instances"]:
            out.append(_record("ec2_instance", i["InstanceId"], i.get("Tags"), state=i["State"]["Name"]))
    for v in ec2.describe_volumes()["Volumes"]:
        out.append(_record("ebs_volume", v["VolumeId"], v.get("Tags"), state=v["State"]))
    for s in ec2.describe_snapshots(OwnerIds=["self"])["Snapshots"]:
        age = (now - s["StartTime"]).days
        out.append(_record("ebs_snapshot", s["SnapshotId"], s.get("Tags"), age_days=age))
    for b in s3.list_buckets()["Buckets"]:
        name = b["Name"]
        try:
            tags = s3.get_bucket_tagging(Bucket=name)["TagSet"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchTagSet":
                raise
            tags = []
        count = s3.list_objects_v2(Bucket=name, MaxKeys=1)["KeyCount"]
        out.append(_record("s3_bucket", name, tags, object_count=count))
    return out


def select(resources: list[dict], rules: dict) -> list[dict]:
    """Resources the idle rules say to remove, in input order."""
    def idle(r: dict) -> bool:
        if r["tags"].get(rules["keep_tag"]) == rules["keep_value"]:
            return False
        return {
            "ec2_instance": lambda: r["state"] == "stopped",
            "ebs_volume": lambda: r["state"] == "available",
            "ebs_snapshot": lambda: r["age_days"] > rules["snapshot_max_age_days"],
            "s3_bucket": lambda: r["object_count"] == 0,
        }[r["type"]]()

    return [r for r in resources if idle(r)]


def delete(ec2, s3, resources: list[dict]) -> None:
    for r in resources:
        if r["type"] == "ec2_instance":
            ec2.terminate_instances(InstanceIds=[r["id"]])
        elif r["type"] == "ebs_volume":
            ec2.delete_volume(VolumeId=r["id"])
        elif r["type"] == "ebs_snapshot":
            ec2.delete_snapshot(SnapshotId=r["id"])
        else:
            s3.delete_bucket(Bucket=r["id"])


def main(argv: list[str] | None = None) -> list[dict]:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="delete; without it only report")
    args = parser.parse_args(argv)
    ec2, s3 = finops.client("ec2"), finops.client("s3")
    picked = select(collect(ec2, s3, datetime.now(UTC)), finops.load_mock()["idle_rules"])
    verb = "deleting" if args.apply else "dry run, would delete"
    for r in picked:
        print(f"{verb} {r['type']} {r['id']} ({r['name']})")
    if args.apply:
        delete(ec2, s3, picked)
    done = "deleted" if args.apply else "found (dry run, pass --apply to delete)"
    print(f"{len(picked)} resources {done}")
    return picked


if __name__ == "__main__":
    main()
