"""Seed floci with the resources and budget in docs/contracts/finops_mock.json.

Each resource gets a Name tag holding its mock id (AWS generates the real ids) plus its mock tags.
Idempotent: a resource whose Name already exists (and is not terminated) is skipped.
RunInstances in floci is slow, so expect this to take a while.

    python -m finops.seed
"""

from __future__ import annotations

from botocore.exceptions import ClientError

import finops
from finops.cleanup import tag_dict

AMI = "ami-0abcdef1234567890"  # floci's built-in Amazon Linux 2 image; replace for a real account
AZ = "us-east-1a"


def _tag_spec(rtype: str, r: dict) -> list[dict]:
    tags = [{"Key": "Name", "Value": r["id"]}] + [{"Key": k, "Value": v} for k, v in r["tags"].items()]
    return [{"ResourceType": rtype, "Tags": tags}]


def _existing(ec2, s3, budgets, account_id: str) -> set:
    names = {
        tag_dict(i.get("Tags")).get("Name")
        for res in ec2.describe_instances()["Reservations"]
        for i in res["Instances"]
        if i["State"]["Name"] != "terminated"
    }
    names |= {tag_dict(v.get("Tags")).get("Name") for v in ec2.describe_volumes()["Volumes"]}
    names |= {tag_dict(s.get("Tags")).get("Name")
              for s in ec2.describe_snapshots(OwnerIds=["self"])["Snapshots"]}
    names |= {b["Name"] for b in s3.list_buckets()["Buckets"]}
    names |= {b["BudgetName"] for b in budgets.describe_budgets(AccountId=account_id).get("Budgets", [])}
    return names


def seed(mock: dict, ec2, s3, budgets, account_id: str) -> list[str]:
    """Create what is missing; return the mock ids (and budget names) created."""
    have = _existing(ec2, s3, budgets, account_id)
    created: list[str] = []
    snapshot_source = None
    for r in mock["resources"]:
        if r["type"] == "ebs_volume" and snapshot_source is None:
            snapshot_source = r["id"]
        if r["id"] in have:
            continue
        if r["type"] == "ec2_instance":
            iid = ec2.run_instances(ImageId=AMI, MinCount=1, MaxCount=1, InstanceType="t3.micro",
                                    TagSpecifications=_tag_spec("instance", r))["Instances"][0]["InstanceId"]
            if r["state"] == "stopped":
                ec2.stop_instances(InstanceIds=[iid])
        elif r["type"] == "ebs_volume":
            ec2.create_volume(AvailabilityZone=AZ, Size=1, TagSpecifications=_tag_spec("volume", r))
        elif r["type"] == "ebs_snapshot":
            # AWS sets StartTime itself, so age_days cannot be reproduced; cleanup reads the real age.
            vols = {tag_dict(v.get("Tags")).get("Name"): v["VolumeId"]
                    for v in ec2.describe_volumes()["Volumes"]}
            try:
                ec2.create_snapshot(VolumeId=vols[snapshot_source],
                                    TagSpecifications=_tag_spec("snapshot", r))
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "UnsupportedOperation":
                    raise
                print(f"skipped {r['id']}: CreateSnapshot not supported by this endpoint")
                continue
        else:
            s3.create_bucket(Bucket=r["id"])
            if r["tags"]:
                s3.put_bucket_tagging(Bucket=r["id"], Tagging={
                    "TagSet": [{"Key": k, "Value": v} for k, v in r["tags"].items()]})
            for n in range(r["object_count"]):
                s3.put_object(Bucket=r["id"], Key=f"seed/{n:03d}.txt", Body=b"seed")
        created.append(r["id"])
    for b in mock["budgets"]:
        if b["name"] not in have:
            budgets.create_budget(AccountId=account_id, Budget={
                "BudgetName": b["name"], "BudgetType": "COST", "TimeUnit": "MONTHLY",
                "BudgetLimit": {"Amount": str(b["limit"]), "Unit": mock["currency"]}})
            created.append(b["name"])
    return created


def main() -> list[str]:
    account_id = finops.client("sts").get_caller_identity()["Account"]
    created = seed(finops.load_mock(), finops.client("ec2"), finops.client("s3"),
                   finops.client("budgets"), account_id)
    print(f"seeded {len(created)}: {', '.join(created)}")
    return created


if __name__ == "__main__":
    main()
