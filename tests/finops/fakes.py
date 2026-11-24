"""In-memory stand-ins for the boto3 clients finops uses (EC2, S3, Cost Explorer, Budgets, STS)."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime

from botocore.exceptions import ClientError

_ids = itertools.count(1)


def _err(code, op):
    return ClientError({"Error": {"Code": code, "Message": code}}, op)


def _tags(tag_specs):
    return [t for spec in tag_specs or [] for t in spec["Tags"]]


class FakeEC2:
    def __init__(self, snapshots_supported=True):
        self.instances: dict[str, dict] = {}
        self.volumes: dict[str, dict] = {}
        self.snapshots: dict[str, dict] = {}
        self.snapshots_supported = snapshots_supported
        self.deleted: list[str] = []

    def run_instances(self, ImageId, MinCount, MaxCount, InstanceType, TagSpecifications=None):
        iid = f"i-{next(_ids):04x}"
        self.instances[iid] = {"InstanceId": iid, "State": {"Name": "running"},
                               "Tags": _tags(TagSpecifications)}
        return {"Instances": [self.instances[iid]]}

    def stop_instances(self, InstanceIds):
        for iid in InstanceIds:
            self.instances[iid]["State"] = {"Name": "stopped"}

    def terminate_instances(self, InstanceIds):
        for iid in InstanceIds:
            self.instances[iid]["State"] = {"Name": "terminated"}
            self.deleted.append(iid)

    def describe_instances(self):
        return {"Reservations": [{"Instances": list(self.instances.values())}]}

    def create_volume(self, AvailabilityZone, Size, TagSpecifications=None):
        vid = f"vol-{next(_ids):04x}"
        self.volumes[vid] = {"VolumeId": vid, "State": "available", "Tags": _tags(TagSpecifications)}
        return self.volumes[vid]

    def describe_volumes(self):
        return {"Volumes": list(self.volumes.values())}

    def delete_volume(self, VolumeId):
        del self.volumes[VolumeId]
        self.deleted.append(VolumeId)

    def create_snapshot(self, VolumeId, TagSpecifications=None):
        if not self.snapshots_supported:
            raise _err("UnsupportedOperation", "CreateSnapshot")
        sid = f"snap-{next(_ids):04x}"
        self.snapshots[sid] = {"SnapshotId": sid, "StartTime": datetime.now(UTC),
                               "Tags": _tags(TagSpecifications)}
        return self.snapshots[sid]

    def describe_snapshots(self, OwnerIds):
        return {"Snapshots": list(self.snapshots.values())}

    def delete_snapshot(self, SnapshotId):
        del self.snapshots[SnapshotId]
        self.deleted.append(SnapshotId)


class FakeS3:
    def __init__(self):
        self.buckets: dict[str, dict] = {}
        self.deleted: list[str] = []

    def create_bucket(self, Bucket):
        self.buckets[Bucket] = {"tags": None, "objects": []}

    def put_bucket_tagging(self, Bucket, Tagging):
        self.buckets[Bucket]["tags"] = Tagging["TagSet"]

    def get_bucket_tagging(self, Bucket):
        if self.buckets[Bucket]["tags"] is None:
            raise _err("NoSuchTagSet", "GetBucketTagging")
        return {"TagSet": self.buckets[Bucket]["tags"]}

    def put_object(self, Bucket, Key, Body):
        self.buckets[Bucket]["objects"].append(Key)

    def list_buckets(self):
        return {"Buckets": [{"Name": n} for n in self.buckets]}

    def list_objects_v2(self, Bucket, MaxKeys):
        return {"KeyCount": min(len(self.buckets[Bucket]["objects"]), MaxKeys)}

    def delete_bucket(self, Bucket):
        del self.buckets[Bucket]
        self.deleted.append(Bucket)


class FakeCE:
    """Answers GetCostAndUsage from the mock's cost_by_service and cost_by_tag."""

    def __init__(self, mock):
        self.mock = mock

    def get_cost_and_usage(self, TimePeriod, Granularity, Metrics, GroupBy):
        group = GroupBy[0]
        if group["Type"] == "DIMENSION":
            keys = [([c["service"]], c["amount"]) for c in self.mock["cost_by_service"]]
        else:
            keys = [([f"{c['tag_key']}${'' if c['tag_value'] == 'untagged' else c['tag_value']}"],
                     c["amount"]) for c in self.mock["cost_by_tag"]]
        groups = [{"Keys": k, "Metrics": {"UnblendedCost": {"Amount": str(a), "Unit": "USD"}}}
                  for k, a in keys]
        return {"ResultsByTime": [{"Groups": groups}]}


class FakeBudgets:
    def __init__(self):
        self.budgets: dict[str, dict] = {}

    def create_budget(self, AccountId, Budget):
        self.budgets[Budget["BudgetName"]] = Budget

    def describe_budgets(self, AccountId):
        return {"Budgets": list(self.budgets.values())}


class FakeSTS:
    def get_caller_identity(self):
        return {"Account": "000000000000"}
