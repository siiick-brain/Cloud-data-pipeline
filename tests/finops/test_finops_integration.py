"""Seeds floci at localhost:4566, runs cleanup --apply and checks exactly what it removed.

floci 2.5.0 does not support EBS snapshots (CreateSnapshot is UnsupportedOperation), so seed skips
them there and the expected set drops the snapshot ids. The snapshot rule is covered by the unit tests.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

import finops
from finops import cleanup, seed

pytestmark = pytest.mark.integration


def _live_names():
    """Names of resources that still exist (terminated instances stay listed for a while)."""
    found = cleanup.collect(finops.client("ec2"), finops.client("s3"), datetime.now(UTC))
    return {r["name"] for r in found if r.get("state") != "terminated"}


def test_cleanup_removes_exactly_expected_resources():
    mock = finops.load_mock()
    seed.main()
    before = _live_names()
    snapshots = {r["id"] for r in mock["resources"] if r["type"] == "ebs_snapshot"}
    seeded = {r["id"] for r in mock["resources"]} - snapshots
    assert seeded <= before

    removed = cleanup.main(["--apply"])

    expected = set(mock["expected_cleanup"]) - snapshots
    assert {r["name"] for r in removed} == expected
    after = _live_names()
    assert not expected & after
    assert seeded - expected <= after
