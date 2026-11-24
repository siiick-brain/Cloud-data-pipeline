"""Unit tests for finops: seed, cleanup and cost report against the finops_mock.json shape."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

import finops
from finops import cleanup, cost_report, seed
from tests.finops.fakes import FakeBudgets, FakeCE, FakeEC2, FakeS3, FakeSTS

ROOT = Path(__file__).resolve().parents[2]
MOCK = json.loads((ROOT / "docs/contracts/finops_mock.json").read_text())


@pytest.fixture
def aws(monkeypatch):
    clients = {"ec2": FakeEC2(), "s3": FakeS3(), "budgets": FakeBudgets(), "sts": FakeSTS(),
               "ce": FakeCE(MOCK)}
    monkeypatch.setattr(finops, "client", lambda name: clients[name])
    return clients


def _age_snapshots(ec2):
    """The mock gives snapshot ages; AWS cannot backdate StartTime, so the test does it."""
    ages = {r["id"]: r["age_days"] for r in MOCK["resources"] if r["type"] == "ebs_snapshot"}
    for snap in ec2.snapshots.values():
        name = cleanup.tag_dict(snap["Tags"])["Name"]
        snap["StartTime"] = datetime.now(UTC) - timedelta(days=ages[name])


# ---------- package helpers ----------

def test_load_mock_reads_contract():
    assert finops.load_mock() == MOCK


def test_client_uses_floci_defaults(monkeypatch):
    calls = {}
    monkeypatch.setattr(finops.boto3, "client", lambda name, **kw: calls.setdefault(name, kw))
    for var in ("AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(var, raising=False)
    finops.client("ec2")
    assert calls["ec2"] == {"endpoint_url": "http://localhost:4566", "aws_access_key_id": "test",
                            "aws_secret_access_key": "test", "region_name": "us-east-1"}


def test_client_real_account_when_endpoint_empty(monkeypatch):
    calls = {}
    monkeypatch.setattr(finops.boto3, "client", lambda name, **kw: calls.setdefault(name, kw))
    monkeypatch.setenv("AWS_ENDPOINT_URL", "")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    finops.client("s3")
    assert calls["s3"] == {"region_name": "eu-west-1"}


# ---------- seed ----------

def test_seed_creates_every_mock_resource(aws):
    created = seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000")
    assert sorted(created) == sorted([r["id"] for r in MOCK["resources"]] + ["monthly-data-platform"])

    by_name = {cleanup.tag_dict(i["Tags"])["Name"]: i for i in aws["ec2"].instances.values()}
    assert by_name["i-idle-001"]["State"]["Name"] == "stopped"
    assert by_name["i-run-001"]["State"]["Name"] == "running"
    assert cleanup.tag_dict(by_name["i-keep-001"]["Tags"])["keep"] == "true"
    vols = {cleanup.tag_dict(v["Tags"])["Name"]: v for v in aws["ec2"].volumes.values()}
    assert set(vols) == {"vol-idle-001", "vol-keep-001"}
    assert len(aws["ec2"].snapshots) == 2
    s3 = aws["s3"].buckets
    assert len(s3["data-bucket-001"]["objects"]) == 12
    assert s3["empty-bucket-001"]["tags"] is None
    assert s3["keep-empty-bucket"]["tags"] == [{"Key": "keep", "Value": "true"}]
    assert aws["budgets"].budgets["monthly-data-platform"]["BudgetLimit"]["Amount"] == "750.0"


def test_seed_is_idempotent(aws):
    seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000")
    assert seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000") == []
    assert len(aws["ec2"].instances) == 3


def test_seed_skips_snapshots_when_unsupported(aws, capsys):
    aws["ec2"].snapshots_supported = False
    created = seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000")
    assert "snap-old-001" not in created and "i-idle-001" in created
    assert "CreateSnapshot not supported" in capsys.readouterr().out


def test_seed_reraises_other_snapshot_errors(aws):
    def boom(**kw):
        raise cleanup.ClientError({"Error": {"Code": "AccessDenied"}}, "CreateSnapshot")
    aws["ec2"].create_snapshot = boom
    with pytest.raises(cleanup.ClientError):
        seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000")


def test_seed_main(aws, capsys):
    seed.main()
    assert "seeded 11" in capsys.readouterr().out


# ---------- cleanup ----------

def test_select_on_mock_matches_expected_cleanup():
    picked = cleanup.select(MOCK["resources"], MOCK["idle_rules"])
    assert [r["id"] for r in picked] == MOCK["expected_cleanup"]


def test_select_never_picks_keep_tagged():
    rules = dict(MOCK["idle_rules"], snapshot_max_age_days=0)
    kept = [dict(r, tags={"keep": "true"}) for r in MOCK["resources"]]
    assert cleanup.select(kept, rules) == []


def test_collect_and_dry_run_delete_nothing(aws, capsys):
    seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000")
    _age_snapshots(aws["ec2"])
    picked = cleanup.main([])
    assert sorted(r["name"] for r in picked) == sorted(MOCK["expected_cleanup"])
    assert aws["ec2"].deleted == [] and aws["s3"].deleted == []
    assert "dry run" in capsys.readouterr().out


def test_apply_deletes_exactly_expected(aws):
    seed.seed(MOCK, aws["ec2"], aws["s3"], aws["budgets"], "000000000000")
    _age_snapshots(aws["ec2"])
    real_to_name = {r["id"]: r["name"] for r in cleanup.collect(aws["ec2"], aws["s3"], datetime.now(UTC))}
    cleanup.main(["--apply"])
    removed = [real_to_name[i] for i in aws["ec2"].deleted] + aws["s3"].deleted
    assert sorted(removed) == sorted(MOCK["expected_cleanup"])
    # a second run finds nothing left to clean
    assert cleanup.main(["--apply"]) == []


def test_collect_falls_back_to_id_without_name_tag(aws):
    aws["s3"].create_bucket(Bucket="raw-bucket")
    aws["ec2"].create_volume(AvailabilityZone="us-east-1a", Size=1)
    found = cleanup.collect(aws["ec2"], aws["s3"], datetime.now(UTC))
    assert {r["name"] for r in found} == {r["id"] for r in found}


# ---------- cost report ----------

def _read_csv(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def test_mock_report_csv_and_svg(tmp_path, monkeypatch):
    monkeypatch.setattr(cost_report, "OUT_DIR", tmp_path)
    cost_report.main(["--mock"])
    rows = _read_csv(tmp_path / "cost_report.csv")
    services = {r["key"]: float(r["amount"]) for r in rows if r["dimension"] == "service"}
    assert services == {c["service"]: c["amount"] for c in MOCK["cost_by_service"]}
    tags = {r["key"]: float(r["amount"]) for r in rows if r["dimension"] == "tag"}
    assert tags == {f"{c['tag_key']}={c['tag_value']}": c["amount"] for c in MOCK["cost_by_tag"]}
    budget = {r["dimension"]: float(r["amount"]) for r in rows if r["key"] == "monthly-data-platform"}
    assert budget == {"budget_limit": 750.0, "budget_actual": 802.45, "budget_forecast": 845.0}
    assert {r["period_start"] for r in rows} == {"2026-10-01"}

    svg = (tmp_path / "cost_report.svg").read_text()
    assert svg.startswith("<svg")
    for c in MOCK["cost_by_service"]:
        assert c["service"] in svg
    assert "team=untagged" in svg


def test_live_report_from_cost_explorer_matches_mock_shape(aws, tmp_path, monkeypatch):
    aws["budgets"].budgets["monthly-data-platform"] = {
        "BudgetName": "monthly-data-platform", "BudgetLimit": {"Amount": "750", "Unit": "USD"},
        "CalculatedSpend": {"ActualSpend": {"Amount": "802.45"}, "ForecastedSpend": {"Amount": "845"}}}
    report = cost_report.from_aws(aws["ce"], aws["budgets"], "000000000000", "2026-10-01", "2026-11-01")
    assert report == {k: MOCK[k] for k in ("period", "currency", "cost_by_service", "cost_by_tag", "budgets")}


def test_live_main_uses_last_full_month(aws, tmp_path, monkeypatch):
    monkeypatch.setattr(cost_report, "OUT_DIR", tmp_path)
    aws["budgets"].budgets["b"] = {"BudgetName": "b", "BudgetLimit": {"Amount": "10"},
                                   "CalculatedSpend": {"ActualSpend": {"Amount": "0"}}}
    cost_report.main([])
    rows = _read_csv(tmp_path / "cost_report.csv")
    first = datetime.now(UTC).date().replace(day=1)
    assert rows[0]["period_end"] == first.isoformat()
    assert rows[0]["period_start"] == (first - timedelta(days=1)).replace(day=1).isoformat()
    assert {r["dimension"] for r in rows if r["key"] == "b"} == {"budget_limit", "budget_actual"}


# ---------- grafana provisioning and dashboards ----------

def test_datasources_provisioned_with_contract_uids():
    ds = yaml.safe_load((ROOT / "dashboards/provisioning/datasources/datasources.yaml").read_text())
    by_uid = {d["uid"]: d for d in ds["datasources"]}
    assert by_uid["warehouse"]["type"] == "grafana-postgresql-datasource"
    assert by_uid["warehouse"]["user"] == "grafana_ro"
    assert by_uid["warehouse"]["jsonData"]["database"] == "warehouse"
    assert by_uid["warehouse"]["secureJsonData"]["password"] == "${GRAFANA_DB_PASSWORD}"
    assert by_uid["prometheus"]["url"] == "http://prometheus:9090"


def test_dashboard_provider_loads_whole_folder():
    prov = yaml.safe_load((ROOT / "dashboards/provisioning/dashboards/dashboards.yaml").read_text())
    (provider,) = prov["providers"]
    assert provider["options"]["path"] == "/var/lib/grafana/dashboards"


@pytest.mark.parametrize(("name", "table"), [("sales", "gold."), ("quality", "quality.dq_results")])
def test_dashboards_query_their_tables(name, table):
    dash = json.loads((ROOT / f"dashboards/json/{name}.json").read_text())
    assert dash["uid"] == name
    assert dash["panels"]
    for panel in dash["panels"]:
        assert panel["datasource"]["uid"] == "warehouse"
        for target in panel["targets"]:
            assert table in target["rawSql"]


# ---------- terraform ----------

def test_terraform_targets_floci_and_covers_mock():
    tf = (ROOT / "infra/terraform/main.tf").read_text()
    assert "http://localhost:4566" in tf and '"test"' in tf
    for r in MOCK["resources"]:
        assert r["id"] in tf
