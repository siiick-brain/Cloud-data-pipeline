"""Monthly cost report by service and by tag, plus budgets, from Cost Explorer and Budgets.

Writes finops/out/cost_report.csv and finops/out/cost_report.svg (bar chart by service and by tag).
--mock reads docs/contracts/finops_mock.json instead of calling AWS.

    python -m finops.cost_report [--mock]
"""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
from html import escape
from pathlib import Path

import finops

OUT_DIR = Path(__file__).resolve().parent / "out"
TAG_KEY = "team"  # the cost allocation tag in finops_mock.json
METRIC = "UnblendedCost"
FIELDS = ["period_start", "period_end", "dimension", "key", "amount", "currency"]


def from_mock(mock: dict) -> dict:
    return {k: mock[k] for k in ("period", "currency", "cost_by_service", "cost_by_tag", "budgets")}


def _grouped(ce, start: str, end: str, group: dict) -> dict[str, float]:
    resp = ce.get_cost_and_usage(TimePeriod={"Start": start, "End": end}, Granularity="MONTHLY",
                                 Metrics=[METRIC], GroupBy=[group])
    totals: dict[str, float] = {}
    for period in resp["ResultsByTime"]:
        for g in period["Groups"]:
            key = g["Keys"][0]
            totals[key] = totals.get(key, 0.0) + float(g["Metrics"][METRIC]["Amount"])
    return totals


def from_aws(ce, budgets, account_id: str, start: str, end: str) -> dict:
    services = _grouped(ce, start, end, {"Type": "DIMENSION", "Key": "SERVICE"})
    tags = _grouped(ce, start, end, {"Type": "TAG", "Key": TAG_KEY})
    report_budgets = []
    for b in budgets.describe_budgets(AccountId=account_id).get("Budgets", []):
        spend = b.get("CalculatedSpend", {})
        row = {"name": b["BudgetName"], "limit": float(b["BudgetLimit"]["Amount"]),
               "actual": float(spend.get("ActualSpend", {}).get("Amount", 0))}
        if "ForecastedSpend" in spend:
            row["forecast"] = float(spend["ForecastedSpend"]["Amount"])
        report_budgets.append(row)
    return {
        "period": {"start": start, "end": end},
        "currency": "USD",
        "cost_by_service": [{"service": k, "amount": v} for k, v in services.items()],
        # Cost Explorer returns tag groups as "team$data"; an empty value means untagged.
        "cost_by_tag": [{"tag_key": TAG_KEY, "tag_value": k.split("$", 1)[-1] or "untagged", "amount": v}
                        for k, v in tags.items()],
        "budgets": report_budgets,
    }


def rows(report: dict) -> list[dict]:
    out = [("service", c["service"], c["amount"]) for c in report["cost_by_service"]]
    out += [("tag", f"{c['tag_key']}={c['tag_value']}", c["amount"]) for c in report["cost_by_tag"]]
    for b in report["budgets"]:
        out += [(f"budget_{f}", b["name"], b[f]) for f in ("limit", "actual", "forecast") if f in b]
    p = report["period"]
    return [dict(zip(FIELDS, (p["start"], p["end"], d, k, round(a, 2), report["currency"]), strict=True))
            for d, k, a in out]


def write_csv(report: dict, path: Path) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows(report))


def write_svg(report: dict, path: Path) -> None:
    """Two horizontal bar charts, cost by service and cost by tag, in plain SVG."""
    sections = [
        ("Cost by service", [(c["service"], c["amount"]) for c in report["cost_by_service"]]),
        (f"Cost by tag ({TAG_KEY})", [(f"{c['tag_key']}={c['tag_value']}", c["amount"])
                                      for c in report["cost_by_tag"]]),
    ]
    peak = max([a for _, items in sections for _, a in items] + [1e-9])
    label_w, bar_w, row_h = 300, 400, 24
    parts, y = [], 30
    title = f"AWS cost {report['period']['start']} to {report['period']['end']} ({report['currency']})"
    parts.append(f'<text x="10" y="20" font-weight="bold">{escape(title)}</text>')
    for heading, items in sections:
        y += 20
        parts.append(f'<text x="10" y="{y}" font-weight="bold">{escape(heading)}</text>')
        for label, amount in sorted(items, key=lambda i: -i[1]):
            y += row_h
            w = bar_w * amount / peak
            parts.append(f'<text x="10" y="{y}">{escape(label)}</text>')
            parts.append(f'<rect x="{label_w}" y="{y - 14}" width="{w:.1f}" height="16" fill="#4e79a7"/>')
            parts.append(f'<text x="{label_w + w + 6:.1f}" y="{y}">{amount:,.2f}</text>')
        y += 10
    width = label_w + bar_w + 100
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{y + 10}" '
        f'font-family="sans-serif" font-size="13">\n' + "\n".join(parts) + "\n</svg>\n")


def last_full_month(today) -> tuple[str, str]:
    end = today.replace(day=1)
    start = (end - timedelta(days=1)).replace(day=1)
    return start.isoformat(), end.isoformat()


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mock", action="store_true", help="read finops_mock.json instead of AWS")
    args = parser.parse_args(argv)
    if args.mock:
        report = from_mock(finops.load_mock())
    else:
        start, end = last_full_month(datetime.now(UTC).date())
        account_id = finops.client("sts").get_caller_identity()["Account"]
        report = from_aws(finops.client("ce"), finops.client("budgets"), account_id, start, end)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(report, OUT_DIR / "cost_report.csv")
    write_svg(report, OUT_DIR / "cost_report.svg")
    print(f"wrote {OUT_DIR / 'cost_report.csv'} and {OUT_DIR / 'cost_report.svg'}")
    return report


if __name__ == "__main__":
    main()
