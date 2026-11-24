# FinOps

Cost report, idle-resource cleanup and seeding, run against floci (local AWS at `localhost:4566`)
by default. The resources, costs, budget and idle rules come from `docs/contracts/finops_mock.json`.

| Command | What it does |
|---|---|
| `python -m finops.seed` | Creates the mock's EC2 instances, EBS volumes, snapshots, S3 buckets and budget. Each resource gets a `Name` tag holding its mock id. Idempotent. RunInstances in floci is slow. |
| `python -m finops.cost_report [--mock]` | Cost Explorer (by service and by `team` tag) and Budgets for the last full month, or the mock with `--mock`. Writes `finops/out/cost_report.csv` and `finops/out/cost_report.svg`. |
| `python -m finops.cleanup [--apply]` | Lists stopped instances, unattached volumes, snapshots older than 90 days and empty buckets. It deletes only with `--apply`. Anything tagged `keep=true` is always skipped. |

`infra/terraform` provisions the same resources through floci (`terraform init && terraform apply`).
Use either the seed script or Terraform. If you run both, the bucket names collide.

## floci limits
floci 2.5.0 has no `CreateSnapshot`, so on floci the seed script skips snapshots and Terraform keeps
`create_snapshots = false`. Unit tests cover the snapshot rule. AWS sets a snapshot's start time
itself, so even on a real account the mock's `age_days` cannot be reproduced. Cleanup reads the real age.

## Using a real AWS account
1. Point the clients at AWS: `export AWS_ENDPOINT_URL=` (empty). With no endpoint, `finops.client`
   uses the normal boto3 credential chain (env vars, `~/.aws` profile, SSO, instance role) and
   `AWS_DEFAULT_REGION` (default `us-east-1`). With an endpoint set, it uses `test`/`test` unless
   `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are set.
2. Cost Explorer must be enabled, and `team` must be activated as a cost allocation tag. Each
   Cost Explorer API call costs $0.01.
3. The IAM principal needs `ce:GetCostAndUsage`, `budgets:ViewBudget`, `sts:GetCallerIdentity`,
   `ec2:Describe*`, `s3:ListAllMyBuckets`, `s3:GetBucketTagging` and `s3:ListBucket`. Cleanup with
   `--apply` also needs `ec2:TerminateInstances`, `ec2:DeleteVolume`, `ec2:DeleteSnapshot` and
   `s3:DeleteBucket`.
4. Always run cleanup without `--apply` first and read the list. Cleanup acts on the whole account
   and region, not just seeded resources.
5. Terraform: `terraform apply -var endpoint= -var create_snapshots=true` after setting real
   credentials. Replace the floci AMI id (`ami-0abcdef1234567890`) in `main.tf` and in
   `finops/seed.py` with one from your region. Terraform and the seed script use `us-east-1`/`us-east-1a`.
