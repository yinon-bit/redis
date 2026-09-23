# AWS benchmark: run, validate, compare

Experimental runner. It executes a documented subset, not the whole shared suite.
Start here; historical scripts/reports in this directory are not this workflow.

Known limitations: no automatic HTML report yet; verbose RDB validation logs can
produce hundreds of MB of compressed evidence. Cumulative archives repeat earlier
case evidence. The 30-minute default case timeout was too short for three full-size
arena repetitions in the live test. Select a suitable case timeout within the
approved instance deadline, or improve the untimed loading/validation harness;
do not silently reduce the dataset or repetitions. The live test used uncommitted
Redis optimizations, which are not automatically included in this runner commit.

## 1. Get the same source and tools

Clone the repository and checkout the exact branch/commit supplied by its owner.
The new runner must have been committed and pushed first; a local untracked file
will not appear in another developer's clone. From repository root:

```sh
python3 -m venv tools/forkless-benchmark/.venv
. tools/forkless-benchmark/.venv/bin/activate
python -m pip install -r tools/forkless-benchmark/requirements-runner.txt
python tools/forkless-benchmark/test-aws-auto.py
python tools/forkless-benchmark/test-scenario-runner.py
python tools/forkless-benchmark/test-check-results.py
```

Use Python 3.11+ and Git locally, with outbound HTTPS. No local Redis compiler,
Docker, SSH key, or AWS CLI is required for automatic provisioning. Offline tests
do not prove live AWS permissions. Tests requiring a server may explicitly skip.

## 2. AWS setup

Export your own temporary AWS credentials in your terminal, or use AWS_PROFILE.
Never paste credentials into a config file, Git, a report, or an issue. Credentials
must stay valid through result collection and cleanup. Keep the launcher and
computer awake throughout the run.

Required permissions include STS GetCallerIdentity; EC2 image/subnet/instance/volume
inspection, RunInstances, CreateTags, Create/DeleteSecurityGroup, TerminateInstances;
and S3 CreateBucket, public-access/ownership/encryption/tagging configuration,
PutObject/GetObject. Organization policies, subnet quotas, regional availability,
and mandatory tags can still block provisioning. No new IAM role is created.

Use your own approved `--owner` and `--team` values. The current defaults are
specific to the original developer; do not accidentally use them for your run.
An existing default public subnet is required unless you supply `--subnet-id`.
The instance needs outbound HTTPS for packages/S3; no inbound ports are opened.
Do not use production resources. This creates paid infrastructure and retains
the private S3 result bucket (storage remains billable).

## 3. Choose a case and preview

```sh
python tools/forkless-benchmark/run-aws-scenario.py --list-scenarios
python tools/forkless-benchmark/run-aws-auto.py \
  --name my-hybrid-r1 --mode hybrid --owner your_owner --team redis_core \
  --hours 2 --scenario standard.mixed.rw50.ample.cpu6.first.bgsave
```

Preview does not contact AWS. Add `--launch` to the same command to run it.
Repeat `--scenario` to select multiple cases; each named case repeats three times.
One instance is created per invocation. A two-hour cap is not a promise that all
chosen cases fit; loading and validation count toward the per-case timeout.
No second instance is launched automatically after failure.

The default machine is r7g.2xlarge, us-east-2, 32-GiB root disk and 100-GiB gp3
data disk (3000 IOPS,125 MiB/s). Case definitions fix CPU and memory limits.
Supply the same `--ami-id` and `--subnet-id` for tighter run-to-run matching.
Without `--ami-id`, latest Ubuntu ARM64 is selected and recorded, not pinned.

For custom variables use `--config` instead of named cases. See
[AWS-SCENARIO-RUNNER.md](AWS-SCENARIO-RUNNER.md) and
[scenario-example.json](scenario-example.json). Custom workloads have a different
schema and weaker validation; do not compare them as identical named cases.

The source archive contains the **current working tree**, including modified C
files. Record a clean commit for reproducibility or retain the source archive/hash
as the exact source identity; Git revision alone does not identify dirty source.
`--mode fork` uses fork mode in this checkout's build, not a separately downloaded
upstream baseline. Checkout the intended baseline source explicitly.

## 4. Validate results

The launcher prints `results/<run-id>/` and the retained S3 bucket. It downloads
case archives and their receipts, verifies checksums, then records compute cleanup.

```sh
python tools/forkless-benchmark/check-results.py \
  tools/forkless-benchmark/results/YOUR_RUN_ID > /tmp/benchmark-check.json
echo $?
```

Exit codes: **0 PASS**, **1 FAIL**, **2 MISSING**. Read the JSON, not just the exit code.

- PASS: selected cases completed with the required evidence, repetitions and
  checksum checks, and the local resource record confirms compute cleanup.
- FAIL: corrupt evidence, failed worker/validation, or inconsistent case identity.
- MISSING: unfinished/uncollected cases, absent receipts/final archive, unsupported
  schema, missing repetitions, or unverified cleanup. This is normal mid-run.

PASS does **not** mean the full suite passed, the POC is production-ready, every
metric exists, no kernel OOM occurred, or the code is faster. The checker is
offline: it verifies recorded cleanup, not current AWS state. Manually inspect
`kernel.log`, `telemetry.jsonl`, `client-errors.json`, validation logs and all
recorded limitations. Absence of an error field alone is not a correctness proof.

Archive layout (each later archive is cumulative):

```text
artifacts-N.tar.gz
  evidence/SHA256SUMS
  evidence/case-I/setup.json
  evidence/case-I/output/result.json
  evidence/case-I/output/rJ/result.json
  evidence/case-I/output/rJ/metrics.csv        # measured cases
  evidence/case-I/output/rJ/telemetry.jsonl    # measured cases
  evidence/case-I/runner.log
  evidence/kernel.log                        # final archive
```

Custom-worker output differs and does not have repetition subdirectories.
For four selected cases, artifacts-4 is the final archive. Receipts are external
`receipt-N.json` files: do not trust the receipt inside a cumulative archive as
that archive's own receipt. Older launcher versions did not save external receipts
locally; download them from the run's bucket before checking. Do not fabricate them.

The archive verifier checks members without extracting them. If you manually
extract evidence, use a separate directory per run/archive; never overwrite the
repository or combine repetitions from cumulative archives twice. Raw RDBs are
not retained; full reload results/logs are. A downloaded archive cannot rerun an
RDB validation without recreating the dataset and executing the case again.

## 5. Compare two named runs

Run the checker on **both** directories first. Match exact case_id, schema,
dataset counts/payload generator, workload mix/rates, clients/pipeline, RAM/CPU,
compression/fsync, lifecycle, instrumentation, generator revision, AMI and disk
settings. Only intended implementation/mode/revision differences should remain.
If settings differ, label the comparison non-equivalent rather than a speedup.

Keep the two run IDs/names, source hashes and revisions in your comparison.
Compare each repetition's SET, GET, HSET and HGET separately and before/save/after
phases separately. `metrics.csv` uses completion-phase throughput and send-phase
latency, including requests crossing a phase boundary. Aggregate throughput across
key classes only when the operation and phase durations match. **Never average
percentiles**; show each repetition or obtain a combined histogram.

Use `(new / old - 1) * 100` for percent change: positive throughput change is good;
positive latency/save-duration change is worse. Report all repetitions, failures,
OOM/pressure evidence and uncertainty. Three repetitions are not a guarantee of
statistical significance. The checker validates evidence; it does not generate
an automatic performance verdict or a universal cross-schema comparison.

## Correctness adapter limits

Arena/snapshot/cancellation fixtures use an isolated instrumented copy of this
POC. The adapter checks exact source anchors and fails closed if they differ.
Another POC must implement/review equivalent snapshot-cut and serializer gates
plus cancellation/routing hooks; merely renaming it does not make it compatible.
Do not compare gated fixture latency against ordinary throughput benchmarks.
Current fixture adapter: fork/hybrid only. Slow/disconnected replicas and the
remaining full-suite fixtures are not implemented.

## Sharing checklist

Use [SHARE-FILES.txt](SHARE-FILES.txt) as the explicit runner-file allowlist.
Review the diff and scan these files for secrets before staging. Never `git add .`
in this working tree: it also contains unrelated user code and historical files.
Do not include result directories, state files, private keys, credentials, or
local virtual environments. Required core changes should be a separately reviewed
commit; do not silently include someone else's dirty source changes.
No push is implied by running any benchmark or checker command.
