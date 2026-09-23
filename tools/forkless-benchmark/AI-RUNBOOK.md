# AI operator runbook: AWS Redis benchmark

Give this file to an AI together with the requested source revision, run name,
mode, scenarios, owner/team tags and approved AWS runtime. Read it before acting.
This file is guidance, **not authorization to create paid resources**.

## Scope and safety

- Use `run-aws-auto.py` for automatic provisioning. Do not substitute historical
  scripts in this directory or reuse old resources from local state documents.
- Each launch creates one paid r7g.2xlarge in us-east-2 by default, encrypted
  32-GiB root and 100-GiB data disks, a private S3 bucket, and a no-inbound SG.
- Obtain approval for the selected cases, machine, region, runtime and number of
  launches. Never extend a deadline or launch a replacement without authorization.
- Use credentials already supplied through the environment or AWS profile.
  Never print credentials, dump the environment, enable shell tracing, commit
  credentials, or expose signed URLs. Ask the user to configure credentials in
  their terminal rather than paste them into chat.
- Preserve dirty source. Do not reset, stash, checkout another revision, commit,
  push, change Redis, or change workload settings merely to make a test pass.
- Keep Redis localhost-only. Do not add inbound Redis/SSH access or IAM roles.
- Retain result buckets and all failed attempts. Do not delete results to save
  space without separate approval. Never claim missing tests passed.

## 1. Resolve inputs before launch

Required: unique human-readable run name, source checkout/revision, explicit
`fork|hybrid|forkless` mode, exact case IDs **or** custom JSON, valid owner/team,
approved instance runtime and case timeout. Confirm credential lifetime covers
the full runtime plus collection/cleanup. Confirm permissions described in
[DEVELOPER-QUICKSTART.md](DEVELOPER-QUICKSTART.md).

Do not assume default owner `yinon_yishay` belongs to another developer.
Do not assume an earlier run's authorization or credential file is still valid.
If comparing runs, preserve exact case settings; change only the intended
implementation/revision/mode. Record source archive SHA256: a Git commit alone
does not identify a dirty checkout.

## 2. Read and preflight (no paid launch)

Work from the repository root. Read [AWS-SCENARIO-RUNNER.md](AWS-SCENARIO-RUNNER.md)
and the relevant cases in [SHARED-POC-BENCHMARK-SPEC.md](SHARED-POC-BENCHMARK-SPEC.md).
The executable catalogue is authoritative for **implemented** cases; the shared
spec also describes cases not implemented yet.

```sh
git status --short
git rev-parse HEAD
python3 -m venv tools/forkless-benchmark/.venv
. tools/forkless-benchmark/.venv/bin/activate
python -m pip install -r tools/forkless-benchmark/requirements-runner.txt
python tools/forkless-benchmark/test-aws-auto.py
python tools/forkless-benchmark/test-scenario-runner.py
python tools/forkless-benchmark/test-check-results.py
python tools/forkless-benchmark/run-aws-scenario.py --list-scenarios
```

Reuse an existing working virtual environment rather than overwrite it. Offline
tests may explicitly skip live-server tests: report skips, not a complete pass.
Do not proceed on test failures without explaining them.

## 3. Preview, then launch the approved selection

Example only—replace name and tags with approved values. This previews one
performance case; it does not create resources:

```sh
python -u tools/forkless-benchmark/run-aws-auto.py \
  --name my-poc-r1 --mode hybrid --owner your_owner --team redis_core \
  --hours 2 --case-timeout 1800 \
  --scenario standard.mixed.rw50.ample.cpu6.first.bgsave
```

After approval, run the **same command with `--launch` appended**. Capture its
run ID and local result directory immediately. Each named case repeats three
times. Repeat `--scenario` for a batch; use `--config PATH` instead for custom
variables. These selectors are mutually exclusive. `--random-cases` chooses
performance cases only; prefer explicit IDs for reproducibility.

Useful implemented fixture IDs:

- `fixture.snapshot.base.ample.cpu6`
- `fixture.arena.string_boundary.during_capture.ample.cpu6`
- `fixture.cancellation.rw50.ample.cpu6`

Fixture adapter supports fork/hybrid only and requires matching source anchors.
Another POC needs reviewed equivalent hooks, not just a different name. It builds
an isolated instrumented copy; never mix its latency with performance results.
Slow/disconnected-replica tests and the full suite are still unsupported.

**Timeout warning:** three full-size arena repetitions exceeded the default
1800-second case allowance in the AWS test. Loading and validation count too.
Choose a larger approved case allowance before launch, or report the timeout.
Do not reduce nine million background keys, omit validation, reduce repetitions,
or modify a running case. Large batches may not fit the approved instance limit;
the runner records `not_run_deadline` rather than silently running smaller tests.

## 4. Monitor the existing run

- Keep the local launcher/computer awake. The launcher polls every 20 seconds,
  collects/checksums results and performs cleanup. Guest shutdown timer is a
  second safeguard, not a substitute for checking cloud cleanup.
- Save the exact returned instance ID, SG, volumes, bucket, absolute deadline,
  launcher session ID and local results path in an **ignored local** state file.
  Never store credentials or signed URLs in it.
- Read `status.json` and `resources.json` in that run directory. Status currently
  changes between cases, not continuously during loading/validation. An unchanged
  status is not proof of a hang, nor proof of healthy progress.
- Poll the existing launcher; do not rerun the launch command to check progress.
  Use exact-resource read-only AWS queries if local status is stale.
- If the user requests recurring chat checks, use the product's scheduling tool
  and verify creation succeeded. A running shell is not a scheduled chat monitor.
  Notify only on meaningful changes unless the user asks for every check.
- Diagnose from retained logs. Do not change measured code, settings or timing
  mid-run. Preserve timeout/error exit codes and partial repetitions.

## 5. Collect and validate

The launcher saves `artifacts-N.tar.gz` plus external `receipt-N.json` locally;
the final archive number equals the number of selected cases. Archives are
cumulative—do not count prior repetitions again from every archive.

```sh
python tools/forkless-benchmark/check-results.py \
  tools/forkless-benchmark/results/ACTUAL_RUN_ID
```

The command prints JSON. Exit **0=PASS, 1=FAIL, 2=MISSING**. PASS covers selected
case evidence and recorded cleanup only. It is not a production-readiness claim,
an exact-child-count check, an independent cloud audit or a performance verdict.

If collection was interrupted, retrieve only the exact run's archives and external
receipts from its recorded bucket using authenticated AWS access. Compare each
archive SHA256 to its external receipt, then verify all member checksums with the
existing verifier. Older runners may not have saved receipts locally; retrieve
them rather than generate replacement receipts. Do not extract unverified files
over the repository. Never silently edit evidence to make checks pass.

Inspect per-attempt `result.json`, `metrics.csv`, `client-errors.json`, RDB
validation logs, `telemetry.jsonl` and final `kernel.log`. Check OOM kills and I/O
errors even if a process returned zero. Exact snapshot checks and ordinary full
reload validation are different; identify which actually ran. Raw RDBs are not
retained in these archives. Verbose validation logs can make archives hundreds
of MB; this is a known limitation, not proof that database files leaked in.

## 6. Verify cleanup and report

Verify the **exact** instance is terminated, every recorded benchmark volume is
deleted and its SG is deleted. If the launcher failed, perform only the approved
exact-resource cleanup. Never use account-wide deletion, broad tags or guessed
resource IDs. Respect the original deadline. Retain S3 evidence.

If credentials expire, report cleanup as unverified and request renewed access;
do not claim the timer proves cloud resources were removed. Remove only temporary
credential material created for this run after verified collection/cleanup, not
the user's normal AWS profile or shell configuration. Pause any run-specific
monitor after completion or an input blocker.

Final response must include:

1. Run name/ID, source revision/hash, mode and tested settings.
2. PASS/FAIL/incomplete **per case and per repetition**, including timeout causes.
3. Separate SET/GET (and HSET/HGET) throughput/latency, by phase, where measured;
   BGSAVE response and total duration; missing metrics explicitly labeled.
4. Correctness and cancellation findings, pressure/OOM evidence and limitations.
5. Links to collected results/check report, retained bucket and cleanup status.

Use the comparison rules in the developer quick-start: match settings, preserve
all repetitions, never average percentiles, never compare instrumented fixture
latency with performance latency. No HTML generator is wired into this runner
yet—do not promise an HTML file unless you actually create and verify one.
Creating this runbook, running a benchmark, or producing a report does not
authorize committing/pushing code or launching an additional benchmark.
