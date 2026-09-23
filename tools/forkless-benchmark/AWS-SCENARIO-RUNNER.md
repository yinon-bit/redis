# Configurable AWS string benchmark

New developer? Start with [DEVELOPER-QUICKSTART.md](DEVELOPER-QUICKSTART.md)
for prerequisites, AWS permissions, validation exit codes and comparison rules.

## Automatic AWS setup (no existing instance or bucket needed)

Use **`run-aws-auto.py`** for the hands-off infrastructure workflow. Install
`boto3` in your local Python environment, then export temporary AWS credentials
or use your normal AWS profile. Never save credentials in the scenario JSON.

```sh
python3 tools/forkless-benchmark/run-aws-auto.py \
  --name hybrid-check --random-cases 3 --hours 2
```

This previews a reproducible selection without contacting AWS. Add `--launch`
to create one On-Demand `r7g.2xlarge`, a private encrypted S3 bucket, and a
security group with **no inbound ports**. Defaults: Ohio, owner `yinon_yishay`,
team `redis_core`. No SSH key, SSM role, or IAM-role creation is needed.

The existing entry point accepts the same automatic workflow through `--provision`:

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py --provision \
  --name hybrid-check --random-cases 3 --hours 2 --launch
```

This flag selects the automatic runner's options; omit it to keep the existing
SSM-instance workflow. Automatic custom variables are supplied via `--config`.

Choose specific cases by repeating `--scenario CASE_ID`; alternatively pass
`--config my-custom.json` for variable-driven tests. The same flags for dataset
size/rates remain in that JSON. `--mode fork|hybrid|forkless` can override mode.
The auto runner discovers the default public subnet and latest Canonical Ubuntu
24.04 ARM64 AMI and records the exact IDs. Use `--subnet-id`/`--ami-id` to fix them
or when no default public subnet exists. It never opens Redis to the internet.

The runner archives allowlisted source from the current working tree, including
uncommitted C changes, without editing them. It uploads the frozen source to S3,
builds on the instance, prepares a **new blank** 100-GiB encrypted gp3 data disk,
and executes cases serially. Each documented case has three repetitions.
`--case-timeout` (default1800 seconds) covers the whole case: loading, repetitions
and validation. A case starts only if this allowance plus upload reserve fits.
Anything that cannot fit is explicitly `not_run_deadline`, not silently reduced.

An absolute guest shutdown timer is armed before package installation; instance
shutdown behavior is **terminate**, not stop. Keep the local launcher running
for its independent deadline watchdog, collection, and AWS cleanup. If boot
does not confirm the timer within10minutes, it terminates the instance. A guest
timer alone cannot protect against a guest that never boots: do not close the
launcher before readiness, and preferably keep it running through cleanup.

**Credential expiration matters.** Source download and uploads use scoped
presigned S3 URLs, not credentials on the host. Those URLs stop working when
the signing credentials expire. Use fresh credentials covering the whole run;
optionally supply `--credentials-expire-at 2026-09-23T18:00:00Z` to enforce a known
expiration. If your local credentials expire, automatic cleanup can also fail;
the guest timer remains armed, and `resources.json` records exact cleanup targets.

Outputs go to a new persistent bucket: source archive, manifest, status,
per-case evidence archives/receipts and final evidence. Local copies are under
`results/<run-id>/`; both archive SHA256 and every archived file checksum are
verified. Validated raw RDBs are **not** included in these auto-run evidence
archives; validation logs/results are. Failed/incomplete cases are retained as
such. The result bucket is **retained**, not deleted, and continues to incur
storage charges. Instance/volumes/security group are cleaned up; unresolved
cleanup is explicitly recorded. No second instance is launched to retry failures.

Live AWS validation completed on 2026-09-23: provisioning, collection and cleanup
worked; snapshot correctness, mixed workload and cancellation/retry each passed
three repetitions. The arena-boundary case passed two repetitions, but the third
hit the 30-minute whole-case timeout. This is not a complete suite pass.
Signed URLs use regional endpoints to avoid fresh-bucket redirect failures.

## Existing-instance alternative

This runner executes **one named run on an existing dedicated EC2 instance**.
It reads the normal AWS CLI environment (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) or configured profile. These are
AWS API credentials, not an SSH key. Never put their values in these scripts,
Git, setup files or reports. No SSH key or inbound port is needed.

## Pick a documented case or configure variables

The benchmark document in this repository is
`SHARED-POC-BENCHMARK-SPEC.md` (there is no file named `benchmark.md`).

List its 240 standard and 48 large performance case IDs and their support status:

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py --list-scenarios
```

Preview one exact documented case, without AWS credentials or machine settings:

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py \
  --scenario standard.mixed.rw50.tight.cpu1.first.bgsave
```

Section-6 performance cases now have an executable driver (`document-cases/1`).
It uses exact dataset counts, payload_v1 values, 16 closed-loop clients (zero for
idle), prescribed pipeline/mix, CPU and RAM limits, and three repetitions.
It supports `first`, `subsequent`, and `after_failure`. The latter compiles a
Linux-only fwrite interposer, gates after at least 1 MiB, injects one EIO,
verifies the previous RDB hash, disarms, and measures the retry. Missing gate
arrival or an unexpected success fails the case; it never substitutes killing
Redis. The interposer remains loaded during retry, explicitly recorded as an
instrumentation difference. `subsequent` validates the prime RDB before retry.

This does **not** claim full shared-v2.0 compliance: it uses a shared Python
load generator, different output schema, publication-based fast-save evidence,
and lacks exact child tracing and internal profiling hooks. Every report lists
these deviations. A separate experimental gated fixture driver now covers a
subset of arena transitions, cancellation/retry, and precise snapshot checks
(see below). Diskless fixtures and `all` remain unsupported; they are not silently substituted.

## Gated correctness fixtures (separate instrumented build)

Use `--list-scenarios` to obtain exact IDs from `fixture_cases`. With
`--provision`, the launcher copies the source into a separate build and adds
test-only gates there. The original checkout and the timed performance binary
are not instrumented. This adapter currently supports **fork and hybrid**, not
full forkless or arbitrary source revisions; unmatched source anchors fail closed.
Existing-instance launches deliberately refuse these fixtures because they cannot
verify that an externally supplied binary contains the required hooks.

Example plan (add `--launch` only when ready to create one tagged AWS instance):

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py --provision \
  --name hybrid-correctness --mode hybrid --hours 2 \
  --scenario fixture.arena.string_boundary.during_capture.ample.cpu6 \
  --scenario fixture.snapshot.base.ample.cpu6 \
  --scenario fixture.cancellation.rw50.ample.cpu6
```

This does not guarantee all selected cases fit in two hours. Loading and full
reload validation count toward each case timeout; missing/deadline cases must
remain visible. `--random-cases` still selects performance cases only.

- **Arena: 96 concrete cases.** Eight traces × three gate positions × two RAM
  caps × two CPU limits. Background: 9,000,000 × 256-byte strings, plus 1,024
  targets. One designated target is mutated; the other targets are controls.
  Traces cover string growth/shrink, 511999/512000/512001-byte boundaries,
  APPEND, SETRANGE, hash listpack limits by field length/count, and type replacement.
  Gate positions: before child traversal, during target serialization, and after
  target serialization. During capture, an independent controller releases the
  gate after 50 ms without waiting for a potentially blocked write. Before/after
  visit hold completion until the mutation trace is acknowledged (20-s timeout).
  Check actual encoding/arena,
  the final live target value, and every reloaded snapshot value.
- **Snapshot: one base case.** 100,000 small strings, 32 × 1-MiB strings,
  1,000 four-field hashes. Unique key/version markers distinguish values.
  After the fork cut: overwrite, delete, insert, rename, and update hashes/big
  strings. Verify the live post-write state and separately reload the snapshot
  to require the exact cut-state key set and values—not merely either version.
- **Cancellation: 12 concrete cases.** Standard mixed dataset; idle/write100/rw50
  × ample/tight × cpu6/cpu1. A test-only output gate pauses after at least 1 MiB.
  A test-only `DEBUG bench-cancel` invokes the existing `killRDBChild()` path.
  Record acknowledgement/cleanup latency and write availability, require the
  previous RDB hash unchanged, then retry and structurally/full-payload validate.
  An independent 100-ms release prevents the harness from deadlocking the test;
  reported cancellation latency includes any wait for that release.

All cases repeat three times. Standard limits are 12/5 GiB with Redis on CPUs
2–7 or CPU 2; generator uses CPUs 0–1. These are resource limits, not achieved
RSS targets. Outputs use **gated-fixtures/1 JSON**, with per-attempt results,
events, checks, errors, build hashes, logs and checksum-verified S3 archives.
They are correctness results, not interchangeable with performance schema rows.

Limits: no slow/disconnected replicas, expiry/FLUSH snapshot variants, ungated
all-target arena stress, budget-exhaustion cancellation, or full contract compliance
yet. Small local Linux smoke tests exercise all 56 mode/trace combinations;
AWS snapshot and cancellation fixtures passed three repetitions; one full-size
arena-boundary case passed two before its whole-case timeout interrupted the third.
The other catalogue variants have not all been AWS-tested. Do not infer production
readiness from these results. The initial bootstrap attempt failed before testing;
regional S3 signing fixed the reproduced redirect issue in the subsequent run.

Run a documented case (replace the example instance/bucket):

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py \
  --name live-dict-first \
  --scenario standard.mixed.read100.tight.cpu6.first.bgsave \
  --instance-id i-0123456789abcdef0 \
  --results-s3 s3://YOUR-BUCKET/benchmarks \
  --redis-binary /opt/redis/src/redis-server \
  --data-dir /mnt/benchmark \
  --timeout-seconds 21600 --launch
```

The existing instance must provide CPUs0–7, root SSM execution, enabled/delegated
cgroup-v2 memory/cpu/cpuset controllers, THP=never, overcommit=1, and no host swap.
Redis and descendants are placed in the run's unique constrained cgroup; clients
use CPUs0–1. A missing prerequisite fails rather than silently changing limits.
`--data-dir` must exist on a separate filesystem (not the root disk). The
operator still supplies the specified AMI/instance/EBS settings; these aren't
provisioned by this script. Install a C compiler and libc headers for EIO tests.
The global timeout includes seed creation, all repetitions, and validation; it
does not extend your independently arranged EC2 termination deadline.

Seed generation is untimed and validated. Each repetition reloads a pristine
copy. Every completed save gets structural checking plus a full key/payload
reload outside the constrained cgroup. Failed attempts remain in results.
JSON includes cgroup counters before/after attempts, sampled memory/pressure,
Redis INFO, full config, binary hash, save and command timing. CSV splits traffic
by command and key size. Read misses fail. Probe/client failures fail rather than
being reported as successful throughput. Drain is a separate phase.

To run a configurable experiment, edit a copy of `scenario-example.json`, then:

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py --config my-scenario.json
python3 tools/forkless-benchmark/run-aws-scenario.py --config my-scenario.json \
  --writes-per-sec 50000 --big-key-percent 5 --launch
```

Precedence: built-in defaults < JSON config < explicit CLI flags. JSON uses
underscore field names, CLI uses hyphens. `scenario` defaults to `custom`.
JSON accepts the settings shown in the example and no arbitrary shell commands,
credentials or `launch` field. Launch is always an explicit CLI decision.
Custom cases use `configurable-strings/1`, never a shared-spec case ID.
Dataset/workload overrides on a documented case are rejected: choose `custom`
instead, so modified tests cannot silently compare as the original case.

Examples of available document selections:

- `standard.small_only.idle.ample.cpu6.first.bgsave`
- `standard.mixed.write100.tight.cpu1.first.bgsave`
- `standard.big_only.read100.ample.cpu6.subsequent.bgsave`
- `large.big_heavy.rw50.tight.cpu1.first.bgsave`

Each dot-separated ID fixes tier, dataset, workload, memory, CPU, lifecycle and
operation. A heading/family in the document is not automatically one case.

## Prerequisites

- Local Python 3.9+ and AWS CLI, authenticated with permission for EC2 describe,
  SSM describe/send/get-command-invocation, and scoped S3 input uploads/downloads.
- An **existing, dedicated** Linux EC2 instance with `owner` and approved `team`
  tags, online Systems Manager agent, Python 3.9+, AWS CLI, GNU `timeout`,
  `sha256sum`, and a built `redis-server` plus adjacent `redis-check-rdb`.
- Its instance role must permit SSM and S3 reads/writes within your benchmark
  bucket prefix. The worker gets **no copy of your exported credentials**.
- Enough RAM for values **plus Redis overhead, clients, snapshot copies and
  file cache**; enough disk for an RDB and logs. Choose and enforce instance
  CPU/memory limits separately for **custom runs**. Documented performance
  cases enforce the selected cgroup limits as described above.
- Arrange your instance's termination deadline separately. This script neither
  creates nor terminates instances, changes firewall rules, builds Redis, nor
  installs dependencies. It does not extend a pre-existing termination timer.

## Default scenario and example

Default: hybrid, roughly **1 GiB of value payload**, 256-byte small strings,
1 MiB big strings, 0.02% big keys by **key count**, 0.02% big requests,
50,000 offered SET/s, no GETs, 32 clients per enabled operation, pipeline 1.
15 seconds before one BGSAVE, pressure throughout the save, then 5 seconds
afterward. Compression and checksum on; AOF off; maxmemory 0; writes overwrite
existing keys. Both initial and replacement payloads are seeded pseudorandom
bytes, reused across keys in each size class. This is not a unique-value corpus.

After exporting credentials in your terminal, **preview** with:

```sh
python3 tools/forkless-benchmark/run-aws-scenario.py \
  --name live-dict-r2 \
  --instance-id i-REPLACE \
  --results-s3 s3://YOUR-BUCKET/benchmarks \
  --redis-binary /opt/redis/src/redis-server \
  --dataset-gib 4 \
  --big-key-percent 5 \
  --big-request-percent 5 \
  --writes-per-sec 50000 \
  --reads-per-sec 50000
```

Replace the instance ID and bucket, inspect the calculated counts, then append
`--launch` to dispatch. Without `--launch`, **no AWS calls happen**.

`--big-key-percent 5` means 5% of **keys**, not bytes. With 1 MiB big values,
almost all payload bytes will then be in big keys. Counts are calculated from
the requested payload budget, so increasing the percentage does not silently
multiply a fixed key count into a much larger database. Actual rounded counts,
percentage and bytes are in setup.json.

`--big-request-percent 5` independently makes 5% of each operation target big
keys. `--writes-per-sec 50000 --reads-per-sec 50000` requests 100k total ops/s.
Rates are global across clients, not per client. Idle: set both rates to zero.
Read-only: set writes to zero. Pure small: set both big percentages to zero.
Pure big: set both to 100. Use `--mode fork` for the baseline binary.

Use separate unique names for revisions. Multiple runs are independent; wait
for one to finish before starting the next **on the same instance**. There is
no automatic batch sequencer or concurrent-run lock; a port conflict is refused.
An SSM dispatch acknowledgement means **started**, not successfully completed.

## Status and results

The launcher prints a command ID and saves local `results/<run-id>/dispatch.json`.
Check it with:

```sh
aws ssm get-command-invocation --region us-east-2 \
  --instance-id i-REPLACE --command-id REPLACE
aws s3 cp s3://YOUR-BUCKET/benchmarks/RUN-ID/artifacts/ ./downloaded-run/ --recursive
cd downloaded-run
sha256sum -c SHA256SUMS
```

S3 layout: `<prefix>/<unique-run-id>/input/` and `artifacts/`.
Artifacts include `output/result.json`, `output/metrics.csv`,
`output/telemetry.jsonl`, `output/dump.rdb`, structural RDB-check log,
Redis/runner logs, setup, worker source, exit code and SHA256SUMS.
For documented cases, per-repetition metrics/logs/RDBs are under `output/r1/`,
`output/r2/`, and `output/r3/`, with an aggregate `output/result.json` and a
validated `output/seed/`. Kernel journal capture includes its own exit status;
failed journal access is missing evidence, never proof of no kernel OOM.
The JSON includes instance/AMI information and the measured binary SHA256;
record your source revision in the run name or associated build records.

Metrics: separate SET/GET before/save/after throughput; response latency
p50/p95/p99/p99.9/max; errors; skipped scheduled requests; full BGSAVE response
time; observed save time; INFO snapshots with memory, fork and CoW fields.
Quantiles are upper bounds from ~1% logarithmic buckets; maximum is exact.
Latency cohorts use **send phase**, including responses crossing phases;
throughput uses **completion phase**. The post-save phase includes the bounded
drain of in-flight requests, so its actual duration can exceed the requested
post-save interval. Rates count successes only.

This is a best-effort paced generator: when a client cannot keep up, overdue
slots are skipped and counted rather than creating an unlimited backlog.
Achieved throughput may be much lower than requested. Python and same-host
client CPU can bottleneck; do not call this a certified 50k/s open-loop test.
Population currently uses sequential SETs and can take significant time.

## Custom-worker scope and failure handling

The custom worker is **configurable-strings/1**, NOT the complete shared v2.0 suite. It does
not implement hashes/arena transitions, CPU/RAM limit provisioning, diskless
replication, cancellation fixtures, full reload/point-in-time validation,
exact child counting, kernel/OOM attribution, profiler correlation, automatic
comparison or HTML generation. Do not compare its numbers directly with a
different harness or label a forkless run zero-child based on this report.

Remote timeout defaults to 1800 seconds and includes loading. On ordinary
failure or timeout the wrapper attempts to upload partial artifacts; upload
failure marks the SSM command failed. An OOM-killed host, terminated instance,
unavailable bucket or lost IAM access can prevent upload. Retrieve remaining
files from the reported `/var/tmp/redis-scenario-<run-id>` directory if needed.
The worker stops only its own Redis subprocess; unrelated Redis is untouched.
Artifacts and local/remote result directories are retained, not auto-deleted.
Do not run this on production. Budget/instance lifetime is your responsibility.
