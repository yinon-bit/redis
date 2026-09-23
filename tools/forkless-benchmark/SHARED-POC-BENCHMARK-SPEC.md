# Shared Redis persistence POC benchmark — specification v2.0

Status: benchmark execution runbook. The selected suite MUST be implemented and
run; writing this document alone does not complete the benchmark. Track actual
execution and evidence in the manifest; never claim unexecuted cases passed.
Purpose: execute selected cases against one user-supplied setup and produce one
independent, named benchmark report. The runner is implementation-agnostic:
Frozen Dict, Live Dict, classic fork and future implementations are labels, not
hard-coded roles. Comparing completed reports is a separate operation.
Normative words **MUST**, **MUST NOT**, and **SHOULD** are requirements, prohibitions,
and recommendations. A deviation requires a new experiment ID and must be visible
in the report. Never silently substitute a smaller database or lighter workload.

## 1. Independent run contract

### Start here: execution checklist

1. Supply benchmark name, immutable build/revision and complete setup.
2. Select named batches below; expand and approve the case catalog and run duration.
3. Freeze shared generator, seeds, settings, expected outcomes and output schemas.
4. Implement missing runner features and pass tiny smoke/correctness gates.
5. Execute case repetitions for that setup; preserve every failure and unsupported case.
6. Validate, upload to persistent S3, verify checksums, then publish its report.
7. Remove temporary resources; retain S3 results and cleanup evidence.

Batch names and scope:
- `core`: section6 standard-tier matrix (all workloads, CPU/RAM profiles and lifecycles).
- `large`: section6 large-tier matrix.
- `arena`: section7 arena-transition matrix.
- `extended`: other section7 correctness, robustness and capability fixtures.
- `profile`: section8 separate instrumented profiling cases.
- `all`: alias for the union of core,large,arena,extended,profile. Expand then
  deduplicate by case_id; each case executes R times, not once per alias occurrence.
  `all` selects cases, never additional builds, implementations or machines.

A normal performance campaign selects `core`; add other batches explicitly.
All batches remain required for a **full-suite** claim. Finishing a selected batch
is not the same as passing it. Unsupported or failed cases remain visible.

- Freeze one source archive and binary for the run before starting. Record Git
  revision, dirty patch SHA256, source archive SHA256, binary SHA256, compiler,
  linker flags, jemalloc version, build image digest, and full effective config.
- `benchmark_name` is the user-facing name, e.g. "Live Dict — allocator update".
  `run_id` uniquely identifies an execution; repeated names are permitted but must
  be resolved to exact run IDs before comparison. `build_id` identifies the frozen
  artifact; `revision` and dirty patch hash distinguish source changes.
- `setup.json` supplies machine/storage, CPU/RAM profiles, config, artifact hashes,
  seeds, adapters, batches, R and S3 destination. Section2–5 settings are the default
  standard setup template; explicit overrides produce a different setup hash.
  Missing required values block preflight. Never silently infer a comparison build.
- The runner needs only this setup. No reference candidate, second POC or baseline
  report is required to run and report results. Test-support artifacts (seed
  producer, replica, expiry oracle) are explicit dependencies, not measured builds.
- Map implementation-specific config/capabilities through the setup's adapter;
  unsupported cases remain visible. Do not assume shared option or arena names.
- Use **one shared load generator binary**, validator, telemetry collector and
  scenario driver version for comparable runs; record their hashes. Do not let each POC
  implement its own benchmark generator.
- Each report records actual dataset bytes, operation sequence algorithm, machine
  type, memory limit, disk settings, CPU allocation and instrumentation. Later
  comparison checks these fields; names alone never establish comparability.
- Correctness comes first. Unsupported cases are `unsupported`, not passing or
  missing. An unsupported mandatory case prevents claiming full suite completion.
- Run serially on one host. No overlapping measurements, builds, backups, validation,
  compression, or profiling postprocessing during a measurement window.
- R defaults to3 per case, independent of how many reports may later be compared.
  Freeze R and case order before execution. R<3 is permitted only as an explicitly
  named smoke/exploratory run, not full standard coverage. Every fixture uses R.
  Record repetition/session timestamps. A separately authorized orchestrator may
  interleave independent named runs on one host for stronger paired evidence;
  otherwise report comparisons are observational, not randomized paired trials.
- Preserve every attempt. Harness retries get new `attempt_id`; never overwrite
  a failed result. Do not rerun until obtaining a favorable measurement.

## 2. AWS and operating-system contract

These are requested experiment settings, not live availability or price claims.

- Region `us-east-2`; On-Demand `r7g.2xlarge`, ARM64, 8 vCPUs, 64 GiB RAM.
  No Spot, burstable instances, or architecture substitution.
- One instance at a time. Ubuntu24.04 ARM64, with an **exact AMI ID and kernel
  version frozen in the manifest before launch**. All paired runs use that image.
- One dedicated encrypted gp3 data volume:100 GiB,3000 IOPS,125 MiB/s, ext4,
  `noatime`, no custom I/O throttle. Root disk must not contain benchmark RDBs.
  Record volume IDs, filesystem/mount flags, free space and EBS settings.
- CPU allocation: Redis parent, all its threads and children on CPUs2–7;
  load generator, PING probes and telemetry on CPUs0–1. Redis cgroup quota600%,
  period100000us. Default scheduler and nice0; no priority experiment in this suite.
- CPU profiles: `cpu6` is the allocation above; `cpu1` pins **all Redis threads
  and children together to CPU2**, with aggregate quota100%, period100000us.
  Clients/probes remain on CPUs0–1. This tests one CPU available to Redis, not
  a one-vCPU EC2 machine. Do not give each helper/child its own100% quota.
  Same instance, dataset, clients and pipelines for both CPU profiles. Record
  cpuset and quota of every descendant; no priority change between profiles.
  Replication source+replica share this same aggregate quota in cpu1 fixtures.
- cgroupv2: Redis and **all descendants** in one measured cgroup. Clients are
  outside it. `memory.swap.max=0`, host swapoff, `memory.high=max`,
  `memory.oom.group=0`; retain cgroup counters after Redis exits. No oom-score tuning.
- THP `never`, `vm.overcommit_memory=1`; record all relevant sysctls, CPU governor,
  kernel, lscpu, NUMA topology and CPU steal. Do not change them between candidates.
- Build outside measurement. Freeze build image by digest, not mutable Ubuntu tag.
  Same optimization/LTO/debug settings. Do not compare profiled debug A to release B.
- Redis binds127.0.0.1. Never expose6379 publicly. SSH only from approved /32 or SSM.
- Required tags: `owner=yinon_yishay`, `team=redis_core`, plus experiment ID/purpose.
- Set an independently enforced absolute terminate deadline within the approved
  rental duration. Never extend without approval. Do not start a case unless its
  timeout + validation + upload allowance fits. Mark remaining cases `not_run_deadline`.
- A six-hour allocation is **not** a promise to finish the full suite. Split into
  explicitly numbered sessions with identical manifests. Do not shrink the matrix.

## 3. Redis configuration

Common config, explicitly applied and exported with `CONFIG GET *`:

```text
save ""
appendonly no
maxmemory 0
maxmemory-policy allkeys-lfu
lfu-decay-time 1
activedefrag no
rdbcompression yes
rdbchecksum yes
rdb-save-incremental-fsync yes
io-threads 1
io-threads-do-reads no
hz 10
dynamic-hz no
lazyfree-lazy-eviction no
lazyfree-lazy-expire no
lazyfree-lazy-server-del no
stop-writes-on-bgsave-error no
latency-monitor-threshold 1
```

Use DB0, no TTLs/modules/cluster/TLS unless the named fixture requires them.
Keep Redis defaults not shown here equal by using the same base config and hashing
its effective export. Record any POC-required differences explicitly.
For hybrid implementations, eligibility cap is512000 bytes; small strings and
listpack hashes eligible, big strings in the fork path. Disable snapshot-budget
limits for ordinary performance cases; test budgets separately. Do not silently
disable a POC feature that has a performance cost.

`maxmemory=0` means the tight-RAM tests measure **kernel/cgroup pressure**, not
Redis eviction. Do not describe kernel OOM as an eviction-policy failure.

## 4. Exact datasets

Units: KiB=1024 bytes, MiB=1048576 bytes, GiB=1073741824 bytes.
DB size is defined by exact key counts and payloads, **not a target RSS**.
Allocator overhead differs between POCs: record it; never change counts to equalize RSS.

| Dataset ID | 256-B string keys | 1-MiB string keys | Listpack hash keys |
|---|---:|---:|---:|
| `small_only` | 11400000 | 0 | 0 |
| `big_only` | 0 | 3276 | 0 |
| `mixed` | 9600000 | 410 | 120000 |
| `small_heavy` | 11400000 | 16 | 0 |
| `big_heavy` | 128 | 3276 | 0 |

`mixed`, `small_heavy`, `big_heavy` retain the historical key counts. They are
roughly4GiB **allocated**, not4GiB payload. Compute payload totals exactly as
`small_count*256 + big_count*1048576 + hash_count*4*32`; keys/fields/metadata are extra.

- Small names: `small:%09u`; big: `big:%06u`; hash: `hash:%07u`, zero-based IDs.
- Each hash has exactly four fields `a,b,c,d`,32 bytes each; assert listpack
  encoding on every hash during seed validation. No accidental hashtable conversion.
- Initial string/hash values are version0; write hammer overwrites with version1.
  Byte generation MUST be identical for both POCs, not merely equal lengths.
- Freeze the existing `bench.c` payload generator as `payload_v1`: unsigned64-bit
  xorshift (`s ^= s<<13; s ^= s>>7; s ^= s<<17`, wrapping at64 bits), seed
  `192837 + version*98765`; each big-value byte is low8 bits of next output.
  Small value:first128 bytes of big value +128 repeats of ASCII `x+version`.
  Each hash field:value bytes256..287 of that version's big value.
- These large values are shared across keys but pseudorandom within each value.
  Do not replace with repeated `A` bytes; that changes compression drastically.
- Seed is supplied as an immutable artifact or created by `seed_producer_build_id`
  (default: this run's build), fully validated, then cloned byte-for-byte
  into each fresh case directory. Record seed SHA256, DBSIZE, counts by type,
  encoding counts and `used_memory`/RSS after load. Seed creation is not timed.
- If RDB formats are incompatible, generate identical logical seeds separately and
  compare a canonical logical digest; document the incompatibility. Never reuse
  a mutated live database as the seed for the next candidate.

### Large-DB tier

Required separately: multiply **all key counts by5** for `mixed`, `small_heavy`,
`big_heavy`; payload sizes remain unchanged. IDs end at new count-1.
Use the same instance and CPU settings, memory caps40GiB ample /22GiB tight.
These are roughly20GiB allocated datasets. A seed that cannot load is an explicit
`load_oom`, not a reason to reduce the data. Capture actual allocated bytes.

## 5. Workloads: exact writing hammer

Four workloads, not interchangeable:

| Workload | Read/write mix | Clients | Load |
|---|---|---:|---|
| `idle` | none | 0 | Only control connection and latency probes |
| `write100` | 0% reads /100% writes | 16 | Closed-loop, unthrottled |
| `read100` | 100% reads /0% writes | 16 | Closed-loop, unthrottled |
| `rw50` | 50% reads /50% writes | 16 | Closed-loop, unthrottled |

- Persistent connections; RESP2, TCP loopback, TCP_NODELAY. No reconnect hiding:
  count reconnects, errors, timeouts and disconnected commands separately.
- `idle` is a workload, not an outcome status: a case can be idle and pass,
  idle and failed, etc. Record workload explicitly. Probe/control traffic still
  exists. `read100` uses only pre-existing keys; expected misses=0. GET may update
  LRU/LFU metadata, so read-only traffic is not necessarily free of snapshot CoW.
- Pipeline16 for `small_only`, `mixed`, `small_heavy`; pipeline1 for `big_only`,
  `big_heavy`. Do not call pipeline latency command execution time.
- Key category probabilities:
  - `small_only`:100%small; `big_only`:100%big.
  - `mixed`:98.98%small,1%hash,0.02%big.
  - `small_heavy`:99.98%small,0.02%big (missing hash share goes to small).
  - `big_heavy`:2%small,98%big.
- Select existing key ID uniformly within chosen category. Small/big write=SET
  entire value; hash write=HSET all four fields. Reads=GET or HGET field `a`.
  No inserts/TTL/DEL in ordinary performance tests. No random variable-sized values.
- Freeze shared generator PRNG draw order. Per-client seed is
  `1234567 + client_id*314159`, client IDs0..15, reset per attempt.
  Category draw first, key-ID second, read/write third (`rw50` only). Version1
  writes in performance tests. Store generator hash and RNG ID in manifest.
- Probabilities are per operation, not per byte. Report actual counts per command
  and size. Never combine 1MiB GET with256B GET into the primary throughput number.
- Unthrottled means completed operation counts differ across candidates; identical
  seeds do not imply identical operations happened before BGSAVE in wall time.
  Use a controlled trace for point-in-time correctness, not this random hammer.
- Measure15s pre-save warmup/baseline under the selected workload, then BGSAVE,
  continue unchanged through completion/failure and5s afterward. No sleeping
  clients during fork/startup/cleanup. Idle has the same15s/5s windows.
- These windows deliberately supersede older5s write-only warmups/2s tails.
  Old results may appear as historical context, not strict paired v2.0 comparisons.

## 6. Performance matrix and lifecycle

### What is one case?

A heading is not automatically one runnable case. Sections1–5 and8–12 define
shared rules; section6 defines matrix families; section7 defines fixture families.
Each distinct combination of dataset, workload, memory cap, CPU profile, lifecycle,
scenario and variant is **one case**, expanded to a concrete `case_id` before launch.
A run/repetition executes that case; an attempt is an execution or retry.

Examples:
- `standard.mixed.rw50.tight.cpu1.first.bgsave` is one case.
- Changing rw50 to read100 creates another case.
- Running the same case in "Live Dict — previous" and "Live Dict — improved"
  creates two independent reports, not different workloads. Case IDs remain equal;
  run IDs and revisions distinguish their results.
- Arena migration is a family: grow/shrink/boundary and each gate position are
  distinct cases, not one combined result.

Export an expanded `case-catalog.json` before starting: each row contains case ID,
description, exact settings, expected outcome, dependencies, timeout, selected
setup hash, repetition count and required artifacts. `planned_cases` references
this catalog. Fail preflight for duplicate IDs or undefined fixture parameters.
Run only an explicitly selected catalog/batch; report excluded families as outside
that batch's scope, not passing. Completion means every selected execution has
an outcome and uploaded evidence; full-suite completion also requires all families.

Catalog shape (all rows must be expanded; no wildcards/ranges at runtime):

```json
{
  "catalog_version": 1,
  "batches": ["core"],
  "cases": [{
    "case_id": "standard.mixed.rw50.tight.cpu1.first.bgsave",
    "batch": "core",
    "dataset_id": "mixed",
    "workload": "rw50",
    "memory_limit_bytes": 5368709120,
    "cpu_profile": "cpu1",
    "lifecycle": "first",
    "scenario": "bgsave",
    "variant": "default",
    "expected_outcome": "snapshot_complete_and_valid",
    "repetitions": 3,
    "save_timeout_s": 600,
    "validation_timeout_s": 600
  }]
}
```

This is one illustrative row, not the full catalog. The runner MUST materialize
all selected rows, resolve dataset/config/trace hashes, and validate counts against
the dimension products before allocation. Required case-specific parameters and
expected outcomes cannot be left as prose placeholders. Include `trace_sha256`,
`effective_settings_sha256`, `dataset_sha256` and `required_artifacts` in actual rows.

Standard tier:5 datasets ×2 caps(12GiB,5GiB) ×4 workloads ×2 CPU profiles
×3 lifecycle cases = **240 cases per run before repetitions**.
Total240×R attempts; default R=3 gives720 attempts for one named run.
This explicitly includes idle/tight/cpu1 and every hammer/tight/cpu1 combination.

Lifecycle cases are separate fresh processes loaded from pristine seed:

1. `first`: first snapshot after startup; no priming BGSAVE.
2. `subsequent`: complete/validate one untimed BGSAVE without hammer, keep process
   alive, wait5s, then run baseline and second BGSAVE under selected workload.
3. `after_failure`: start an untimed save; common test gate pauses snapshot output
   after first1MiB, inject one EIO on the next output write, release gate. Confirm
   failure and complete cleanup; previous seed RDB unchanged. Wait5s, then baseline
   and measured retry. All selected builds need the same injection semantics.
   For `empty_db` only, inject EIO on the **first output write**, with no1MiB
   threshold. Record `fault_trigger=first_output_write`; ordinary datasets use
   `fault_trigger=after_1048576_output_bytes`. A trigger not reached is a harness
   error, not an expected save failure. Require injected failure within30s of gate
   arrival and verify the retry runs with injection disabled.

If deterministic I/O injection is unavailable, this case is `unsupported`; do not
substitute killing different internal processes in the two implementations.
Never leave a barrier or fault injector active in a normal performance case.

Large tier:3 datasets ×2 caps(40,22GiB) ×4 workloads ×2 CPU profiles ×`first`
= **48 cases per run before repetitions**:48×R attempts, default144.
These counts exclude robustness fixtures and profiling. The full matrix is a
multi-session campaign, not a single six-hour rental. A smoke/subset run must list
its selected case IDs and must not be called full coverage.

Per measured save timeout600s; full validation timeout600s; timeout is a failure,
not a600s completed save. First prove fixtures/injection on a tiny smoke dataset.
No skipped test counts as a pass; gate failure and exclusion must be disclosed.

## 7. Required robustness/correctness fixtures

Each fixture uses R repetitions. Retain all attempts and unsupported outcomes.
There is no implicit dataset/workload cross-product in this section. Use exactly
the settings named in each fixture; unspecified dimensions must block catalog
generation rather than inherit settings from a different subsection.
All fixtures use a fresh process, first save, DB0, standard payloads and section3
config unless explicitly overridden. Ordinary save/validation timeout600s applies;
special cancellation/replication timeouts override it only where stated.

### Arena migration: both directions, size boundaries and encoding changes

Terminology: `small_arena` means the hybrid/forkless-eligible arena; `big_arena`
means the fork arena. Routing is **not just object size**: a small hashtable hash
may belong in the fork arena. Record the real routing predicate for each POC.
Classic fork is the command/latency/correctness control; arena counters there are
`not_applicable`, not fabricated migrations.

Use a separate `arena_transition` fixture: fixed keyspace background of9000000 ×256B
strings plus1024 target keys. Target strings use the payload_v1 pseudorandom byte
stream truncated to exact length, unique version markers, and names
`transition:%06u`. The first16 bytes encode target ID and version as two unsigned
64-bit little-endian integers, overwriting those bytes without changing length.
Use12/5GiB caps, cpu6/cpu1,R repetitions, fresh process per
subcase. Target dataset manifests/digests must include initial shape and size.
Transitions are fixed traces, not substitutions into the ordinary random hammer.

Required subcases (all six):

1. `string_grow`: each target256B→1048576B via SET, small→big.
2. `string_shrink`: each target1048576B→256B via SET, big→small.
3. `string_boundary`: initial511999B, then512000→512001→512000→511999B.
   With inclusive cap512000, the middle steps must cross both ways.
4. `string_inplace`: initial511999B; APPEND2bytes (512001B), then SET256B.
   Separate variant uses SETRANGE offset512000 with1byte. Assert no bypass of
   mutation capture or arena reclassification through in-place APIs.
5. `hash_encoding`: set hash-max-listpack-entries512 and value64. Start4fields
   ×32B; HSET fifth field65B forces hashtable. HDEL that field is **not assumed**
   to restore listpack: assert actual encoding/routing. DEL+recreate4×32B must
   return to eligible encoding. Separate variant crosses512→513 fields of32B.
6. `type_replace`: target string256B→SET1MiB→DEL+HSET4×32B→DEL+RPUSH one32B
   item→SET256B. Covers small→big→small→fork-only type→small and pointer reuse.

Ungated cycles MUST restore the initial shape before repeating:
- grow: SET1MiB (forward), SET256B (reset).
- shrink: SET256B (forward), SET1MiB (reset).
- boundary: the listed trace already returns to511999B; no extra reset.
- inplace/SETRANGE: execute listed forward trace ending at256B, then SET511999B
  (reset), before the next APPEND/SETRANGE.
- hash: DEL+recreate restores4fields for the65B variant,512fields for the513-field
  variant. Field names are `f0000`..`f0511` for the latter; add `f0512` on growth.
- type_replace: final SET256B restores the initial type/size.
Count **all commands including resets** in total throughput/latency and memory;
also split forward/reset metrics using `transition_step_role`. No hidden reset
outside the measured window. Finish the current command at save end, stop issuing
new transitions, and record the final partial-cycle position.

For each subcase run three serializer positions, with a common semantic test gate:
`before_visit` (snapshot cut taken, target not captured), `during_capture` (serializer
holds target), `after_visit` (target fully captured). Issue mutation asynchronously;
in during_capture hold gate50ms then release independently of mutation completion.
Never deadlock the test by waiting for a blocked command before releasing it.
Gate one designated target, ID0, per gated attempt; other1023 targets are unchanged.
This avoids assuming dict traversal visits keys in numeric order. For classic
fork, gate the child serializer; for a POC, gate whichever serializer owns that
target. `after_visit` also holds completion until mutations finish. Execute the
forward trace once, without reset, in gated attempts. Record latency
per command and per direction. This injected50ms is explicitly labeled and must
not be merged into ungated latency results. Idle background while gated.
Compare saved content to exact snapshot-cut target values; compare live content
to final values. Validate routing after each command, not just OBJECT ENCODING.

Add **ungated oscillation stress** per subcase, under idle/read100/write100/rw50
background load using small-only background keys. Use one additional transition
connection, pipeline1, cyclic target order0..1023, repeat full trace until save
ends. All candidates have identical worker count (16 background +1 transition,
or just1 for idle background). Run15s baseline, save,5s tail; initial transition
driver starts at BGSAVE send, so baseline has no transitions. Pure idle/no-transition
performance is covered separately. Maximum live sizes are bounded by1024 targets;
snapshot retention is not bounded by that live-size cap and must be measured.

The primary six subcases have6×(3 gated positions +4 ungated backgrounds)×2caps
×2CPUprofiles=168 cases per run. SETRANGE and513-field variants add56;
total224 before repetitions,224×R attempts (default672 per run).
Record `variant`, `serializer_position`
and `background_workload` so no subcase is conflated with another.

Required evidence: actual old/new arena IDs or equivalent debug routing check,
allocation/copy/retained/tracking bytes, migrations both directions, per-command
latency, invalid/double frees, RDB correctness, and post-save cleanup completion.
Encoding alone does not prove arena migration. If routing instrumentation or a
gate is absent, mark coverage unsupported rather than infer correctness.
On supported sanitizer builds, rerun the small deterministic trace under ASan/UBSan
separately; do not use sanitizer timings as performance results.

### Additional missing-case coverage

These are separate named fixtures, not silent changes to the primary matrix:

- `hot_key_contention`: standard mixed,12/5GiB,cpu6/cpu1,write100/rw50, first save,
 R repetitions. 90%of small-key operations target IDs0..99,10%uniform rest; other
 categories unchanged. Uniform random traffic alone can hide same-key contention.
- `incremental_mutation`:1024 target keys, no background dataset/hammer,12GiB,cpu6,
 R repetitions, three gated serializer positions as above, target0only mutated.
 Each trace is a separate case: (a) SET initial256B, then APPEND ASCII `zz`, then
 SETRANGE offset0 ASCII `Q`; (b) SET initial ASCII `42`, then INCR, then APPEND
 ASCII `x` (live result `43x`); (c) HSET initial `a=0,b=32x,c=32x,d=32x`, then
 HINCRBY a1, then HDEL b; (d) same initial hash then HPEXPIRE key60000 FIELDS1 a
 while gate held. Confirm absolute field deadline bounded by command send/reply
 time+60000ms. Record encoding rather than assume listpack-ex in every build.
 Snapshot must retain pre-cut values/no field expiry, live DB reflects full trace.
 Unsupported field-expiry command is an explicit capability outcome for(d).
- `empty_db`: DB0empty, idle,12GiB,cpu6/cpu1, all3 lifecycles,R repetitions. Validate
 empty RDB and no stuck completion/join even when serializer receives no keys.
- `multi_db`: DB0/1/15 each10000 ×256B values, identical key names but distinct
 per-DB payload markers;12GiB,cpu6, rw50 uniformly across the3DBs, first save,
R repetitions. Verify DB selection and no misplaced/duplicated records.
- `disk_full`: mixed12GiB,cpu6,write100,R repetitions; private per-case filesystem
 with free space limited to64MiB beyond existing seed RDB. Expect ENOSPC, preserve
 previous RDB, restore space, retry/validate. Never fill the host/root filesystem.
- `slow_disk`: mixed12/5GiB,cpu6,write100, first save,R repetitions; source cgroup
 output write cap16MiB/s on the dedicated data device via io.max, no read cap.
 Record actual limiter/accounting; retry as unsupported if buffered writeback
 cannot be charged correctly. Exposes retention/cleanup under storage backpressure.
- `replica_disconnect_retry`: mixed12GiB,cpu6,write100,R repetitions; close only the
 replication connection after1MiB transfer, restart replica connection, verify
 convergence, full-sync attempts, no orphan serializer/thread. Ordinary slow
 receiver success does not prove retry correctness.
- `save_overlap`: mixed12GiB,cpu6,write100,R repetitions; hold output at1MiB, issue
 second BGSAVE and BGREWRITEAOF separately, verify documented refusal/queueing,
 release gate, verify single valid completion and no stuck state. No need to
 demand identical error text if the documented behavior is equivalent.

Before implementing an additional fixture, freeze its command trace/seed and
expected outcome in planned_cases. These priorities do not authorize broader
production workloads or new machines. Preserve the explicitly separate core suite.

### Diskless replication, including slow receiver

- Dataset matrix:mixed,small_heavy,big_heavy; cpu6; R repetitions; fresh source
  and replica per case. Source uses this run's build. Replica uses the explicitly
  supplied `replica_build_id` (default:this run's build). Later strict source-only
  comparisons require identical replica build/config; otherwise label whole-system
  comparisons. The replica artifact is a dependency, not a second measured run.
- Source12GiB cgroup, replica separate12GiB cgroup; both inside one6-CPU parent
  cgroup on CPUs2–7. Source workload clients remain on CPUs0–1. This topology is
  separate from persistence comparisons, not mixed into their throughput table.
- `repl-diskless-sync yes`, delay0, replica diskless load disabled, repl-timeout300s;
  fixed replication backlog256MiB. Same output-buffer limit512MiB hard/256MiB soft60s.
- Dedicated replication connection
  limiter16MiB/s, burst1MiB, no added RTT/loss. Do not throttle workload sockets or
  entire host. Record measured receiver bytes/s and limiter configuration.
- Three distinct scenario IDs, not one ambiguous "slow receiver" result:
  1. `normal_sync`: no limiter; each of idle/read100/write100/rw50. Start replica
     after15s baseline; run workload until sync completes or300s after connection,
     whichever first, then stop issuing commands and drain outstanding requests.
  2. `slow_sync_converge`:16MiB/s limiter, idle/read100 only. No writes during sync,
     so this measures bounded snapshot transfer, not unbounded command backlog.
  3. `slow_sync_pressure`:16MiB/s limiter, write100/rw50. Run hammer for exactly30s
     after replica connection, then stop/drain it. Record transferred RDB bytes,
     output-buffer peak and any disconnections/full-sync retries during pressure.
     A pressure disconnect is an allowed observation, not automatically corruption;
     preserve it and require eventual convergence after writes stop.
- Allow1200s total from replica connection, including catch-up/retry. Completion
  requires online replica and matching offset after writes drain; verify exact
  key counts and canonical logical digest. Read probes may continue. A candidate
  that never converges fails, even if a disconnect under pressure was allowed.
- `replica_disconnect_retry` uses this topology without limiter; force a single
  disconnect at1MiB, stop writes30s after initial connection, and use the same
 1200s convergence criterion. Never erase the initial sync failure from metrics.
- Count full-sync retries/disconnects; retain source and replica logs. A successful
  second sync does not erase a failed first sync. No claim that AOF performs replication.

### Explicit cancellation and retry

- Cases:mixed,small_heavy,big_heavy × idle/read100/write100/rw50;12GiB,cpu6,R.
- Pause output at1MiB using the common test gate, request cancellation via each
  candidate's documented API, timestamp request/ack/serializer-exit/cleanup-end.
- Release gate immediately after request dispatch. Continue probes and writes.
  Measure cancellation while busy; do not wait until save has already finished.
- Previous valid RDB SHA256 must remain identical. Retry after cleanup, within
  same Redis process, and validate resulting RDB. Cancellation timeout30s.
- Report whether acknowledgement is quick **and** whether main-thread availability
  remains good during deferred cleanup. No API means `unsupported`, not success.

### Snapshot budget exhaustion

- Cases:mixed,12GiB,cpu6,R; one sequential mutation connection,pipeline1,
  no ordinary random hammer, plus continuous PING. This is `budget_write_trace`,
  not the sixteen-client `write100` workload. Budget-trigger deadline30s.
- Selected POCs: retained-snapshot budget64MiB, mixed dataset. At snapshot cut,
  before any target is captured, arm a test gate preventing snapshot capture and
  deferred-drain consumption of small IDs0..524287. Keep command processing and
  budget checks runnable; do not hold a mutex required by the main thread. Merely
  pausing output is insufficient: buffered serializers could still capture targets.
  Verify all524288 targets are uncaptured and the gate is armed before mutating.
- One writer overwrites those keys once each with version1, ascending ID. Their
  old payload alone is128MiB, exceeding64MiB if retained. Stop on budget abort,
  target exhaustion or30s. Record actual retained bytes and abort reason.
  An independent controller releases the gate on observed abort or at30s,
  regardless of command replies; abort cleanup must never depend on a held gate.
- If budget abort does not occur, mark assertion failed with `budget_not_triggered`
  and distinguish target exhaustion, timeout or unexpected early capture. If the
  implementation cannot hold targets without blocking writes, mark this fixture
  unsupported; do not relabel an output-only pause as equivalent coverage.
- Budget definition must include cloned/retained values; report whether tracking
  and encoded buffers are included. If budgets differ semantically, flag unpaired.
- Report trigger bytes, peak overshoot, abort/cleanup latency, write errors and
  PING tails. Previous RDB unchanged. Disable budget and retry/validate.
- Repeat with stop-writes-on-bgsave-error yes and no, with `save "86400 1"` for
  this fixture only. With yes, report intentional MISCONF separately from stalls.
- Classic fork without snapshot budget is `not_applicable`, never synthesize one.

### Point-in-time, mutation, expiry and bulk operations

- Cases:12GiB,cpu6,R,idle background; separate scenarios base/expiry/flush_sync/
  flush_async. One mutation connection,pipeline1, no random hammer.
- Deterministic fixture:100000 ×256B strings +32 ×1MiB strings +1000 hashes.
  Store unique per-key version/checksum (not the shared performance payload).
- Obtain a test-only barrier immediately **after snapshot cut** and before traversal.
  Start an asynchronous mutation worker: update first1000small keys, delete
  next1000, add1000new keys, update
  all32big values and first100hashes; rename small IDs2000..2099 to
  `renamed:%09u` (same ID). New keys use small IDs100000..100999. Commands execute
  in that listed order, IDs ascending within each group, pipeline1. Independently
  release traversal50ms after the first mutation dispatch, regardless of replies.
  The controller uses a channel independent of Redis's event loop/serializer locks.
  Record dispatch/reply and, where instrumented, application timestamps relative
  to gate release and save completion. Mutations after release still test the
  snapshot-cut guarantee, but must not be reported as mutations while paused.
  Allow30s per command and120s for the complete trace; timeout fails assertions,
  releases any outstanding gate and retains partial-trace evidence. No gate may
  wait for the mutation worker to finish.
- RDB must contain exactly pre-cut values/keys, including deleted old keys, and no
  new keys. Post-cut live DB must contain mutations. Validate every key.
- Expiry is a separate trace, with no base-trace updates/deletes. Read Redis TIME,
  choose absolute wall-clock T=that_time+10000ms, and PEXPIREAT small IDs0..999 to T
  **before** BGSAVE. Record each PEXPIRETIME result. Take cut and hold traversal;
  require cut_wall_ms<=T-2000ms, otherwise mark harness_error and preserve attempt.
  Release traversal at T+1000ms via independent controller, never by command reply.
  Do not set TTL after cut or invent a future cut timestamp.
- Before running, freeze `expiry_oracle.json`: oracle version/hash, reference base
  revision, expiry comparison clock semantics, target IDs, expected raw record
  presence, exact absolute expiry when present, and expected post-T reload absence.
  Use an already verified classic-fork oracle for the same base semantics, or a
  separately authorized oracle-generation fixture; fork need not be a measured
  build in a revision comparison. Missing/incompatible oracle blocks this fixture, not the core
  matrix. Store absolute T and resolved expected records with each attempt. Compare
  raw RDB and post-T reload separately; never infer raw presence from reload alone.
- Separate variants asynchronously dispatch FLUSHDB SYNC or ASYNC after cut,
  instead of the base mutation trace; independently release traversal50ms after
  dispatch, never after its reply. Apply the same30s response deadline. After
  acknowledgement live DB must be empty and saved snapshot must equal pre-cut DB;
  no crash/UAF. Record lazy-free completion separately for ASYNC. Test under
  sanitizers separately. Expiry retains its independently timed T+1000ms release.
- Barriers must expose equivalent semantic cut in both POCs. If unavailable,
  explicitly report point-in-time coverage missing; random payload validation is
  not a substitute. For diskless, additionally verify post-cut command replay.

### Metadata, eviction and active defrag

- Separate standard `mixed`,12GiB,cpu6,`rw50`, first-save fixtures,R repetitions.
- LRU/LFU: `allkeys-lru`, `allkeys-lfu`, then `noeviction` independently. The setup
  supplies fixed `eviction_maxmemory_bytes` and a calibration artifact/hash. If
  absent, calibrate on this run's build: load immutable seed, wait5s, record
  used_memory, and set floor(110*used_memory/100). Freeze before timed cases.
  Later comparisons require the same byte limit for matched eviction results;
  independently recalibrated limits are different setups, not a matched comparison.
  Every fourth write per client inserts a new256B key named `insert:<client>:<seq>`;
  other operations keep normal mix. Run15s baseline then save+5s. Record counts,
  evictions/rejections and whether memory threshold was actually reached. If not,
  report eviction not exercised (not evidence of eviction correctness).
- Metadata scenarios: `get_touch`, `policy_lru`, `policy_noeviction`, `lfu_decay2`.
  Take snapshot cut, hold traversal. Dispatch asynchronously: for get_touch enqueue
  GET small IDs0..999 in ascending order on one dedicated connection; for the
  other scenarios send only CONFIG SET maxmemory-policy to the named policy or
  CONFIG SET lfu-decay-time2. Workload remains rw50.
  An independent controller releases the gate50ms after first command dispatch,
  without waiting for replies. Record commands applied before/after release and
  actual gate duration; delayed commands must not be claimed as pre-release touches.
  Command-response deadline30s; timeout is a failed assertion, not deferred success.
  Expected behavior (accept/refuse/defer) must be declared per candidate before
  run; immutable snapshot metadata must match pre-cut values from a metadata
  oracle, even if the command was accepted. Logical payload checks alone cannot
  certify metadata correctness. Never silently accept an unplanned response.
- Defrag: prepare churn by delete/reinsert every second key once before starting
  baseline. Run activedefrag yes with min/max cycles1/25, thresholds10/100 and
  ignore-bytes1MiB versus identical churn with defrag no. Report whether POC pauses
  defrag and for how long. Do not compare churned A with untouched B.
  Churn means delete all even IDs in each type in ascending order, then recreate
  them with version0 payloads in ascending order, before any warmup. Disable
  defrag during preparation; enable it only at baseline_start for the yes case.
- Runtime switching, if supported: fork→POC→fork with populated DB; attempt switch
  during active save. Use two scenarios: switch both ways while idle and validate
  a save after each; or hold active POC save at1MiB, dispatch fork-mode request
  asynchronously and independently release gate50ms after dispatch. Await response
  up to30s, validate completion and next save. Record declared refusal/defer
  behavior; no implicit timeout means success. Gate timers use a separate control
  channel that does not require Redis's event loop or serializer mutex.
- AOF rewrite, if claimed: same mixed12GiB,cpu6,rw50,R. Set appendonly yes,
  appendfsync everysec, no-appendfsync-on-rewrite no, aof-use-rdb-preamble yes,
  auto-aof-rewrite-percentage0, aof-load-truncated no. Bootstrap AOF completely
  from pristine seed before15s baseline; BGREWRITEAOF is the measured command.
  Run hammer through completion+5s, stop/drain, capture live canonical digest,
  SHUTDOWN SAVE and reload **from AOF** in an untimed process; compare full state.
  Record rewrite command latency, rewrite duration, AOF errors and fsync delays.
  Do not reuse BGSAVE duration fields for AOF; use scenario-specific rewrite fields.
  Unsupported features belong in the capability matrix, not the BGSAVE timing table.

## 8. Measurement rules

- Timestamp all events with host monotonic nanoseconds and UTC wall time. Record
  load_end, baseline_start, command_send, command_reply, snapshot_cut(if available),
  save_end, cleanup_end, failure, cancel_request, retry and probe_stop.
- Headline `save_end` is **client-observed Redis completion**, not last write,
  rename, child exit or helper join. On a dedicated connection poll INFO persistence
  every10ms (one request outstanding); timestamp full reply receipt. Completion
  requires rdb_bgsave_in_progress=0, rdb_last_bgsave_status=ok, and evidence that
  this attempt completed: either active=1 was observed after its accepted BGSAVE,
  or an attempt-specific terminal-success event was recorded by the adapter.
  Stale ok from a prior save is insufficient. A missed fast/empty save requires
  the event adapter; absent that, mark timing unavailable/harness_error, not zero.
  Adapter events must identify the attempt and must be identical semantically
  across builds. They corroborate the INFO result, not replace its timestamp.
- `save_duration_us=(save_end-command_send)/1000`; include polling/response delay.
  Record poll send/reply timestamps, prior poll and terminal event timestamp to
  expose uncertainty. On terminal err use reply receipt as `failure`; on process
  death use monitor detection timestamp and record detection method. Ordinary
  telemetry excludes duplicate INFO persistence polling already covered here.
- Separately record internal final-write, file-publication, child-exit,
  serializer-join and cleanup-end events when available; never substitute those
  for headline duration. RDB validity is established later by full validation.
- Let terminal be save_end or failure; workload_stop=terminal+5s. Windows:
  baseline=[baseline_start,command_send), save=[command_send,terminal),
  after=[terminal,workload_stop), whole=[baseline_start,workload_stop).
  `after` is always exactly5s, even if cleanup takes longer. Stop ordinary issuance
  at workload_stop; continue PING/telemetry until both cleanup and pending-request
  drain complete, capped at workload_stop+30s. Record probe_stop and timeout status.
  Extension is a separate `cleanup_observation` window=[workload_stop,probe_stop).
  Do not divide ordinary workload completions by this extension: its ops_per_sec
  is null. Record drain responses and their original send-cohort latency, along
  with extended PING latency; no duplicate additions to whole-window throughput.
  PING full-observation tails use phase `observation_whole` over
  [baseline_start,probe_stop), separately from workload `whole`. Zero-length
  observation extensions emit no quantiles/rates. Pending requests have30s socket
  timeout; uncompleted cleanup at the cap fails the required cleanup assertion.
- Full BGSAVE response latency=client send→complete reply; separately report fork
  duration and send→snapshot completion. `latest_fork_usec` is not command latency.
- Count children created by Redis during save using kernel tracing or equivalent
  event capture (not periodic ps alone). Exclude harness launch/validator children.
  If any are created, label claimed forkless mode **still forks**. Unavailable≠zero.
- Probe PING on dedicated connection every1ms, one outstanding. Record missed
  schedule slots and raw send/reply times; a stalled probe must remain in max/p99.
- Per-command raw/histogram latency at least1us resolution, up to600s overflow
  bucket; include p50/p95/p99/p99.9/max, successes/errors/timeouts/disconnects.
  Record latency from before enqueue/flush, not after pipeline transmission.
  Histogram implementation and uncertainty must be identical across POCs.
- Attribute workload requests by send phase and record end phase; include requests
  crossing startup/completion boundaries. Report whole-window tails too.
- Throughput=sum successful completions in window/window duration. Also report
  send-cohort completion rates separately; never label those as identical metrics.
- Sample cgroup memory/current/peak, anon,file,kernel,pagetables,slab,swap, memory
  events, CPUusage/throttling, IO counters, memory/CPU/IO PSI every100ms. Sample Redis
  INFO100ms; smaps_rollup1s. Record collection overhead/lost samples.
- Metric scope is per stage and window, never implicitly process lifetime.
  Headline resource fields in result.json use the primary stage's save window
  [command_send,terminal_observed), including time to failure for failed saves.
  Retain baseline, after and cleanup_observation metrics as separate window records;
  preparation/retry stages have their own records. Never add preparation counters
  to the measured save. Record every window's start_ns/end_ns and metric source.
- `peak_cgroup_bytes`, `peak_anon_bytes`, `peak_file_bytes` and
  `peak_child_private_dirty_bytes` are maxima of timestamped samples **inside that
  window**, not Linux lifetime high-water marks. Label them sampled peaks. No
  in-window samples means null with reason, not0 or the previous sample. Record
  sample count and largest sampling gap; short saves may have unavailable peaks.
  Preserve cgroup memory.peak separately as `cgroup_lifetime_peak_bytes`, with
  cgroup creation time and collection timestamp. It may include load/priming and
  MUST NOT be presented as save-window peak or subtracted to estimate added RAM.
  Do not reset lifetime counters for one candidate only.
- CPU usage/throttling, PSI total stall time, I/O totals, memory.events, copy
  count/bytes/time and reader/mutex waits are cumulative counters: report end-start
  deltas, not final absolute values. For sampled counters select the nearest
  sample at or before each boundary (maximum age200ms); record actual sample
  timestamps and label the delta boundary-approximate. Missing/stale endpoints,
  process/cgroup replacement or an untracked reset produce null plus reason.
  Never clamp a negative delta to0. Event-based adapters may provide exact
  window-attributed totals; declare method and use the same method across builds.
- Retained/tracking/encoded-buffer peak fields use sampled current-byte gauges
  within the window. Lifetime maximum-only counters are stored separately and
  cannot substitute for them. An attempt-tagged exact maximum may be reported as
  an additional `attempt_peak_bytes` metric, with its interval/method; do not mix
  it with sampled window peaks. Capture gauge samples before cleanup frees them.
- `metrics.jsonl` is mandatory: one row per stage/window/metric, containing attempt
  identity, stage_id, window, start_ns, end_ns, metric, value, unit, source, method,
  start_sample_ns, end_sample_ns, sample_count, max_sample_gap_us and missing_reason.
  Fields not applicable to that method are null. Allowed methods: sampled_max,
  sampled_counter_delta, exact_event_total, attempt_reported, lifetime_high_water.
  Headline fields must be reproducible from these records. OOM kernel evidence
  corroborates cgroup delta; unavailable deltas must not hide an observed OOM.
- Record Redis CoW estimate and sampled childPrivate_Dirty/PSS separately; do not
  call either an exact kernel CoW byte counter. Record source/availability.
  Redis CoW must be corroborated as belonging to the current attempt (including
  a terminal report); stale last-save values are null for this attempt. Its method
  is attempt_reported, not a cumulative delta or sampled memory peak.
- POC instrumentation: current/peak retained bytes, tracking bytes, encoded-buffer
  bytes, copy count/bytes/time, mutex/reader waits, cleanup/join durations, budget
  aborts. Unsupported counters=null plus reason, not0.
- Export timestamped main-thread copy, waits, cleanup and join intervals for spike
  correlation. Trace enabled identically; dropped events invalidate strong attribution.
- Performance pass: no perf sampling. Separate matched profiling pass for each
  candidate on standard mixed/write100/12GiB and22GiB large mixed pressure:49Hz
  cycles, DWARF8192, save-window filtering, main/helper/child separately. Profiling
  numbers must not replace unprofiled performance results.
- Capture kernel journal and cgroup events on failure. Exit0 alone is not success.
  If main or child is OOM-killed, preserve which process and any surviving service.

## 9. Cache, lifecycle and validation discipline

- No global cache drop between candidates. Copy immutable seed, load completely,
  wait5s with no clients, then start15s baseline. Record start anon/file cache.
  Remove previous case RDB only after validation and durable evidence backup;
  call sync between cases, not while measuring. Do not change fsync/fadvise behavior
  externally for one POC. A dedicated cold-cache experiment needs another ID.
- Every completed save: redis-check-rdb success **and** reload in separate untimed
  process, exact counts/types, full payload validation. Preserve logs and RDB SHA256.
- Validate before deleting temporary RDB. Retaining every multi-GiB RDB is optional;
  retain seed identity, manifests, digests, checksums, complete logs and metrics.
- Report attempted/validated/failed/unsupported/missing denominators. Show each run
  and median/min/max across repetitions; never average away OOMs or timeouts.
- Do not average percentiles across runs. Show individual quantiles and either a
  merged histogram quantile or explicitly labeled median-of-run-quantiles.

## 10. Machine-readable output contract

Canonical format: UTF-8 JSON/JSONL, schema ID `redis-persistence-bench/2.0`.
Derived CSV files are mandatory for spreadsheet comparison. Units are encoded in
field names; JSON numbers only, no `"4 GiB"`,NaN/Infinity or thousands separators.
Use null for unavailable, accompanied by `missing_metrics` reasons. Zero means measured0.

Directory layout:

```text
<run_id>/
  manifest.json
  case-catalog.json
  cases.jsonl
  operations.csv
  cases.csv
  capability-matrix.json
  checksums.sha256
  <case_id>/<build_id>/r<repetition>/a<attempt>/
    result.json
    events.jsonl
    latency-histograms.json
    probes.jsonl
    telemetry.jsonl
    metrics.jsonl
    validation.json
    effective-config.json
    server.log
    kernel.log
    harness.log
```

Stable `case_id` excludes run name/build/repetition:
`<tier>.<dataset>.<workload>.<memory_profile>.<cpu_profile>.<lifecycle>.<scenario>`.
Example:`standard.mixed.rw50.tight.cpu1.first.bgsave`.
Transition scenario IDs additionally include variant and serializer position.
Primary join key:`schema_version,run_id,case_id,build_id,repetition,attempt`.
Historical comparison joins by case definition SHA256, never just display name.

### Required manifest fields

`schema_version, run_id, benchmark_name, created_utc, spec_sha256, case_definition_sha256,
generator_sha256, validator_sha256, telemetry_sha256, build{}, aws{}, os{},
storage{}, cpu{}, memory_profiles{}, datasets[], workloads[], planned_cases[],
termination_deadline_utc, deviations[]`.
Also require `build_id`, `revision`, `setup_sha256`, `selected_batches`, `R`,
`seed_producer_build_id` when seeds are generated, `replica_build_id` when applicable,
`case_catalog_sha256`, and `results_s3_uri`. Record eviction calibration artifact,
bytes and producer build hash when eviction fixtures are selected.

The build includes base revision/source/binary hashes and exact mode/config;
each dataset includes counts, sizes, generator ID, seed hash and logical digest.
Complete these fields before running; unresolved AMI/image/hash placeholders block
launch. Never put AWS credentials, private keys or signed URLs in outputs.

### Required result.json fields

```json
{
  "schema_version": "redis-persistence-bench/2.0",
  "run_id": "assigned-before-launch",
  "benchmark_name": "Live Dict — allocator update",
  "revision": "resolved-commit-plus-dirty-patch-hash",
  "case_id": "standard.mixed.rw50.tight.cpu1.first.bgsave",
  "workload": "rw50",
  "cpu_profile": "cpu1",
  "memory_profile": "tight",
  "build_id": "immutable-build-sha256",
  "repetition": 1,
  "attempt": 1,
  "execution_status": "not_run",
  "assertion_status": "not_checked",
  "upload_status": "pending",
  "expected_outcome": "snapshot_complete_and_valid",
  "observed_outcome": null,
  "failure_reason": null,
  "started_utc": null,
  "finished_utc": null,
  "db_keys": 9720410,
  "loaded_used_memory_bytes": null,
  "memory_limit_bytes": 5368709120,
  "bgsave_response_us": null,
  "fork_duration_us": null,
  "save_duration_us": null,
  "time_to_failure_us": null,
  "cleanup_duration_us": null,
  "cancel_response_us": null,
  "cancel_complete_us": null,
  "redis_children_created": null,
  "peak_cgroup_bytes": null,
  "cgroup_lifetime_peak_bytes": null,
  "peak_anon_bytes": null,
  "peak_file_bytes": null,
  "redis_cow_bytes": null,
  "peak_child_private_dirty_bytes": null,
  "snapshot_retained_peak_bytes": null,
  "tracking_peak_bytes": null,
  "encoded_buffer_peak_bytes": null,
  "copy_bytes": null,
  "copy_us": null,
  "reader_wait_us": null,
  "oom_kills": null,
  "cpu_throttled_us": null,
  "memory_psi_full_us": null,
  "io_psi_full_us": null,
  "rdb_check_ok": null,
  "full_payload_validation_ok": null,
  "point_in_time_validation_ok": null,
  "previous_rdb_preserved": null,
  "stages": [],
  "missing_metrics": {},
  "artifact_sha256": {},
  "deviations": []
}
```

Status fields are independent:
- `execution_status`: not_run, running, completed, unsupported, not_applicable,
  not_run_deadline, harness_error. Completed means the attempt ended and evidence
  was collected, not that Redis successfully saved.
- `assertion_status`: not_checked, passed, failed. Expected cancellation/EIO/ENOSPC
  can pass only if all specified preservation/availability/retry assertions pass.
  A normal performance OOM fails assertions; never label it a validated snapshot.
- `upload_status`: pending, verified, upload_failed. Upload failure must not
  overwrite the Redis outcome or assertion result.
- `observed_outcome`: snapshot_complete, cancelled, injected_eio, enospc,
  budget_exhausted, oom, load_oom, timeout, replication_converged, rewrite_complete,
  command_refused, other_error; null until observed. Record detailed subevents too.
`save_duration_us` MUST be null unless a snapshot completed; validation failure
is separately visible. Retry save metrics use a separate `retry` stage record,
not the failed/cancelled attempt's save duration. A fixture passes only when its
expected outcome and all required assertions agree with evidence.
Example above is a schema template, not a measurement. Add cancellation/replication
fields without dropping common fields; new required semantics need schema version bump.

### Stage records (required for every attempt)

`result.json.stages` is an ordered array, mirrored as `stages.jsonl` and
`stages.csv`. Every stage uses the following fixed scalar fields; absent/not
applicable metrics are null, not omitted. The attempt identity prefixes each
JSONL/CSV row. Stage IDs: `primary`, `cancel`, `retry_1`, `retry_2`, etc.; sequence
increases monotonically. Initial lifecycle priming/fault preparation uses
`prepare_1` etc. and is excluded from headline performance totals.

```json
{
  "stage_id": "retry_1",
  "sequence": 2,
  "kind": "bgsave",
  "command": "BGSAVE",
  "command_send_ns": null,
  "command_reply_ns": null,
  "terminal_observed_ns": null,
  "cleanup_end_ns": null,
  "expected_outcome": "snapshot_complete_and_valid",
  "observed_outcome": null,
  "execution_status": "not_run",
  "assertion_status": "not_checked",
  "command_response_us": null,
  "save_duration_us": null,
  "rewrite_duration_us": null,
  "replication_duration_us": null,
  "time_to_failure_us": null,
  "cleanup_duration_us": null,
  "rdb_check_ok": null,
  "full_payload_validation_ok": null,
  "previous_rdb_preserved": null,
  "failure_reason": null
}
```

`kind` enum: bgsave, cancellation, replication, aof_rewrite, configuration.
`stages.csv` field order is identity prefix (schema_version,run_id,case_id,
build_id,repetition,attempt) followed by the fields in the example order.
Attempt-level timing fields mirror `primary` only; retry success never overwrites
primary cancellation/failure. Overall assertion requires every mandatory stage
to pass. Replication duration starts at connection initiation, ends at observed
online/matching-offset condition; rewrite duration ends at first INFO reply with
aof_rewrite_in_progress=0 and aof_last_bgrewrite_status=ok, using the same active/
attempt-event corroboration rule as BGSAVE. Cancellation completion uses observed
attempt-specific cleanup-end, not cancellation acknowledgement.

`operations.csv` exact columns:

```csv
schema_version,run_id,case_id,build_id,repetition,attempt,stage_id,workload,cpu_profile,memory_profile,phase,accounting_basis,response_class,operation,command,transition_step_role,value_bytes,window_us,sent,completed,errors,timeouts,disconnects,misses,ops_per_sec,p50_us,p95_us,p99_us,p999_us,max_us,cross_phase_requests
```

Phases:`baseline,save,after,whole,cleanup_observation,observation_whole,cancel,retry`;
`stage_id` disambiguates repeated
save/retry windows. Core operation labels remain small_set,big_set,hset,small_get,
big_get,hget,ping. Fixture labels are append,setrange,incr,hincrby,hdel,del,rename,
rpush,pexpireat,pexpiretime,hpexpire,flushdb_sync,flushdb_async,config_set,bgsave,
bgrewriteaof,shutdown,transition_set,transition_hset. A new label requires updating
the shared registry in the manifest before execution; arbitrary per-run labels
are prohibited. `command` is the uppercase Redis command name (e.g. SET for both
small_set and transition_set); subcommand/options are in the frozen command trace.
`transition_step_role` is forward/reset for transitions, otherwise null.
`value_bytes` means submitted value bytes on mutations or actual returned value
bytes on reads; HSET sums field values, not field names; APPEND/SETRANGE use patch
bytes, not final object size (record that in transitions). Integer/control replies
and commands without a byte payload use null. Group differing byte lengths into
separate rows, never mix them into an average-size label.
Ordinary HSET=128B, HGET=32B. Do not write a
zero-traffic percentile row as0; use empty CSV fields (JSONnull). Count missing GETs.
For each operation/window emit the following `accounting_basis`/`response_class` rows:
- `completion_window`: count success/error responses by reply timestamp in the
  half-open window[start,end); ops_per_sec=successful replies/window seconds.
  response_class=`all`; latency quantile columns and `sent` are null. This is
  headline throughput, except cleanup_observation/observation_whole rates are null.
- `send_cohort`: select requests sent in[start,end), follow until response or
  declared timeout even after the window ends; sent/completed/errors/timeouts and
  latency quantiles describe that cohort. ops_per_sec is always null. Emit one
  response_class=`all` row with cohort counts and null quantiles; additionally emit
  `success` and `error` rows with their own counts and separate latency histograms.
  Success means a non-error protocol reply; refusals/MISCONF/OOM error replies are
  error responses even when expected by the fixture. A failed fixture assertion
  does not change a protocol reply's class. Count nil reads as successes plus misses.
  Success/error subset rows have `sent=null`; completed means successful replies,
  errors means error replies, and only the matching count is nonzero. Timeout and
  disconnect counts appear only on the all row; neither enters response quantiles.
  Resolve each unanswered request once as timeout or disconnect, never both.
  No responses in a class means null quantiles, not0; include timed-out request
  counts and their censoring deadlines beside latency results.
  Headline operation percentiles use **success only**; error latency is a separate
  labeled series. Never merge success/error histograms. Include crossing requests
  once in their send cohort. Subset rows are breakdowns, not additional traffic.
`cross_phase_requests` counts requests whose send and reply phases differ within
the row's selection. Whole-window rows are aggregations, not additional traffic.
`cases.csv` flattens scalar result fields in the order listed above; nested fields
remain in JSON. `cases.jsonl` contains one complete result per attempt.

`events.jsonl`: `monotonic_ns,event,pid,tid,duration_us,bytes,details`.
Every event also includes `stage_id`; frozen traces and per-command transition
records carry it so cross-stage activity cannot be counted as the initial save.
`probes.jsonl`: `scheduled_ns,send_ns,reply_ns,send_phase,reply_phase,status`.
`telemetry.jsonl`: monotonic timestamp plus unit-suffixed metrics and source IDs.
Histogram grouping MUST exactly match the corresponding send_cohort CSV row:
schema_version,run_id,case_id,build_id,repetition,attempt,stage_id,
workload,cpu_profile,memory_profile,phase,accounting_basis,response_class,
operation,command,transition_step_role,value_bytes.
accounting_basis is send_cohort; response_class is success or error. Preserve
null grouping values (e.g. transition_step_role), never replace them with0/empty
strings internally. Distinct payload sizes and forward/reset steps MUST NOT merge.
Each histogram also records window_start_ns,window_end_ns,window_us, bucket
boundaries/counts, response count, sum_us and exact observedmax. Derive p50/p95/
p99/p99.9 from these same buckets using nearest-rank ceil(p*count), reporting the
selected bucket's upper bound; max_us uses exact observedmax. Empty histograms
have null percentiles/max. Freeze bucket boundaries in the shared manifest;
overflow quantiles are null with an overflow flag, never reported as finite600s.
No histogram is emitted for completion_window or response_class=all rows.
Validate one-to-one correspondence of histogram keys to success/error CSV rows,
response count to completed/errors respectively, and all reported quantiles.
Timestamps must allow offline phase-boundary verification.
Arena fixtures additionally emit `transitions.jsonl` with run/build/case/repetition/
attempt, target key, command, version, serializer_position, old/new type, old/new
encoding, old/new arena, old/new payload_bytes, send_ns, reply_ns, status and
snapshot_validation_ok. Missing arena instrumentation is null with reason.
Include migration counts and bytes per direction in result.json. Keep injected
gate-delay measurements separate from ungated operation histograms.

## 11. Reporting and acceptance

### Durable S3 results — mandatory

- Freeze an approved **persistent results bucket** and region in the manifest
  before launch; use `s3://<approved-results-bucket>/redis-persistence-bench/
  <schema-version>/<run_id>/`. Do not invent a shared bucket or reuse an
  old deleted temporary bucket. Bucket creation/access and retention need approval.
- Private bucket, block public access, encryption enabled, least-privilege instance
  role restricted to the run prefix. No public objects/ACLs, credentials, SSH keys
  or signed URLs in artifacts. Record encryption and retention policy.
- Preserve the local output tree under that prefix, including expanded case catalog,
  source/build/config manifests, JSON/CSV, histograms, traces, logs, validation,
  failed attempts and final HTML/Jira summaries. Raw large RDBs remain optional
  after full validation; store their SHA256 and validation evidence regardless.
- After each attempt, outside timed measurement, upload its immutable artifacts
  and checksums before starting the next case. Use unique repetition/attempt paths;
  never overwrite a previous named run with a new run or a failed attempt with a retry. Upload partial
  failure logs if the attempt times out or Redis crashes.
- Verify remote checksums against SHA256 manifest using a full-object SHA256
  checksum or download-and-hash. ETag alone is not SHA256 and multipart ETags are
  not ordinary content MD5. Record verification evidence and object version IDs
  where versioning is enabled. Publish final run completion marker only after all
  selected attempts and reports are verified remotely.
- Results must survive EC2 termination. On upload failure retain local artifacts,
  set `upload_status=upload_failed`, retry within approved deadline and report risk; never
  silently extend rental. Do not claim durable completion without verified backup.
- Delete temporary transport buckets only after verified promotion into the
  persistent results prefix. **Do not delete the persistent result bucket/prefix
  during instance cleanup.** Retention/deletion is a separate explicit decision.
- Reports link to stable S3 object URIs/console locations, not expiring signed URLs.
  A result URI is not public access; authorized readers use AWS authentication.

- Produce JSON/CSV first, HTML/Jira summaries derived from them. No manually copied
  numbers. Include matrix coverage, validation status, exact setup and deviations.
- Each report stands alone: SET/GET throughput pre-save→save, own-pre-save loss%,
  p99/p99.9/max, full command latency, save duration and resource/correctness results.
  Show benchmark_name, run_id and revision prominently. Do not require or invent
  a winner, reference report, or baseline-relative percentage at execution time.
- Secondary: memory headroom/CoW/retained/tracking, OOM, cancellation, retry,
  replication convergence and profile evidence. A faster failed save is not a win.
- Use capability evidence to separate unsupported functionality from performance.
- No universal winner from one dataset; no causal claim from different-instance
  single observations. Historical September22 data is context: profiler enabled,
  shorter tail and different explicit defaults prevent strict v2.0 pairing.
- Required preflight: machine manifest locked, binary/config hashes checked, seed
  full validation passed, clocks/probes/child tracing verified, tiny success/OOM/
  cancel fixtures checked, deadline/backups verified, no unresolved placeholders.
- Required completion: every planned row has execution/assertion/upload statuses,
  every completed snapshot validated,
  all failures retained, backups hash-verified, only exact authorized resources
  cleaned, cleanup evidence saved. Do not print/preserve secrets in report artifacts.

## 12. Current harness implementation gap

### Comparing existing reports (separate from execution)

Input: two or more completed run IDs, or benchmark names/revisions that resolve
unambiguously to those IDs. The caller selects the comparison reference report
only here. Examples: "Fork — September run" vs "Live Dict — allocator update",
or "Live Dict — previous revision" vs "Live Dict — improved revision". A comparison
never automatically launches a new benchmark or changes source reports.

1. Verify each report's schema and checksum manifest. Resolve name collisions
   explicitly; never silently select the newest run sharing a name.
2. Match case_id plus case-definition hash, generator/validator versions, actual
   datasets/seed identity, workload, CPU/RAM limits, machine/storage, common Redis
   settings, instrumentation, replica/oracle dependencies and repetition coverage.
3. Build/revision and declared implementation-specific settings are the intended
   differences, not automatic mismatches. Emit a field-level setup diff; a full
   setup hash difference alone does not determine compatibility. Common config
   differences must not be hidden as implementation details.
4. Label each match `matched_settings`, `context_only` or `incompatible`. Separate
   hosts/times always carry observational uncertainty even with matched settings;
   only recorded interleaved scheduling supports a paired-run claim. Show unmatched
   cases and differing repetition counts; do not invent missing results.
5. Derive throughput/latency/duration/memory deltas from the stored JSON/CSV,
   preserving failures and OOMs. Reference-relative percent change is null when
   reference value is0/unavailable or the comparison is semantically invalid.
6. Write a NEW comparison_id directory with comparison.json/CSV/HTML and exact
   source run IDs/hashes, names, revisions and compatibility notes. Save it to
   the persistent results bucket under `comparisons/<comparison_id>/`; keep all
   original named reports immutable.

The run report answers "what happened for this setup?". The comparison report
answers "what changed between these named runs?". They are separate artifacts.

This is required implementation work, not a reason to stop at documentation.
Execution sequence: receive benchmark name/setup and catalog batch; freeze manifests
and persistent S3 destination; implement missing shared harness features; pass
preflight/smoke; execute selected cases; upload/verify results; generate run report;
clean temporary resources while retaining persistent results. Report blockers
explicitly rather than silently dropping cases. Starting AWS still requires the
specific machine-duration authorization and access described above.

Existing `bench.c`, `run-three-mode.sh`, `run-buffered.sh` and validators are useful
starting points, **not conformant v2.0 runners yet**. Specifically implement the
full lifecycle/repetition matrix, exact config export,5s tail, unprofiled primary
pass, completion-window accounting, common deterministic fault gates, canonical
schemas and continuous child events before calling a run v2.0-compliant.
The prior report's source archives remain immutable historical evidence.
This document does not authorize launching AWS resources or extending rental time.
