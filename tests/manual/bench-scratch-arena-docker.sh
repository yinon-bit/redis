#!/usr/bin/env bash
#
# Compare scratch-arena on vs off inside Linux Docker (jemalloc), with the
# server CPU-pinned via --cpuset-cpus. Mirrors the harness described in
# SCRATCH_ARENA_MEMORY_OPTIMIZATION.md.
#
# Usage (from repo root):
#   ./tests/manual/bench-scratch-arena-docker.sh
#   CPUS=4 REPEATS=5 ./tests/manual/bench-scratch-arena-docker.sh
#   # Saturate an 8-core pin so the main thread is still CPU-starved:
#   CPUS=8 PRESSURE=1 REPEATS=5 WORKLOADS="baseline pipeline" ./tests/manual/bench-scratch-arena-docker.sh
#
# Why PRESSURE helps on 8 cores:
#   With io-threads=1, command + I/O run on ONE Redis main thread. Giving the
#   container 8 CPUs often leaves headroom, so allocator isolation barely shows.
#   PRESSURE=1 starts busy-loops on the SAME cpuset, stealing cycles/cache so
#   the main thread is contested — closer to the "4-core pressure" regime where
#   the Enterprise handoff saw gains.
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CPUS="${CPUS:-4}"
REPEATS="${REPEATS:-5}"
REQUESTS="${REQUESTS:-1000000}"
CLIENTS="${CLIENTS:-50}"
PRESSURE="${PRESSURE:-0}"          # 1 = run CPU hogs on the same cpuset
STRESS_WORKERS="${STRESS_WORKERS:-}" # default: CPUS (fully contend the pin)
WORKLOADS="${WORKLOADS:-baseline pipeline bitop sort sinter zunion georadius hrandfield srandmember}"
FORCE_REBUILD="${FORCE_REBUILD:-0}"
NET_NAME="scratch-arena-bench-net"
IMG="ubuntu:22.04"
BIN_VOL="scratch-arena-bench-bin"
BUILD_CTR="scratch-arena-build"
SERVER_CTR="scratch-arena-server"
CLIENT_CTR="scratch-arena-client"
STRESS_CTR="scratch-arena-stress"
RESULT_DIR="${RESULT_DIR:-$ROOT/tests/tmp/scratch-arena-bench}"
PORT=6379

if [[ -z "$STRESS_WORKERS" ]]; then
  STRESS_WORKERS="$CPUS"
fi

mkdir -p "$RESULT_DIR"
SUMMARY="$RESULT_DIR/summary.tsv"
RAW="$RESULT_DIR/raw.log"
: >"$RAW"
printf 'cpus\tworkload\tscratch\trepeat\trps\tpressure\n' >"$SUMMARY"

need_docker() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker daemon not available" >&2
    exit 1
  fi
}

cleanup() {
  docker rm -f "$SERVER_CTR" "$CLIENT_CTR" "$BUILD_CTR" "$STRESS_CTR" >/dev/null 2>&1 || true
  docker network rm "$NET_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

start_pressure() {
  docker rm -f "$STRESS_CTR" >/dev/null 2>&1 || true
  [[ "$PRESSURE" == "1" ]] || return 0
  echo "==> Starting $STRESS_WORKERS CPU stress workers on cpuset 0-$((CPUS-1))"
  # Busy-loop workers share the server's cpuset so Redis's main thread contends
  # for CPU time and last-level cache even when CPUS=8.
  docker run -d --name "$STRESS_CTR" \
    --cpuset-cpus "0-$((CPUS-1))" \
    "$IMG" \
    bash -lc '
      n='"$STRESS_WORKERS"'
      for i in $(seq 1 "$n"); do
        (while true; do :; done) &
      done
      wait
    ' >/dev/null
}

stop_pressure() {
  docker rm -f "$STRESS_CTR" >/dev/null 2>&1 || true
}

ensure_binaries() {
  if [[ "$FORCE_REBUILD" == "1" ]]; then
    echo "==> FORCE_REBUILD=1: rebuilding Linux jemalloc binaries"
    docker volume rm "$BIN_VOL" >/dev/null 2>&1 || true
  fi
  if docker run --rm -v "$BIN_VOL":/out "$IMG" \
      test -x /out/redis-server -a -x /out/redis-benchmark 2>/dev/null; then
    echo "==> Reusing Linux jemalloc binaries in volume $BIN_VOL"
    return
  fi

  echo "==> Building Linux/jemalloc redis-server + redis-benchmark in Docker"
  docker pull "$IMG" >/dev/null
  docker volume create "$BIN_VOL" >/dev/null
  docker rm -f "$BUILD_CTR" >/dev/null 2>&1 || true
  docker run -d --name "$BUILD_CTR" \
    -v "$ROOT:/workspace:ro" \
    -v "$BIN_VOL":/out \
    "$IMG" sleep infinity >/dev/null

  docker exec "$BUILD_CTR" bash -lc '
    set -euo pipefail
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq build-essential ca-certificates wget python3 rsync \
      pkg-config automake autoconf libtool > /tmp/apt.log
    rm -rf /build
    mkdir -p /build
    rsync -a --delete \
      --exclude .git --exclude tests/tmp --exclude '"'"'*.o'"'"' --exclude '"'"'*.d'"'"' \
      --exclude src/redis-server --exclude src/redis-cli --exclude src/redis-benchmark \
      --exclude src/redis-check-aof --exclude src/redis-check-rdb --exclude src/redis-sentinel \
      /workspace/ /build/
    cd /build
    make distclean >/tmp/distclean.log 2>&1 || true
    make -C deps jemalloc -j"$(nproc)" >/tmp/jemalloc.log 2>&1
    make -C src -j"$(nproc)" MALLOC=jemalloc all 2>&1 | tee /tmp/build.log | tail -40
    test -x src/redis-server
    test -x src/redis-benchmark
    ./src/redis-server --version
    cp -f src/redis-server src/redis-benchmark src/redis-cli /out/
    chmod +x /out/redis-server /out/redis-benchmark /out/redis-cli
  '
  docker rm -f "$BUILD_CTR" >/dev/null
  echo "==> Build complete"
}

parse_rps() {
  # Extract the number immediately before "requests per second".
  # Avoid awk's `$i+0==$i` trap (e.g. "ZUNION:" coerces to 0).
  local out="$1"
  printf '%s\n' "$out" | awk '
    /requests per second/ {
      for (i = 1; i <= NF; i++) {
        if ($(i+1) == "requests" && $(i+2) == "per") {
          print $i
          exit
        }
      }
    }
  '
}

cli() {
  docker run --rm --network "$NET_NAME" -v "$BIN_VOL":/out:ro "$IMG" \
    /out/redis-cli -h redis "$@"
}

# Populate keys once per server start so command-scratch workloads hit real temps.
seed_workload() {
  local workload="$1"
  case "$workload" in
    baseline|pipeline|large_value|hot_key) return 0 ;;
    bitop)
      cli EVAL "for i=1,5 do redis.call('SET','bitop:k'..i,string.rep('A',8192)) end return 1" 0 >/dev/null
      ;;
    sort)
      cli DEL sort:list >/dev/null
      cli EVAL "for i=1,3000 do redis.call('LPUSH',KEYS[1],'v'..i) end return 1" 1 sort:list >/dev/null
      ;;
    sinter)
      cli EVAL "for i=1,2000 do redis.call('SADD',KEYS[1],'m'..i) end; for i=500,2500 do redis.call('SADD',KEYS[2],'m'..i) end; for i=1000,3000 do redis.call('SADD',KEYS[3],'m'..i) end return 1" 3 set:a set:b set:c >/dev/null
      ;;
    zunion)
      cli EVAL "for i=1,2000 do redis.call('ZADD',KEYS[1],i,'m'..i) end; for i=500,2500 do redis.call('ZADD',KEYS[2],i,'m'..i) end return 1" 2 zset:a zset:b >/dev/null
      ;;
    georadius)
      cli EVAL "for i=1,2000 do local lon=-180+((i*37)%3600)/10; local lat=-80+((i*19)%1600)/10; redis.call('GEOADD',KEYS[1],lon,lat,'p'..i) end return 1" 1 geo:cities >/dev/null
      ;;
    hrandfield)
      cli EVAL "for i=1,2000 do redis.call('HSET',KEYS[1],'f'..i,'v'..i) end return 1" 1 hash:h >/dev/null
      ;;
    srandmember)
      cli EVAL "for i=1,2000 do redis.call('SADD',KEYS[1],'m'..i) end return 1" 1 set:rand >/dev/null
      ;;
    *) echo "unknown workload for seed: $workload" >&2; exit 1 ;;
  esac
}

workload_bench_args() {
  local workload="$1"
  local n_cmd=$(( REQUESTS < 200000 ? REQUESTS : 200000 ))
  case "$workload" in
    baseline)    printf '%s ' -t set,get -n "$REQUESTS" -c "$CLIENTS" -q ;;
    pipeline)    printf '%s ' -t set,get -n "$REQUESTS" -c "$CLIENTS" -P 32 -q ;;
    large_value) printf '%s ' -t set,get -n 200000 -c "$CLIENTS" -d 100000 -q ;;
    hot_key)     printf '%s ' -t set,get -n "$REQUESTS" -c "$CLIENTS" -r 10 -q ;;
    bitop)       printf '%s ' -n "$n_cmd" -c "$CLIENTS" -q BITOP AND bitop:dst bitop:k1 bitop:k2 bitop:k3 bitop:k4 bitop:k5 ;;
    sort)        printf '%s ' -n "$n_cmd" -c "$CLIENTS" -q SORT sort:list ALPHA LIMIT 0 50 ;;
    sinter)      printf '%s ' -n "$n_cmd" -c "$CLIENTS" -q SINTER set:a set:b set:c ;;
    zunion)      printf '%s ' -n "$(( n_cmd < 20000 ? n_cmd : 20000 ))" -c "$CLIENTS" -q ZUNION 2 zset:a zset:b ;;
    georadius)   printf '%s ' -n "$(( n_cmd < 50000 ? n_cmd : 50000 ))" -c "$CLIENTS" -q GEORADIUS geo:cities 0 0 200 km COUNT 100 ;;
    hrandfield)  printf '%s ' -n "$n_cmd" -c "$CLIENTS" -q HRANDFIELD hash:h 50 ;;
    srandmember) printf '%s ' -n "$n_cmd" -c "$CLIENTS" -q SRANDMEMBER set:rand 50 ;;
    *) echo "unknown workload: $workload" >&2; exit 1 ;;
  esac
}

run_one() {
  local scratch="$1" workload="$2" repeat="$3"
  # shellcheck disable=SC2207
  local bench_args=( $(workload_bench_args "$workload") )

  docker rm -f "$SERVER_CTR" >/dev/null 2>&1 || true
  start_pressure

  docker run -d --name "$SERVER_CTR" \
    --network "$NET_NAME" \
    --cpuset-cpus "0-$((CPUS-1))" \
    --network-alias redis \
    -v "$BIN_VOL":/out:ro \
    "$IMG" \
    /out/redis-server \
      --bind 0.0.0.0 --port "$PORT" \
      --protected-mode no \
      --save "" --appendonly no \
      --io-threads 1 \
      --scratch-arena "$scratch" \
      --daemonize no \
    >/dev/null

  for _ in $(seq 1 50); do
    if cli PING 2>/dev/null | grep -q PONG; then
      break
    fi
    sleep 0.1
  done

  seed_workload "$workload"

  local out rps
  out="$(docker run --rm --name "$CLIENT_CTR" --network "$NET_NAME" \
    -v "$BIN_VOL":/out:ro "$IMG" \
    /out/redis-benchmark -h redis -p "$PORT" "${bench_args[@]}" 2>&1)" || true
  printf '%s\n' "$out" >>"$RAW"
  rps="$(parse_rps "$out")"
  if [[ -z "$rps" ]]; then
    echo "WARN: failed to parse RPS for scratch=$scratch workload=$workload repeat=$repeat" >&2
    printf '%s\n' "$out" | tail -20 >&2
    rps="nan"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$CPUS" "$workload" "$scratch" "$repeat" "$rps" "$PRESSURE" | tee -a "$SUMMARY"
  docker rm -f "$SERVER_CTR" >/dev/null 2>&1 || true
  stop_pressure
}

print_report() {
  echo
  echo "============================================================"
  echo " Scratch-arena Docker benchmark (CPUS=$CPUS, REPEATS=$REPEATS, PRESSURE=$PRESSURE)"
  echo " Results: $SUMMARY"
  echo "============================================================"
  printf '%-12s %-8s %14s %8s %10s\n' workload scratch median_rps 'cv%' delta
  printf '%-12s %-8s %14s %8s %10s\n' -------- ------- ---------- ---- -----

  python3 - "$SUMMARY" <<'PY'
import sys
from collections import defaultdict
from statistics import median, pstdev

path = sys.argv[1]
rows = defaultdict(list)
order = []
seen = set()
with open(path) as f:
    next(f)
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 5:
            continue
        cpus, wl, scratch, rep, rps = parts[:5]
        if rps == "nan":
            continue
        rows[(wl, scratch)].append(float(rps))
        if wl not in seen:
            seen.add(wl)
            order.append(wl)

for wl in order:
    meds = {}
    for scratch in ("no", "yes"):
        vals = rows.get((wl, scratch), [])
        if not vals:
            print(f"{wl:<12} {scratch:<8} {'n/a':>14}")
            continue
        med = median(vals)
        cv = (100.0 * pstdev(vals) / med) if len(vals) > 1 and med else 0.0
        meds[scratch] = med
        print(f"{wl:<12} {scratch:<8} {med:14.0f} {cv:7.1f}%")
    if "no" in meds and "yes" in meds and meds["no"]:
        d = (meds["yes"] - meds["no"]) / meds["no"] * 100.0
        print(f"{'':<12} {'delta':<8} {'':>14} {'':>8} {d:+9.1f}%")
PY
  echo
  echo "Note: Docker Desktop on macOS is a noisy harness (see handoff doc)."
  echo "Treat deltas as directional; prefer bare-metal Linux for merge confidence."
}

main() {
  need_docker
  cleanup
  ensure_binaries
  docker network create "$NET_NAME" >/dev/null

  echo "==> Running benchmarks (cpus=$CPUS repeats=$REPEATS requests=$REQUESTS pressure=$PRESSURE workloads=$WORKLOADS)"
  local wl scratch i
  # On/off are interleaved within each repeat rather than run as two separate
  # blocks. Docker Desktop drifts over minutes (thermal, host load), and a
  # blocked layout lets that drift show up as a fake on-vs-off delta.
  # shellcheck disable=SC2086
  for wl in $WORKLOADS; do
    for i in $(seq 1 "$REPEATS"); do
      for scratch in no yes; do
        echo "-- cpus=$CPUS workload=$wl scratch=$scratch pressure=$PRESSURE repeat=$i/$REPEATS"
        run_one "$scratch" "$wl" "$i"
      done
    done
  done

  print_report
}

main "$@"
