#!/usr/bin/env python3
"""Dedicated, isolated Redis string benchmark. Python 3 standard library only."""
import asyncio
import collections
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import sys
import time


class Connection:
    async def open(self, port):
        self.reader, self.writer = await asyncio.open_connection('127.0.0.1', port)
        return self

    async def command(self, *parts):
        self.send(*parts)
        await self.writer.drain()
        return await self.reply()

    def send(self, *parts):
        parts = [p if isinstance(p, bytes) else str(p).encode() for p in parts]
        self.writer.write(b'*%d\r\n' % len(parts) + b''.join(b'$%d\r\n' % len(p)+p+b'\r\n' for p in parts))

    async def reply(self):
        line = await self.reader.readline()
        if not line: raise ConnectionError('Redis closed connection')
        if line[:1] == b'-': raise RuntimeError(line.decode().strip())
        if line[:1] == b'$':
            size = int(line[1:])
            return None if size < 0 else (await self.reader.readexactly(size+2))[:-2]
        if line[:1] == b':': return int(line[1:])
        if line[:1] == b'*':
            return [await self.reply() for _ in range(int(line[1:]))]
        return line[1:-2]

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()


def parse_info(raw):
    return dict(line.split(':', 1) for line in raw.decode().splitlines() if ':' in line and not line.startswith('#'))


class Metric:
    def __init__(self):
        self.n = self.errors = self.skipped = self.completed = 0
        self.maximum = 0.0
        self.hist = collections.Counter()

    def add(self, ms):
        self.n += 1
        self.maximum = max(self.maximum, ms)
        # 1% logarithmic buckets; bounded memory, upper-bound quantiles.
        self.hist[math.ceil(math.log(max(ms, .001), 1.01))] += 1

    def quantile(self, p):
        total = 0
        for bucket, count in sorted(self.hist.items()):
            total += count
            if total >= math.ceil(self.n*p): return 1.01**bucket
        return None


async def run(c, out, result):
    control = await Connection().open(c['port'])
    if c['mode'] == 'fork':
        try: await control.command('CONFIG', 'SET', 'bgsave-mode', 'fork')
        except RuntimeError as exc:
            if 'Unknown option' not in str(exc): raise
    rng = random.Random(c['seed'])
    payload = {size: [rng.randbytes(c[size+'_bytes']) for _ in range(2)] for size in ('small','big')}
    for size in ('small', 'big'):
        for i in range(c[size+'_keys']):
            await control.command('SET', f'{size}:{i}', payload[size][0])
    result['loaded_keys'] = await control.command('DBSIZE')
    result['info_before'] = parse_info(await control.command('INFO', 'all'))
    metrics = collections.defaultdict(Metric)
    stop = asyncio.Event()
    phase = ['before']
    start = time.monotonic()
    boundaries = {'before': start}

    async def client(op, rate, ident):
        conn = await Connection().open(c['port'])
        rand = random.Random(c['seed']+ident+(100000 if op == 'GET' else 0))
        interval = c['clients']/rate
        due = start + ident/rate
        try:
            while not stop.is_set():
                delay = max(0, due-time.monotonic())
                if delay:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                        break
                    except asyncio.TimeoutError: pass
                if stop.is_set(): break
                now = time.monotonic()
                missed = max(0, int((now-due)/interval))
                metrics[(phase[0], op)].skipped += missed
                due += (missed+1)*interval
                size = 'big' if rand.random()*100 < c['big_request_percent'] else 'small'
                key = f'{size}:{rand.randrange(c[size+"_keys"])}'
                sent = time.monotonic()
                send_phase = phase[0]
                try:
                    args = (op, key, payload[size][1]) if op == 'SET' else (op, key)
                    await asyncio.wait_for(conn.command(*args), timeout=10)
                    metrics[(send_phase, op)].add((time.monotonic()-sent)*1000)
                    metrics[(phase[0], op)].completed += 1
                except Exception:
                    metrics[(send_phase, op)].errors += 1
                    break
        finally:
            await conn.close()

    # Completion-window counts are separate from send-cohort latency counts.
    async def monitor():
        conn = await Connection().open(c['port'])
        try:
            with (out/'telemetry.jsonl').open('w') as log:
                while not stop.is_set():
                    sample = {'elapsed_s':time.monotonic()-start, 'phase':phase[0],
                              'info':parse_info(await conn.command('INFO', 'all'))}
                    log.write(json.dumps(sample)+'\n')
                    log.flush()
                    await asyncio.sleep(.1)
        finally: await conn.close()

    tasks = [asyncio.create_task(monitor())]
    for op, rate in [('SET',c['writes_per_sec']), ('GET',c['reads_per_sec'])]:
        if rate: tasks.extend(asyncio.create_task(client(op,rate,i)) for i in range(c['clients']))
    try:
        await asyncio.sleep(c['before_seconds'])
        phase[0] = 'save'
        boundaries['save'] = time.monotonic()
        await control.command('BGSAVE')
        result['bgsave_response_ms'] = (time.monotonic()-boundaries['save'])*1000
        while True:
            info = parse_info(await control.command('INFO', 'persistence'))
            if info.get('rdb_bgsave_in_progress') == '0': break
            await asyncio.sleep(.01)
        result['save_status'] = info.get('rdb_last_bgsave_status')
        result['save_seconds'] = time.monotonic()-boundaries['save']
        phase[0] = 'after'
        boundaries['after'] = time.monotonic()
        await asyncio.sleep(c['after_seconds'])
    finally:
        boundaries['end'] = time.monotonic()
        stop.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        result['task_errors'] = [type(x).__name__ for x in outcomes if isinstance(x,Exception)]
        result['phase_boundaries_monotonic'] = boundaries
        rows = []
        ends = {'before':'save', 'save':'after', 'after':'end'}
        for (window, op), m in sorted(metrics.items()):
            duration = boundaries.get(ends[window], boundaries['end'])-boundaries[window]
            rows.append(dict(phase=window, operation=op, seconds=duration,
                             offered_per_sec=c['writes_per_sec' if op=='SET' else 'reads_per_sec'],
                             achieved_per_sec=m.completed/duration, completed=m.completed,
                             latency_samples=m.n, errors=m.errors, skipped_scheduled=m.skipped,
                             p50_ms=m.quantile(.5), p95_ms=m.quantile(.95), p99_ms=m.quantile(.99),
                             p999_ms=m.quantile(.999), max_ms=m.maximum))
        result['metrics'] = rows
        if rows:
            with (out/'metrics.csv').open('w') as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader(); writer.writerows(rows)
        await control.close()


def main():
    c = json.loads(Path(sys.argv[1]).read_text())
    out = Path('output').resolve()
    out.mkdir(exist_ok=False)
    result = {'schema':c['schema'], 'run_id':c['run_id'], 'setup':c, 'status':'failed',
              'limitations':['strings only', 'pipeline=1', 'same-host Python client may limit throughput',
                              'skipped offered requests counted; no unbounded catch-up',
                              'no cgroup limits imposed', 'no exact child count or kernel CoW measurement',
                              'RDB structural validation only; no point-in-time/full reload validation',
                              'not shared benchmark v2.0 compliant']}
    proc = None
    def terminate(signum, frame): raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM, terminate)
    try:
        # Refuse an occupied port: never benchmark or alter an existing Redis.
        with socket.socket() as s: s.bind(('127.0.0.1',c['port']))
        binary = Path(c['redis_binary'])
        result['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
        result['binary_version'] = subprocess.check_output([str(binary),'--version'], text=True).strip()
        args = [str(binary), '--bind','127.0.0.1','--port',str(c['port']), '--save','',
                '--appendonly','no','--protected-mode','yes','--dir',str(out),'--dbfilename','dump.rdb',
                '--rdbcompression','yes','--rdbchecksum','yes','--maxmemory','0', '--daemonize','no']
        if c['mode'] != 'fork': args += ['--bgsave-mode',c['mode']]
        # Explicit fork when supported; stock Redis rejects this config, so omit it.
        result['server_argv'] = args
        with (out/'server.log').open('w') as log:
            proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        for _ in range(100):
            if proc.poll() is not None: raise RuntimeError('Redis startup failed; see server.log')
            try:
                with socket.create_connection(('127.0.0.1',c['port']), timeout=.1): break
            except OSError: time.sleep(.1)
        asyncio.run(run(c,out,result))
        checker = binary.with_name('redis-check-rdb')
        with (out/'check-rdb.log').open('w') as f:
            checked = subprocess.run([str(checker),str(out/'dump.rdb')],stdout=f,stderr=subprocess.STDOUT)
        result['rdb_check_exit'] = checked.returncode
        if (checked.returncode == 0 and result.get('save_status') == 'ok'
                and not result.get('task_errors') and not any(r['errors'] for r in result['metrics'])):
            result['status'] = 'completed'
    except Exception as exc:
        result['error'] = str(exc)
    finally:
        if proc and proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        (out/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    sys.exit(0 if result['status'] == 'completed' else 1)


if __name__ == '__main__': main()
