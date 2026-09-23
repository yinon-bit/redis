#!/usr/bin/env python3
"""Executable section-6 cases. Reports deviations; never claims full v2 compliance."""
import asyncio
import collections
import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time

spec = importlib.util.spec_from_file_location('scenario_worker', Path(__file__).with_name('scenario-worker.py'))
shared = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared)
Connection, Metric, parse_info = shared.Connection, shared.Metric, shared.parse_info
MASK = (1 << 64)-1


def next_random(s):
    s ^= (s << 13) & MASK
    s ^= s >> 7
    s ^= (s << 17) & MASK
    return s


def values():
    result = {}
    for version in range(2):
        s = 192837+version*98765
        big = bytearray(1048576)
        for i in range(len(big)):
            s = next_random(s)
            big[i] = s & 255
        result[version] = {'big': bytes(big), 'small': bytes(big[:128])+bytes([120+version])*128,
                           'hash': bytes(big[256:288])}
    return result


def key(kind, i):
    return f'{kind}:{i:0{dict(small=9,big=6,hash=7)[kind]}d}'


def write_args(kind, name, value):
    return ('HSET', name, 'a', value, 'b', value, 'c', value, 'd', value) if kind == 'hash' else ('SET',name,value)


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''): h.update(block)
    return h.hexdigest()


class Resources:
    def __init__(self, c):
        self.c = c
        self.path = Path('/sys/fs/cgroup') / ('redis-bench-'+c['run_id'])
        self.created = False

    def create(self):
        if sys.platform != 'linux' or os.geteuid() != 0:
            raise RuntimeError('Named cases require Linux root and delegated cgroup v2 controllers')
        if not set(range(8)).issubset(os.sched_getaffinity(0)):
            raise RuntimeError('Named cases require CPUs 0–7; no silent CPU remapping')
        if len(Path('/proc/swaps').read_text().splitlines()) > 1:
            raise RuntimeError('Host swap must be disabled before running this suite')
        if '[never]' not in Path('/sys/kernel/mm/transparent_hugepage/enabled').read_text():
            raise RuntimeError('THP must be set to never before running')
        if Path('/proc/sys/vm/overcommit_memory').read_text().strip() != '1':
            raise RuntimeError('vm.overcommit_memory must be 1')
        self.path.mkdir()
        self.created = True
        cpus = '2' if self.c['case']['cpu_profile']=='cpu1' else '2-7'
        settings = {'memory.max':str(self.c['case']['memory_limit_bytes']), 'memory.swap.max':'0',
                    'memory.high':'max', 'memory.oom.group':'0', 'cpu.max':('100000' if cpus=='2' else '600000')+' 100000',
                    'cpuset.cpus':cpus, 'cpuset.mems':Path('/sys/fs/cgroup/cpuset.mems.effective').read_text().strip()}
        for name, value in settings.items():
            if not (self.path/name).exists(): raise RuntimeError('Missing delegated controller: '+name)
            (self.path/name).write_text(value)
        os.sched_setaffinity(0, {0,1})
        return settings

    def enter(self):
        (self.path/'cgroup.procs').write_text(str(os.getpid()))
        os.sched_setaffinity(0, {2} if self.c['case']['cpu_profile']=='cpu1' else set(range(2,8)))

    def sample(self):
        return {name:(self.path/name).read_text() for name in
                ('memory.current','memory.stat','memory.events','cpu.stat','memory.pressure','io.pressure','cpu.pressure')}

    def close(self):
        if not self.created: return
        # This uniquely created cgroup contains only this run's Redis descendants.
        if (self.path/'cgroup.kill').exists(): (self.path/'cgroup.kill').write_text('1')
        for _ in range(100):
            try: self.path.rmdir(); return
            except OSError: time.sleep(.05)
        raise RuntimeError('Benchmark cgroup cleanup incomplete: '+str(self.path))


async def connect(port):
    return await asyncio.wait_for(Connection().open(port),10)


async def start(c, directory, resources=None, injection=None):
    binary = c['redis_binary']
    args = [binary, '--bind','127.0.0.1','--port',str(c['port']), '--dir',str(directory),
            '--dbfilename','dump.rdb','--save','', '--appendonly','no','--daemonize','no',
            '--maxmemory','0','--maxmemory-policy','allkeys-lfu','--lfu-decay-time','1',
            '--activedefrag','no','--rdbcompression','yes','--rdbchecksum','yes',
            '--rdb-save-incremental-fsync','yes','--io-threads','1','--io-threads-do-reads','no',
            '--hz','10','--dynamic-hz','no','--lazyfree-lazy-eviction','no',
            '--lazyfree-lazy-expire','no','--lazyfree-lazy-server-del','no',
            '--stop-writes-on-bgsave-error','no','--latency-monitor-threshold','1']
    if c['mode'] != 'fork': args += ['--bgsave-mode',c['mode']]
    if c.get('fixture'): args += ['--enable-debug-command','yes']
    env = os.environ.copy()
    if injection: env.update(LD_PRELOAD=str(injection), BENCH_EIO_DIR=str(directory))
    if c.get('fixture'):
        env.update(BENCH_FIXTURE_DIR=str(directory),BENCH_FIXTURE_POSITION=c['case'].get('serializer_position','before_visit'))
        if injection: env['BENCH_EIO_MODE']='pause'
    with (directory/'server.log').open('ab') as log:
        proc = subprocess.Popen(args, stdout=log, stderr=log, env=env,
                                preexec_fn=resources.enter if resources else None)
    try:
        deadline = time.monotonic()+600
        while time.monotonic()<deadline:
            if proc.poll() is not None: raise RuntimeError('Redis exited while loading; see server.log and cgroup counters')
            try:
                conn = await connect(c['port'])
                await conn.command('PING')
                break
            except (OSError, RuntimeError):
                if 'conn' in locals(): await conn.close()
                await asyncio.sleep(.05)
        else: raise TimeoutError('Redis startup/load timeout')
        info = parse_info(await conn.command('INFO','server'))
        if int(info['process_id']) != proc.pid: raise RuntimeError('Redis PID mismatch; refusing existing server')
        if c['mode']=='fork':
            try: await conn.command('CONFIG','SET','bgsave-mode','fork')
            except RuntimeError as e:
                if 'Unknown option' not in str(e): raise
        cfg = await conn.command('CONFIG','GET','*')
        (directory/'effective-config.json').write_text(json.dumps([x.decode(errors='replace') for x in cfg],indent=2))
        return proc, conn
    except BaseException:
        stop_process(proc)
        raise


def stop_process(proc):
    if proc.poll() is None:
        proc.terminate()
        try: proc.wait(10)
        except subprocess.TimeoutExpired: proc.kill(); proc.wait()


async def populate(conn, case, payload):
    for kind in ('small','big','hash'):
        n = case[kind+'_keys']
        batch = 1 if kind=='big' else 128
        for first in range(0,n,batch):
            length = min(batch,n-first)
            for i in range(first,first+length): conn.send(*write_args(kind,key(kind,i),payload[0][kind]))
            await conn.writer.drain()
            for _ in range(length): await conn.reply()


async def validate(c, directory, case, payload, seed=False):
    check = await asyncio.create_subprocess_exec(str(Path(c['redis_binary']).with_name('redis-check-rdb')),
                  str(directory/'dump.rdb'), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        output, _ = await check.communicate()
    finally:
        if check.returncode is None:
            check.kill()
            await check.wait()
    (directory/'check-rdb.log').write_bytes(output)
    if check.returncode: raise RuntimeError('Structural RDB validation failed')
    proc, conn = await start(dict(c,mode='fork'), directory)
    try:
        expected = sum(case[k+'_keys'] for k in ('small','big','hash'))
        if await conn.command('DBSIZE') != expected: raise AssertionError('DBSIZE mismatch on reload')
        for kind in ('small','big','hash'):
            for first in range(0,case[kind+'_keys'],128 if kind!='big' else 1):
                last = min(case[kind+'_keys'],first+(128 if kind!='big' else 1))
                for i in range(first,last):
                    if kind=='hash':
                        conn.send('HGETALL',key(kind,i))
                        if seed: conn.send('OBJECT','ENCODING',key(kind,i))
                    else: conn.send('GET',key(kind,i))
                await conn.writer.drain()
                for i in range(first,last):
                    value = await conn.reply()
                    if kind=='hash':
                        fields = dict(zip(value[::2],value[1::2]))
                        if set(fields)!={b'a',b'b',b'c',b'd'}: raise AssertionError('Invalid hash fields')
                        if len(set(fields.values()))!=1: raise AssertionError('Torn hash')
                        value = fields[b'a']
                        if seed and await conn.reply()!=b'listpack': raise AssertionError('Seed hash is not listpack')
                    allowed = (payload[0][kind],) if seed else (payload[0][kind],payload[1][kind])
                    if value not in allowed: raise AssertionError('Missing/invalid payload: '+key(kind,i))
        return {'ok':True,'keys':expected,'scope':'full key and payload reload; not point-in-time proof'}
    finally:
        await conn.close()
        stop_process(proc)


async def save(conn, directory, expected='ok', gate=False):
    sent = time.monotonic()
    old = (directory/'dump.rdb').stat().st_mtime_ns if (directory/'dump.rdb').exists() else None
    await conn.command('BGSAVE')
    response = time.monotonic()
    observed = False
    if gate:
        # Independent filesystem gate: never wait on a Redis command while held.
        deadline = time.monotonic()+30
        while not (directory/'gate.arrived').exists():
            if time.monotonic()>deadline: raise TimeoutError('EIO gate did not arrive')
            await asyncio.sleep(.001)
        (directory/'gate.release').touch()
    while time.monotonic()-sent < 600:
        info = parse_info(await asyncio.wait_for(conn.command('INFO','persistence'),30))
        observed |= info.get('rdb_bgsave_in_progress')=='1'
        if info.get('rdb_bgsave_in_progress')=='0':
            status = info.get('rdb_last_bgsave_status')
            if status != expected: raise RuntimeError('Unexpected BGSAVE status: '+str(status))
            published = (directory/'dump.rdb').stat().st_mtime_ns != old
            if not observed and not published and not (gate and (directory/'gate.injected').exists()):
                raise RuntimeError('No evidence this BGSAVE attempt completed')
            return {'bgsave_response_ms':(response-sent)*1000,'save_seconds':time.monotonic()-sent,
                    'active_observed':observed,'publication_changed':published,'status':status,
                    'info':info}
        await asyncio.sleep(.01)
    raise TimeoutError('BGSAVE timeout')


async def measure(c, conn, directory, resources, payload, action=None):
    case = c['case']
    metrics = collections.defaultdict(Metric)
    phase = ['before']; end = asyncio.Event(); begin = asyncio.Event()
    ready = []
    bounds = {}

    async def client(ident):
        r = await connect(c['port']); ready.append(ident)
        state = 1234567+ident*314159
        await begin.wait()
        try:
            while not end.is_set():
                pending = []
                for _ in range(case['pipeline']):
                    state=next_random(state); category=state%10000
                    dataset=case['dataset_id']
                    if dataset=='big_only': kind='big'
                    elif dataset=='big_heavy': kind='big' if category<9800 else 'small'
                    else:
                        kind='big' if category<2 else 'hash' if category<102 else 'small'
                        if not case[kind+'_keys']: kind='small'
                    state=next_random(state); name=key(kind,state%case[kind+'_keys'])
                    if case['workload']=='rw50':
                        state=next_random(state); reading=state%100<50
                    else: reading=case['workload']=='read100'
                    op=('HGET' if kind=='hash' else 'GET') if reading else ('HSET' if kind=='hash' else 'SET')
                    args=(op,name,'a') if op=='HGET' else (op,name) if reading else write_args(kind,name,payload[1][kind])
                    pending.append((phase[0],op,kind,time.monotonic(),reading))
                    r.send(*args)
                await r.writer.drain()
                for sent_phase,op,kind,sent,reading in pending:
                    value=await asyncio.wait_for(r.reply(),30)
                    if reading and value is None: raise AssertionError('Unexpected read miss')
                    metrics[(sent_phase,op,kind)].add((time.monotonic()-sent)*1000)
                    metrics[(phase[0],op,kind)].completed+=1
        finally: await r.close()

    async def probe():
        r=await connect(c['port']); await begin.wait()
        try:
            while not end.is_set():
                sent=time.monotonic(); p=phase[0]
                await asyncio.wait_for(r.command('PING'),30)
                metrics[(p,'PING','control')].add((time.monotonic()-sent)*1000)
                metrics[(phase[0],'PING','control')].completed+=1
                await asyncio.sleep(.001)
        finally: await r.close()

    async def monitor():
        r=await connect(c['port']); await begin.wait()
        try:
            with (directory/'telemetry.jsonl').open('w') as f:
                while not end.is_set():
                    data={'t':time.monotonic(),'phase':phase[0],'cgroup':resources.sample(),
                          'info':parse_info(await asyncio.wait_for(r.command('INFO','all'),30))}
                    f.write(json.dumps(data)+'\n'); f.flush(); await asyncio.sleep(.1)
        finally: await r.close()

    tasks=[asyncio.create_task(client(i)) for i in range(case['clients'])]
    tasks += [asyncio.create_task(probe()),asyncio.create_task(monitor())]
    try:
        ready_deadline=time.monotonic()+30
        while len(ready)<case['clients']:
            if time.monotonic()>ready_deadline: raise TimeoutError('Load clients did not become ready')
            if any(t.done() for t in tasks): raise RuntimeError('Client failed during startup')
            await asyncio.sleep(.001)
        bounds['before']=time.monotonic(); begin.set()
        await asyncio.sleep(case['before_seconds'])
        phase[0]='save'; bounds['save']=time.monotonic()
        result=await (action(conn,directory) if action else save(conn,directory))
        phase[0]='after'; bounds['after']=time.monotonic()
        await asyncio.sleep(case['after_seconds'])
    finally:
        bounds['end']=time.monotonic(); phase[0]='drain'; end.set(); begin.set()
        outcomes=await asyncio.gather(*tasks,return_exceptions=True)
        bounds['drain_end']=time.monotonic()
        rows=[]
        for (p,op,kind), m in sorted(metrics.items()):
            left=bounds.get(p,bounds['end'])
            right=bounds.get({'before':'save','save':'after','after':'end','drain':'drain_end'}[p],bounds['end'])
            duration=max(right-left,1e-9)
            rows.append(dict(phase=p,operation=op,key_class=kind,seconds=duration,completed=m.completed,
                             ops_per_sec=m.completed/duration,latency_samples=m.n,p50_ms=m.quantile(.5),
                             p95_ms=m.quantile(.95),p99_ms=m.quantile(.99),p999_ms=m.quantile(.999),max_ms=m.maximum))
        if rows:
            with (directory/'metrics.csv').open('w') as f:
                w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
        (directory/'client-errors.json').write_text(json.dumps([str(x) for x in outcomes if isinstance(x,BaseException)]))
    if any(isinstance(x,BaseException) for x in outcomes): raise RuntimeError('Load/probe/telemetry failure; see client-errors.json')
    return dict(result,metrics=rows,boundaries=bounds)


async def execute(c, out, resources, result):
    case=c['case']; payload=values()
    seed=out/'seed'; seed.mkdir()
    proc,conn=await start(dict(c,mode='fork'),seed,resources)
    try:
        await populate(conn,case,payload)
        await conn.command('SAVE')
    finally:
        await conn.close(); stop_process(proc)
    result['seed_sha256']=digest(seed/'dump.rdb')
    result['seed_validation']=await asyncio.wait_for(validate(c,seed,case,payload,True),600)
    injector=None
    if case['lifecycle']=='after_failure':
        injector=(out/'scenario-eio.so').resolve()
        subprocess.run(['cc','-shared','-fPIC','-O2','-o',str(injector),str(Path(__file__).with_name('scenario-eio.c')),'-ldl','-pthread'],check=True)
        result['injector_sha256']=digest(injector)
    attempts=[]; result['attempts']=attempts
    for repetition in range(1,case['repetitions']+1):
        directory=out/f'r{repetition}'; directory.mkdir()
        shutil.copyfile(seed/'dump.rdb',directory/'dump.rdb')
        attempt={'repetition':repetition,'status':'failed','cgroup_before':resources.sample()}; attempts.append(attempt)
        proc=None; conn=None
        try:
            proc,conn=await start(c,directory,resources,injector)
            if case['lifecycle']=='subsequent':
                attempt['prime']=await save(conn,directory)
                # Validate a copy in a separate process, with source stopped from traffic.
                checkpoint=directory/'prime'; checkpoint.mkdir()
                shutil.copyfile(directory/'dump.rdb',checkpoint/'dump.rdb')
                attempt['prime_validation']=await asyncio.wait_for(validate(dict(c,port=c['port']+1),checkpoint,case,payload,True),600)
                await asyncio.sleep(5)
            elif case['lifecycle']=='after_failure':
                previous=digest(directory/'dump.rdb'); (directory/'gate.armed').touch()
                try: attempt['injected_save']=await save(conn,directory,'err',gate=True)
                finally:
                    (directory/'gate.release').touch()
                    (directory/'gate.armed').unlink(missing_ok=True)
                if not (directory/'gate.injected').exists() or (directory/'gate.timeout').exists():
                    raise AssertionError('Fault gate did not inject exactly one controlled EIO')
                if digest(directory/'dump.rdb')!=previous: raise AssertionError('Failed save replaced previous RDB')
                attempt['previous_rdb_preserved']=True
                await asyncio.sleep(5)
            attempt['measurement']=await measure(c,conn,directory,resources,payload)
            await conn.close(); conn=None
            stop_process(proc); proc=None
            attempt['validation']=await asyncio.wait_for(validate(c,directory,case,payload),600)
            attempt['status']='completed'
        except Exception as exc: attempt['error']=str(exc)
        finally:
            if conn: await conn.close()
            if proc: stop_process(proc)
            attempt['cgroup_final']=resources.sample()
            (directory/'result.json').write_text(json.dumps(attempt,indent=2))
    result['status']='completed' if all(a['status']=='completed' for a in attempts) else 'failed'


def main():
    c=json.loads(Path(sys.argv[1]).read_text()); out=Path(c['data_dir'])/c['run_id']; out.mkdir()
    result={'schema':'document-cases/1','run_id':c['run_id'],'case_id':c['case']['case_id'],
            'setup':c,'status':'failed','deviations':[
                'Shared Python generator; client capacity must be checked, not the historical C generator binary.',
                'No exact child trace, snapshot event adapter, or profiling attribution; INFO counters are retained.',
                'Fast-save completion corroborated by RDB publication, not an adapter event.',
                'Full payload reload validation is not a point-in-time fixture.',
                'AWS storage/image contract is recorded by operator setup, not automatically provisioned.',
                'After-failure interposer remains loaded disarmed in timed retry; report as instrumented.',
                'Results use document-cases/1, not complete shared v2 output schema.']}
    resources=Resources(c)
    def terminate(signum, frame): raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,terminate)
    try:
        if out.stat().st_dev == Path('/').stat().st_dev:
            raise RuntimeError('--data-dir must be on a separate data filesystem, not the root disk')
        result['filesystem']=subprocess.check_output(['findmnt','--json','--target',str(out)],text=True)
        for port in (c['port'],c['port']+1):
            with socket.socket() as s: s.bind(('127.0.0.1',port))
        result['binary_sha256']=digest(c['redis_binary'])
        result['cgroup_settings']=resources.create()
        asyncio.run(execute(c,out,resources,result))
    except Exception as exc: result['error']=str(exc)
    finally:
        if resources.created:
            try: result['cgroup_final']=resources.sample()
            except Exception as exc: result['telemetry_error']=str(exc)
        try: resources.close()
        except Exception as exc: result['cleanup_error']=str(exc); result['status']='failed'
        (out/'result.json').write_text(json.dumps(result,indent=2))
        # The SSM wrapper uploads the output link contents without copying large RDBs.
        Path('output').symlink_to(out,target_is_directory=True)
    sys.exit(0 if result['status']=='completed' else 1)


if __name__=='__main__': main()
