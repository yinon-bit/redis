#!/usr/bin/env python3
"""Instrumented gated correctness fixtures. Not performance measurements."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import time

spec=importlib.util.spec_from_file_location('driver',Path(__file__).with_name('spec-worker.py'))
w=importlib.util.module_from_spec(spec); spec.loader.exec_module(w)


async def wait_file(path, seconds=30):
    end=time.monotonic()+seconds
    while not path.exists():
        if time.monotonic()>end: raise TimeoutError('Fixture gate was not reached')
        await asyncio.sleep(.001)


def marked(payload, ident, version=0):
    return struct.pack('<QQ',ident,version)+payload[16:]


async def canonical(conn, key):
    typ=await conn.command('TYPE',key)
    if typ==b'none': return None
    if typ==b'string': return ('string',await conn.command('GET',key))
    if typ==b'hash':
        fields=await conn.command('HGETALL',key)
        return ('hash',dict(zip(fields[::2],fields[1::2])))
    if typ==b'list': return ('list',await conn.command('LRANGE',key,0,-1))
    raise AssertionError('Unexpected fixture datatype')


async def save_finished(conn, expected='ok'):
    until=time.monotonic()+600
    while time.monotonic()<until:
        info=w.parse_info(await asyncio.wait_for(conn.command('INFO','persistence'),30))
        if info.get('rdb_bgsave_in_progress')=='0':
            if expected and info.get('rdb_last_bgsave_status')!=expected: raise AssertionError('Unexpected save status')
            return info
        await asyncio.sleep(.01)
    raise TimeoutError('Fixture snapshot did not complete')


async def routed(conn,name,expected,mode,events):
    typ=await conn.command('TYPE',name)
    if typ==b'none': return
    encoding=await conn.command('OBJECT','ENCODING',name)
    if expected is None:
        if typ!=b'hash': raise AssertionError('Encoding-dependent routing is only defined for hashes')
        expected=encoding==b'listpack'
    actual=await conn.command('DEBUG','forkless-arena',name)
    info=w.parse_info(await conn.command('INFO','persistence'))
    events.append({'event':'routing','t':time.monotonic(),'type':typ.decode(),'encoding':encoding.decode(),'arena':actual})
    if mode=='fork': return  # Arena migration is not applicable to the baseline.
    if actual<0: raise RuntimeError('Arena fixture needs jemalloc routing instrumentation')
    eligible=int(info['forkless_arena'])
    if (actual==eligible)!=expected: raise AssertionError('Unexpected arena routing')


def shape(variant, ident, payload):
    big=marked(payload[0]['big'],ident)
    small=marked(payload[0]['small'],ident)
    if variant=='string_shrink': return ('string',big)
    if variant in ('string_boundary','string_inplace','string_setrange'): return ('string',big[:511999])
    if variant=='hash_encoding': return ('hash',{k:marked(payload[0]['hash'],ident) for k in (b'a',b'b',b'c',b'd')})
    if variant=='hash_fields': return ('hash',{f'f{i:04d}'.encode():marked(payload[0]['hash'],ident) for i in range(512)})
    return ('string',small)


async def store(conn,name,value):
    typ,data=value
    if typ=='string': await conn.command('SET',name,data)
    else: await conn.command('HSET',name,*[x for pair in data.items() for x in pair])


async def mutation_trace(conn,variant,payload,mode,events):
    name='transition:000000'
    small=marked(payload[1]['small'],0,1); big=marked(payload[1]['big'],0,1)
    fields=[x for f in ('a','b','c','d') for x in (f,marked(payload[1]['hash'],0,1))]
    traces={
        'string_grow':[(('SET',name,big),False)],
        'string_shrink':[(('SET',name,small),True)],
        'string_boundary':[(('SET',name,big[:n]),n<=512000) for n in (512000,512001,512000,511999)],
        'string_inplace':[(('APPEND',name,b'zz'),False),(('SET',name,small),True)],
        'string_setrange':[(('SETRANGE',name,512000,b'z'),False),(('SET',name,small),True)],
        'hash_encoding':[(('HSET',name,'fifth',b'z'*65),False),(('HDEL',name,'fifth'),None),
                         (('DEL',name),False),(('HSET',name,*fields),True)],
        'hash_fields':[(('HSET',name,'f0512',b'z'*32),False),(('HDEL',name,'f0512'),None),
                       (('DEL',name),False),(('HSET',name,*[x for i in range(512) for x in (f'f{i:04d}',b'z'*32)]),True)],
        'type_replace':[(('SET',name,big),False),(('DEL',name),False),(('HSET',name,*fields),True),
                        (('DEL',name),False),(('RPUSH',name,b'z'*32),False),(('SET',name,small),True)]}
    for args,eligible in traces[variant]:
        sent=time.monotonic(); await asyncio.wait_for(conn.command(*args),30)
        events.append({'event':'mutation','command':args[0],'sent':sent,'replied':time.monotonic()})
        await routed(conn,name,eligible,mode,events)
    if variant=='string_grow': expected=('string',big)
    elif variant=='string_boundary': expected=('string',big[:511999])
    elif variant=='hash_encoding': expected=('hash',{k.encode():marked(payload[1]['hash'],0,1) for k in ('a','b','c','d')})
    elif variant=='hash_fields': expected=('hash',{f'f{i:04d}'.encode():b'z'*32 for i in range(512)})
    else: expected=('string',small)
    if await canonical(conn,name)!=expected: raise AssertionError('Live transition value mismatch')
    events.append({'event':'live_target_valid'})


async def controlled(conn,directory,mutate,events,position='before_visit'):
    task=None
    (directory/'fixture.armed').touch()
    await conn.command('BGSAVE')
    try:
        await wait_file(directory/'fixture.arrived')
        events.append({'event':'gate_arrived','t':time.monotonic()})
        task=asyncio.create_task(mutate())
        # During capture a write may block on the reader. Other positions hold
        # snapshot completion until the entire mutation trace is acknowledged.
        if position=='during_capture': await asyncio.sleep(.05)
        else: await asyncio.wait_for(asyncio.shield(task),20)
        (directory/'fixture.release').touch()
        events.append({'event':'gate_release','t':time.monotonic()})
        await asyncio.wait_for(task,120)
        await save_finished(conn)
        if (directory/'fixture.timeout').exists(): raise AssertionError('Gate timed out rather than being released')
    finally:
        (directory/'fixture.release').touch()
        (directory/'fixture.armed').unlink(missing_ok=True)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)


async def snapshot_data(conn,case,payload,live=False,load=False):
    counts={'small':case['small_keys'],'big':case['big_keys'],'hash':case['hash_keys']}
    changes=min(1000,counts['small']//4)
    expected=0
    for kind,n in counts.items():
        ids=list(range(n))
        if live and kind=='small': ids=[i for i in ids if not changes<=i<2*changes and not 2*changes<=i<2*changes+min(100,changes)]+list(range(n,n+changes))
        for i in ids:
            version=int(live and ((kind=='small' and (i<changes or i>=n)) or kind=='big' or (kind=='hash' and i<min(100,n))))
            v=marked(payload[version][kind],i,version)
            item=('hash',{k:v for k in (b'a',b'b',b'c',b'd')}) if kind=='hash' else ('string',v)
            name=w.key(kind,i)
            if load: await store(conn,name,item)
            elif await canonical(conn,name)!=item: raise AssertionError('Snapshot/live mismatch: '+name)
            expected+=1
    if live:
        for i in range(2*changes,2*changes+min(100,changes)):
            name=f'renamed:{i:09d}'
            if await canonical(conn,name)!=('string',marked(payload[0]['small'],i)): raise AssertionError('Rename mismatch')
            expected+=1
    if not load and await conn.command('DBSIZE')!=expected: raise AssertionError('Unexpected extra/missing keys')


async def snapshot_mutate(conn,case,payload,events):
    changes=min(1000,case['small_keys']//4)
    for kind,ids in [('small',range(changes)),('delete',range(changes,2*changes)),
                     ('small',range(case['small_keys'],case['small_keys']+changes)),
                     ('big',range(case['big_keys'])),('hash',range(min(100,case['hash_keys']))),
                     ('rename',range(2*changes,2*changes+min(100,changes)))]:
        for i in ids:
            sent=time.monotonic()
            if kind=='delete': await asyncio.wait_for(conn.command('DEL',w.key('small',i)),30)
            elif kind=='rename': await asyncio.wait_for(conn.command('RENAME',w.key('small',i),f'renamed:{i:09d}'),30)
            else: await asyncio.wait_for(conn.command(*w.write_args(kind,w.key(kind,i),marked(payload[1][kind],i,1))),30)
            events.append({'event':'mutation','kind':kind,'id':i,'sent':sent,'replied':time.monotonic()})


async def arena_data(conn,case,payload,load=False):
    for i in range(case['small_keys']):
        name=w.key('small',i)
        if load: await conn.command('SET',name,payload[0]['small'])
        elif await conn.command('GET',name)!=payload[0]['small']: raise AssertionError('Background snapshot mismatch')
    for i in range(case['target_keys']):
        name=f'transition:{i:06d}'; expected=shape(case['variant'],i,payload)
        if load: await store(conn,name,expected)
        elif await canonical(conn,name)!=expected: raise AssertionError('Target snapshot mismatch: '+name)
    if not load and await conn.command('DBSIZE')!=case['small_keys']+case['target_keys']: raise AssertionError('Arena DB size mismatch')


async def cancel_action(conn,directory,events):
    release=None
    before=w.digest(directory/'dump.rdb')
    (directory/'gate.armed').touch(); await conn.command('BGSAVE')
    try:
        await wait_file(directory/'gate.arrived')
        sent=time.monotonic()
        conn.send('DEBUG','bench-cancel'); await conn.writer.drain()
        async def release_later():
            await asyncio.sleep(.1)
            (directory/'gate.release').touch()
        release=asyncio.create_task(release_later())
        await asyncio.wait_for(conn.reply(),30)
        acknowledged=time.monotonic()
        deadline=sent+30
        while time.monotonic()<deadline:
            info=w.parse_info(await conn.command('INFO','persistence'))
            if info['rdb_bgsave_in_progress']=='0' and int(info.get('forkless_snapshot_bytes','0'))==0: break
            await asyncio.sleep(.01)
        else: raise TimeoutError('Cancellation cleanup timeout')
        if w.digest(directory/'dump.rdb')!=before: raise AssertionError('Cancellation replaced previous RDB')
        if (directory/'gate.timeout').exists(): raise AssertionError('Output gate timed out')
        data={'cancel_ack_ms':(acknowledged-sent)*1000,'cancel_cleanup_ms':(time.monotonic()-sent)*1000,
              'previous_rdb_preserved':True,'output_gate_release_delay_ms':100,
              'api':'test-only DEBUG bench-cancel -> killRDBChild'}
        events.append(data); return data
    finally:
        (directory/'gate.release').touch(); (directory/'gate.armed').unlink(missing_ok=True)
        if release:
            release.cancel()
            await asyncio.gather(release,return_exceptions=True)


async def attempt(c,directory,resources,payload):
    case=c['case']; result={'status':'failed','events':[]}; events=result['events']
    proc=None; conn=None; lib=None
    try:
        if c['mode'] not in ('fork','hybrid'): raise RuntimeError('Fixture adapter currently supports fork/hybrid only')
        if case['scenario']=='cancellation_retry':
            lib=directory/'pause.so'
            subprocess.run(['cc','-shared','-fPIC','-O2','-o',str(lib),str(Path(__file__).with_name('scenario-eio.c')),'-ldl','-pthread'],check=True)
        proc,conn=await w.start(c,directory,resources,lib)
        await conn.command('CONFIG','SET','hash-max-listpack-entries','512')
        await conn.command('CONFIG','SET','hash-max-listpack-value','64')
        if case['scenario']=='arena_transition': await arena_data(conn,case,payload,True)
        elif case['scenario']=='point_in_time': await snapshot_data(conn,case,payload,load=True)
        else: await w.populate(conn,case,payload)
        await conn.command('SAVE')
        result['seed_rdb_sha256']=w.digest(directory/'dump.rdb')
        if case['scenario']=='arena_transition':
            variant=case['variant']
            await routed(conn,'transition:000000',variant!='string_shrink',c['mode'],events)
            await controlled(conn,directory,lambda:mutation_trace(conn,variant,payload,c['mode'],events),events,case['serializer_position'])
            result['live_target_type']=(await conn.command('TYPE','transition:000000')).decode()
        elif case['scenario']=='point_in_time':
            await controlled(conn,directory,lambda:snapshot_mutate(conn,case,payload,events),events)
            await snapshot_data(conn,case,payload,live=True)
            result['live_state_valid']=True
        else:
            result['measurement']=await w.measure(c,conn,directory,resources,payload,
                action=lambda con,path:cancel_action(con,path,events))
            rows=result['measurement']['metrics']
            result['writes_completed_during_cancellation']=sum(r['completed'] for r in rows
                if r['phase']=='save' and r['operation'] in ('SET','HSET'))
            result['write_availability']='not_applicable' if case['workload']=='idle' else (
                'observed' if result['writes_completed_during_cancellation'] else 'not_observed')
            result['retry']=await w.save(conn,directory)
        await conn.close(); conn=None; w.stop_process(proc); proc=None
        if case['scenario']=='cancellation_retry':
            result['validation']=await asyncio.wait_for(w.validate(c,directory,case,payload),600)
        else:
            checker=str(Path(c['redis_binary']).with_name('redis-check-rdb'))
            with (directory/'check-rdb.log').open('w') as f:
                subprocess.run([checker,str(directory/'dump.rdb')],stdout=f,stderr=subprocess.STDOUT,check=True,timeout=600)
            proc,conn=await w.start(dict(c,mode='fork'),directory)
            if case['scenario']=='arena_transition': await asyncio.wait_for(arena_data(conn,case,payload),600)
            else: await asyncio.wait_for(snapshot_data(conn,case,payload),600)
            result['snapshot_cut_values_valid']=True
        result['status']='completed'
    except Exception as exc: result['error']=str(exc)
    finally:
        (directory/'fixture.release').touch(); (directory/'gate.release').touch()
        if conn: await conn.close()
        if proc: w.stop_process(proc)
        (directory/'result.json').write_text(json.dumps(result,indent=2))
    return result


def main():
    c=json.loads(Path(sys.argv[1]).read_text()); c['fixture']=True
    out=Path(c['data_dir'])/c['run_id']; out.mkdir()
    result={'schema':'gated-fixtures/1','case_id':c['case']['case_id'],'status':'failed',
            'instrumented':True,'limitations':['Only fork/hybrid adapter','Gated variants only; no ungated arena oscillation',
                'Snapshot base trace only; expiry and FLUSH variants not included','Cancellation uses test-only API wrapper']}
    resources=w.Resources(c)
    def terminated(signum,frame): raise SystemExit(128+signum)
    signal.signal(signal.SIGTERM,terminated)
    try:
        manifest=Path(c['redis_binary']).resolve().parents[1]/'fixture-build.json'
        if not manifest.is_file(): raise RuntimeError('Missing isolated fixture-build manifest')
        result['fixture_build']=json.loads(manifest.read_text())
        if not result['fixture_build'].get('instrumented'): raise RuntimeError('Not an instrumented fixture build')
        if out.stat().st_dev==Path('/').stat().st_dev: raise RuntimeError('Fixture data must be on dedicated filesystem')
        result['settings']=resources.create(); result['binary_sha256']=w.digest(c['redis_binary'])
        payload=w.values(); result['attempts']=[]
        for i in range(c['case']['repetitions']):
            directory=out/f'r{i+1}'; directory.mkdir()
            result['attempts'].append(asyncio.run(attempt(c,directory,resources,payload)))
        result['status']='completed' if all(a['status']=='completed' for a in result['attempts']) else 'failed'
    except Exception as exc: result['error']=str(exc)
    finally:
        try: resources.close()
        except Exception as exc: result['cleanup_error']=str(exc); result['status']='failed'
        (out/'result.json').write_text(json.dumps(result,indent=2)); Path('output').symlink_to(out,target_is_directory=True)
    sys.exit(0 if result['status']=='completed' else 1)


if __name__=='__main__': main()
