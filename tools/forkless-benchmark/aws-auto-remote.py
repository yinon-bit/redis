#!/usr/bin/env python3
"""Private bootstrap payload. No AWS credentials; object-scoped URLs only."""
import hashlib
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tarfile
import time
import urllib.request


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()


def put(url, path):
    # Do not log exceptions: HTTP exceptions can include signed URLs.
    for attempt in range(3):
        try:
            with Path(path).open('rb') as f:
                req=urllib.request.Request(url,data=f,method='PUT',headers={'Content-Length':str(Path(path).stat().st_size)})
                with urllib.request.urlopen(req,timeout=120) as response:
                    if response.status not in (200,204): raise RuntimeError('Upload failed')
            return
        except Exception:
            if attempt==2: raise RuntimeError('Object upload failed; check credential lifetime and network') from None
            time.sleep(2)


def execute(config):
    root=Path('/opt/redis-auto'); root.mkdir(exist_ok=True)
    evidence=root/'evidence'; evidence.mkdir(exist_ok=True)
    state={'status':'starting','cases':[],'deadline_epoch':config['deadline_epoch']}

    def status():
        path=evidence/'status.json'; path.write_text(json.dumps(state,indent=2))
        put(config['status_url'],path)

    def bundle(number):
        # Preserve metrics/logs/source identity, not multi-GB RDBs after validation.
        paths=[p for p in evidence.rglob('*') if p.is_file() and not p.is_symlink() and p.name!='SHA256SUMS']
        if config.get('quick'):
            paths=[p for p in paths if p.parent==evidence or p.relative_to(evidence).parts[0]==f'case-{number}']
            paths=[p for p in paths if p.name!='receipt.json']
        (evidence/'SHA256SUMS').write_text(''.join(f'{sha(p)}  {p.relative_to(evidence)}\n' for p in paths))
        archive=root/f'artifacts-{number}.tar.gz'
        with tarfile.open(archive,'w:gz',compresslevel=1) as t:
            if config.get('quick'):
                for p in paths+[evidence/'SHA256SUMS']:
                    t.add(p,arcname='evidence/'+str(p.relative_to(evidence)),recursive=False)
            else:
                t.add(evidence,arcname='evidence')
        put(config['artifact_urls'][number],archive)
        receipt=evidence/'receipt.json'
        receipt.write_text(json.dumps({'archive':archive.name,'sha256':sha(archive)}))
        put(config['receipt_urls'][number],receipt)

    def command(args, log):
        remaining=int(config['deadline_epoch']-time.time()-300)
        if remaining<=0: raise TimeoutError('Deadline reserve reached')
        with (evidence/log).open('ab') as f:
            subprocess.run(args,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=remaining)

    try:
        # The shell bootstrap already armed an absolute shutdown timer.
        state['status']='timer_armed'; status()
        command(['apt-get','update','-qq'],'build.log')
        command(['apt-get','install','-y','-qq','build-essential','tcl','pkg-config','libssl-dev','python3','ca-certificates'],'build.log')
        # Only a uniquely identified blank 100-GiB disk on this newly created host.
        disks=json.loads(subprocess.check_output(['lsblk','-J','-b','-o','PATH,TYPE,SIZE,FSTYPE,MOUNTPOINTS'],text=True))['blockdevices']
        candidates=[d for d in disks if d['type']=='disk' and int(d['size'])==100*1024**3
                    and not d.get('children') and not d.get('fstype') and not any(d.get('mountpoints') or [])]
        if len(candidates)!=1: raise RuntimeError('Cannot uniquely identify new blank benchmark data disk')
        device=candidates[0]['path']
        command(['mkfs.ext4','-F',device],'build.log')
        Path('/mnt/redis-benchmark').mkdir(exist_ok=True)
        command(['mount','-o','noatime',device,'/mnt/redis-benchmark'],'build.log')
        subprocess.run(['swapoff','-a'],check=True)
        Path('/sys/kernel/mm/transparent_hugepage/enabled').write_text('never')
        Path('/proc/sys/vm/overcommit_memory').write_text('1')
        Path('/sys/fs/cgroup/cgroup.subtree_control').write_text('+cpu +memory +cpuset')
        archive=root/'source.tar.gz'
        try:
            with urllib.request.urlopen(config['source_url'],timeout=120) as src, archive.open('wb') as dst:
                while chunk:=src.read(1024*1024): dst.write(chunk)
        except Exception: raise RuntimeError('Source download failed') from None
        if sha(archive)!=config['source_sha256']: raise RuntimeError('Source checksum mismatch')
        source=root/'source'; source.mkdir()
        with tarfile.open(archive) as t:
            for m in t.getmembers():
                if not m.isfile() or Path(m.name).is_absolute() or '..' in Path(m.name).parts:
                    raise RuntimeError('Unsafe source archive member')
            t.extractall(source,filter='data')
        (evidence/'source-sha256.txt').write_text(config['source_sha256']+'\n')
        command(['make','-C',str(source/'src'),'-j6','MALLOC=jemalloc'],'build.log')
        tools=source/'tools/forkless-benchmark'
        seed_manifest={}; published=set()
        if config.get('quick'):
            cache=Path('/mnt/redis-benchmark/seed-cache'); cache.mkdir()
            for key, entry in config['seeds'].items():
                if 'get_url' not in entry: continue
                dest=cache/(key+'.rdb')
                try:
                    with urllib.request.urlopen(entry['get_url'],timeout=120) as src, dest.open('wb') as dst:
                        shutil.copyfileobj(src,dst)
                except Exception: raise RuntimeError('Seed download failed') from None
                if sha(dest)!=entry['sha256']: raise RuntimeError('Remote seed checksum mismatch')
                Path(str(dest)+'.sha256').write_text(entry['sha256'])
        fixture_source=root/'fixture-source'
        if any(s.get('case',{}).get('fixture') for s in config['setups']):
            command(['python3',str(tools/'prepare-fixture-build.py'),str(source),str(fixture_source)],'fixture-prepare.log')
            command(['make','-C',str(fixture_source/'src'),'-j6','MALLOC=jemalloc'],'fixture-build.log')
            shutil.copyfile(fixture_source/'fixture-build.json',evidence/'fixture-build.json')
        for index, setup in enumerate(config['setups']):
            if time.time()+setup['timeout_seconds']+300>config['deadline_epoch']:
                state['cases'].append({'case_id':setup['scenario'],'mode':setup['mode'],'status':'not_run_deadline'})
                continue
            case_root=root/f'case-{index}'; case_root.mkdir()
            setup['redis_binary']=str(source/'src/redis-server')
            if setup.get('case',{}).get('fixture'):
                setup['fixture']=True
                setup['redis_binary']=str(fixture_source/'src/redis-server')
            setup['data_dir']='/mnt/redis-benchmark'
            if config.get('quick'):
                setup['seed_cache']=str(cache/(setup['seed_key']+'.rdb'))
            setup_path=case_root/'setup.json'; setup_path.write_text(json.dumps(setup,indent=2))
            state['status']='running'; state['active_case']=setup['scenario']; state['active_mode']=setup['mode']; state['active_index']=index; status()
            worker='spec-worker.py' if setup['scenario']!='custom' else 'scenario-worker.py'
            if setup.get('fixture'): worker='fixture-worker.py'
            with (case_root/'runner.log').open('wb') as log:
                run=subprocess.run(['timeout','--signal=TERM','--kill-after=30s',str(setup['timeout_seconds']),
                                    'python3',str(tools/worker),str(setup_path)],cwd=case_root,stdout=log,stderr=log)
            target=evidence/f'case-{index}'; target.mkdir()
            for p in case_root.rglob('*'):
                if p.is_file() and not p.is_symlink() and p.suffix not in ('.rdb','.so'):
                    dest=target/p.relative_to(case_root); dest.parent.mkdir(parents=True,exist_ok=True); dest.write_bytes(p.read_bytes())
            actual=case_root/'output'
            if actual.exists():
                for p in actual.rglob('*'):
                    if p.is_file() and not p.is_symlink() and p.suffix not in ('.rdb','.so'):
                        dest=target/'output'/p.relative_to(actual); dest.parent.mkdir(parents=True,exist_ok=True); dest.write_bytes(p.read_bytes())
            state['cases'].append({'case_id':setup['scenario'],'mode':setup['mode'],'exit_code':run.returncode,
                                   'status':'completed' if run.returncode==0 else 'failed'})
            if config.get('quick') and run.returncode==0 and setup['seed_key'] not in published:
                key=setup['seed_key']; entry=config['seeds'][key]; seed=Path(setup['seed_cache'])
                put(entry['put_url'],seed)
                seed_manifest[key]={k:entry[k] for k in ('identity','bucket','key')}
                seed_manifest[key]['sha256']=sha(seed)
                (evidence/'seed-manifest.json').write_text(json.dumps(seed_manifest,indent=2))
                published.add(key)
            bundle(index); status()
            # RDBs are temporary and excluded from evidence by contract. Remove
            # only this completed case's files after successful evidence upload.
            # Leave logs/results and failed attempts intact; never follow links.
            data_root=Path(setup['data_dir'])/setup['run_id']
            if data_root.parent==Path('/mnt/redis-benchmark') and not data_root.is_symlink():
                for dump in data_root.rglob('*.rdb'):
                    if dump.is_file() and not dump.is_symlink() and data_root.resolve() in dump.resolve().parents:
                        dump.unlink()
        state['status']='completed' if all(x['status']=='completed' for x in state['cases']) else 'incomplete'
    except BaseException as exc:
        state['status']='failed'; state['error_type']=type(exc).__name__
    finally:
        with (evidence/'kernel.log').open('wb') as f:
            subprocess.run(['journalctl','-k','--no-pager'],stdout=f,stderr=subprocess.STDOUT)
        try:
            (evidence/'status.json').write_text(json.dumps(state,indent=2))
            bundle(len(config['setups']))
            status()
        finally:
            subprocess.run(['shutdown','-h','now'],check=False)


if __name__=='__main__': execute(json.loads(Path(sys.argv[1]).read_text()))
