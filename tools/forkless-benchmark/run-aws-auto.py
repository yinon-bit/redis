#!/usr/bin/env python3
"""Create one disposable EC2 host and private persistent S3 results bucket."""
import argparse
import base64
from datetime import datetime, timezone
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import re
import subprocess
import tarfile
import time
import uuid
import urllib.request

from scenario_catalog import performance_cases, runnable_cases

ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).parent


def arguments(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--name',required=True)
    pick=p.add_mutually_exclusive_group(required=True)
    pick.add_argument('--scenario',action='append',help='Repeat for multiple documented case IDs')
    pick.add_argument('--random-cases',type=int,help='Reproducible random performance-case selection')
    pick.add_argument('--config',help='Custom-variable JSON, as used by run-aws-scenario.py')
    p.add_argument('--selection-seed',type=int,default=20260923)
    p.add_argument('--hours',type=float,default=2)
    p.add_argument('--case-timeout',type=int,default=1800)
    p.add_argument('--region',default='us-east-2')
    p.add_argument('--mode',choices=['fork','hybrid','forkless'])
    p.add_argument('--then-mode',choices=['fork','hybrid','forkless'],help='Repeat named cases in this mode on the same instance, after the first mode')
    p.add_argument('--owner',default='yinon_yishay')
    p.add_argument('--team',default='redis_core')
    p.add_argument('--subnet-id',help='Optional public subnet; otherwise discover default-VPC public subnet')
    p.add_argument('--ami-id',help='Optional exact Ubuntu24.04 ARM64 AMI; otherwise resolve and record latest Canonical image')
    p.add_argument('--credentials-expire-at',help='Optional ISO UTC expiration; reject a deadline beyond it')
    p.add_argument('--launch',action='store_true')
    p.add_argument('--quick',action='store_true',help='One repetition, shared seeds, non-cumulative case archives')
    p.add_argument('--arena-tcache',action='store_true',help='Enable the experimental forkless arena local cache at startup')
    p.add_argument('--forkless-timing',action='store_true',help='Enable opt-in helper and child timing telemetry')
    p.add_argument('--seed-manifest',help='Previously downloaded seed-manifest.json; quick mode only')
    a=p.parse_args(argv)
    if a.seed_manifest and not a.quick: p.error('--seed-manifest requires --quick')
    if a.quick and a.config: p.error('--quick currently requires named performance cases')
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,39}',a.name): p.error('Name: lowercase letters, digits, underscore or hyphen; max40')
    for value in (a.owner,a.team):
        if not re.fullmatch(r'[a-z0-9]+(?:_[a-z0-9]+)*',value) or len(value)>63: p.error('Invalid owner/team tag')
    if not .5<=a.hours<=6: p.error('hours must be 0.5–6')
    if not 60<=a.case_timeout<=a.hours*3600-600: p.error('Case timeout must fit inside runtime with a 10-minute reserve')
    catalog=runnable_cases()
    if a.then_mode and (a.config or not a.mode or a.then_mode==a.mode):
        p.error('--then-mode requires named cases and a different explicit --mode')
    if a.random_cases is not None:
        if not 1<=a.random_cases<=len(catalog): p.error('Invalid random case count')
        a.scenario=random.Random(a.selection_seed).sample(list(performance_cases()),a.random_cases)
    if a.scenario:
        if a.quick and any(catalog.get(x,{}).get('fixture') for x in a.scenario): p.error('--quick supports performance cases only')
        if len(a.scenario)!=len(set(a.scenario)): p.error('Duplicate scenario')
        if any(x not in catalog for x in a.scenario): p.error('Unknown/unimplemented scenario')
        if 'forkless' in (a.mode,a.then_mode) and any(catalog[x].get('fixture') for x in a.scenario): p.error('Fixture adapter supports fork/hybrid only')
    if a.credentials_expire_at:
        try:
            expiry_time=datetime.fromisoformat(a.credentials_expire_at.replace('Z','+00:00'))
            if expiry_time.tzinfo is None: p.error('Credential expiration must include Z or a UTC offset')
            expiry=expiry_time.timestamp()
            if expiry<time.time()+a.hours*3600+300: p.error('Credentials expire before runtime plus upload reserve')
        except ValueError: p.error('Invalid credential expiration')
    return a


def setups(a, run_id):
    if a.config:
        spec=importlib.util.spec_from_file_location('launcher',HERE/'run-aws-scenario.py')
        launcher=importlib.util.module_from_spec(spec); spec.loader.exec_module(launcher)
        c=launcher.configuration(['--config',a.config,'--scenario','custom','--name',a.name,
                                  '--instance-id','i-0000','--results-s3','s3://placeholder/auto',
                                  '--redis-binary','/opt/redis-auto/source/src/redis-server'])
        c.update(run_id=run_id+'-c0',timeout_seconds=a.case_timeout,launch=False)
        if a.mode: c['mode']=a.mode
        return [c]
    sequence=[(mode,case) for mode in ([a.mode or 'hybrid']+([a.then_mode] if a.then_mode else [])) for case in a.scenario]
    return [dict(schema='document-cases/1',run_id=run_id+f'-c{i}',name=a.name,scenario=case,
                 case=dict(runnable_cases()[case], **({'repetitions':1} if a.quick else {}), arena_tcache=a.arena_tcache, forkless_timing=a.forkless_timing),mode=mode,port=16379,timeout_seconds=a.case_timeout,quick=a.quick)
            for i,(mode,case) in enumerate(sequence)]


def source_archive(target):
    tracked=subprocess.check_output(['git','ls-files','-z'],cwd=ROOT).decode().split('\0')
    prefixes=('src/','deps/','tests/','utils/','modules/vector-sets/')
    names={n for n in tracked if n and (n.startswith(prefixes) or n in ('redis.conf','runtest','runtest-moduleapi'))}
    names.update('tools/forkless-benchmark/'+name for name in
                 ('spec-worker.py','scenario-worker.py','scenario-eio.c','fixture-worker.py','fixture-gate.h','prepare-fixture-build.py','seed-cache.py'))
    # Include new implementation source files but never arbitrary untracked secrets/results.
    names.update(str(p.relative_to(ROOT)) for p in (ROOT/'src').glob('*') if p.suffix in ('.c','.h'))
    with tarfile.open(target,'w:gz') as archive:
        for name in sorted(names):
            path=ROOT/name
            if path.is_symlink(): raise RuntimeError('Source archive refuses symlinks: '+name)
            if path.is_file(): archive.add(path,arcname=name,recursive=False)
    return hashlib.sha256(target.read_bytes()).hexdigest()


def bootstrap(config, config_url=None, config_sha256=None):
    deadline=datetime.fromtimestamp(config['deadline_epoch'],timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    encoded_config=base64.b64encode(json.dumps(config).encode()).decode()
    encoded_runner=base64.b64encode((HERE/'aws-auto-remote.py').read_bytes()).decode()
    config_loader = f"payload=base64.b64decode('{encoded_config}')"
    if config_url is not None:
        if not config_sha256 or not re.fullmatch('[0-9a-f]{64}', config_sha256):
            raise ValueError('Remote configuration requires SHA256')
        config_loader = f'''import urllib.request, hashlib
with urllib.request.urlopen({config_url!r}, timeout=60) as response:
    payload=response.read()
if hashlib.sha256(payload).hexdigest() != {config_sha256!r}:
    raise RuntimeError('Configuration checksum mismatch')'''
    script=f'''#!/bin/bash
set -eu
umask 077
trap 'shutdown -h now' EXIT
systemd-run --unit=redis-auto-deadline --on-calendar='{deadline}' --timer-property=Persistent=true /sbin/shutdown -h now
python3 - <<'PY'
import base64
from pathlib import Path
{config_loader}
Path('/root/redis-auto-config.json').write_bytes(payload)
Path('/root/redis-auto-remote.py').write_bytes(base64.b64decode('{encoded_runner}'))
PY
python3 /root/redis-auto-remote.py /root/redis-auto-config.json
'''
    data=gzip.compress(script.encode())
    if len(data)>16384: raise RuntimeError('Selection too large for EC2 user data; split into smaller sessions')
    return data


def verify_archive(path):
    with tarfile.open(path) as t:
        members=t.getmembers()
        if len({m.name for m in members})!=len(members): raise RuntimeError('Duplicate archive member')
        if any(m.issym() or m.islnk() or Path(m.name).is_absolute() or '..' in Path(m.name).parts for m in members):
            raise RuntimeError('Unsafe result archive')
        listed=set()
        manifest=t.extractfile('evidence/SHA256SUMS').read().decode()
        for line in manifest.splitlines():
            expected,name=line.split('  ',1)
            if not re.fullmatch(r'[0-9a-f]{64}',expected) or Path(name).is_absolute() or '..' in Path(name).parts:
                raise RuntimeError('Invalid checksum manifest')
            entry='evidence/'+name
            if entry in listed: raise RuntimeError('Duplicate checksum entry')
            listed.add(entry)
            h=hashlib.sha256()
            with t.extractfile(entry) as f:
                for block in iter(lambda:f.read(1024*1024),b''): h.update(block)
            if h.hexdigest()!=expected: raise RuntimeError('Artifact checksum mismatch')
        files={m.name for m in members if m.isfile() and m.name!='evidence/SHA256SUMS'}
        if files!=listed: raise RuntimeError('Incomplete artifact checksum coverage')


def main():
    a=arguments()
    run_id=a.name+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6]
    selected=setups(a,run_id)
    plan=dict(run_id=run_id,instance_type='r7g.2xlarge',region=a.region,hours=a.hours,
              owner=a.owner,team=a.team,cases=[c.get('scenario') for c in selected],
              case_modes=[c['mode'] for c in selected],
              quick=a.quick, archive_layout='per-case' if a.quick else 'cumulative',
              result_bucket='new private bucket, retained',inbound_ports=[],
              note='One instance. Presigned uploads require unexpired credentials. Keep launcher running for watchdog/collection/cleanup.')
    print(json.dumps(plan,indent=2))
    if not a.launch: return
    if os.environ.get('AWS_SESSION_TOKEN') and not a.credentials_expire_at:
        print('Warning: credential expiration is unknown. Uploads/cleanup need credentials valid for the whole run.')
    try:
        import boto3
        from botocore.config import Config
    except ImportError: raise SystemExit('Install boto3 in your Python environment first: python3 -m pip install boto3')
    session=boto3.Session(region_name=a.region)
    ec2=session.client('ec2')
    # Fresh buckets can redirect global-endpoint PUTs. A signed request cannot
    # simply change hosts, so use the region's endpoint before signing.
    s3=session.client('s3',endpoint_url=f'https://s3.{a.region}.amazonaws.com',
                      config=Config(signature_version='s3v4',s3={'addressing_style':'virtual'}))
    sts=session.client('sts')
    account=sts.get_caller_identity()['Account']
    bucket='redis-bench-'+account+'-'+uuid.uuid4().hex[:12]
    out=HERE/'results'/run_id; out.mkdir(parents=True,exist_ok=False)
    os.chmod(out,0o700)
    state=dict(plan,bucket=bucket,instance_id=None,security_group=None,volumes=[],status='provisioning',
               boto3_version=boto3.__version__,revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip())
    def record(): (out/'resources.json').write_text(json.dumps(state,indent=2))
    record()
    instance=None; group=None; launch_attempted=False
    try:
        if a.subnet_id:
            subnet=ec2.describe_subnets(SubnetIds=[a.subnet_id])['Subnets'][0]
        else:
            subnets=ec2.describe_subnets(Filters=[{'Name':'default-for-az','Values':['true']},{'Name':'state','Values':['available']}])['Subnets']
            candidates=sorted([s for s in subnets if s.get('MapPublicIpOnLaunch')],key=lambda s:s['SubnetId'])
            if not candidates: raise RuntimeError('No default public subnet; supply --subnet-id')
            subnet=candidates[0]
        if a.ami_id:
            image=ec2.describe_images(ImageIds=[a.ami_id])['Images'][0]
        else:
            images=ec2.describe_images(Owners=['099720109477'],Filters=[
                {'Name':'name','Values':['ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-arm64-server-*']},
                {'Name':'state','Values':['available']}])['Images']
            image=max(images,key=lambda i:i['CreationDate'])
        if image['Architecture']!='arm64': raise RuntimeError('AMI must be ARM64')
        create={'Bucket':bucket}
        if a.region!='us-east-1': create['CreateBucketConfiguration']={'LocationConstraint':a.region}
        s3.create_bucket(**create)
        s3.put_public_access_block(Bucket=bucket,PublicAccessBlockConfiguration={k:True for k in
            ('BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets')})
        s3.put_bucket_ownership_controls(Bucket=bucket,OwnershipControls={'Rules':[{'ObjectOwnership':'BucketOwnerEnforced'}]})
        s3.put_bucket_encryption(Bucket=bucket,ServerSideEncryptionConfiguration={'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]})
        tags=[{'Key':'owner','Value':a.owner},{'Key':'team','Value':a.team},{'Key':'purpose','Value':'redis_benchmark'},
              {'Key':'benchmark_run','Value':run_id.replace('-','_')}]
        s3.put_bucket_tagging(Bucket=bucket,Tagging={'TagSet':tags})
        source=out/'source.tar.gz'; source_hash=source_archive(source)
        s3.upload_file(str(source),bucket,'source.tar.gz')
        deadline=int(time.time()+a.hours*3600)
        state.update(ami=image['ImageId'],subnet=subnet['SubnetId'],source_sha256=source_hash,deadline_epoch=deadline)
        ttl=int(a.hours*3600+300)
        def url(key,method):
            return s3.generate_presigned_url(method,Params={'Bucket':bucket,'Key':key},ExpiresIn=ttl)
        config=dict(deadline_epoch=deadline,source_sha256=source_hash,source_url=url('source.tar.gz','get_object'),
                    setups=selected,status_url=url('status.json','put_object'),
                    artifact_urls=[url(f'artifacts-{i}.tar.gz','put_object') for i in range(len(selected)+1)],
                    receipt_urls=[url(f'receipt-{i}.json','put_object') for i in range(len(selected)+1)])
        if a.quick:
            seed_spec=importlib.util.spec_from_file_location('seed_cache',HERE/'seed-cache.py')
            seed_module=importlib.util.module_from_spec(seed_spec); seed_spec.loader.exec_module(seed_module)
            prior=json.loads(Path(a.seed_manifest).read_text()) if a.seed_manifest else {}
            config['quick']=True; config['seeds']={}
            for setup in selected:
                seed_key=seed_module.key(setup['case']); setup['seed_key']=seed_key
                entry={'identity':seed_module.identity(setup['case']), 'bucket':bucket,'key':f'seeds/{seed_key}.rdb',
                       'put_url':url(f'seeds/{seed_key}.rdb','put_object')}
                if seed_key in prior:
                    previous=prior[seed_key]
                    if previous['identity']!=entry['identity'] or not re.fullmatch('[0-9a-f]{64}',previous['sha256']):
                        raise RuntimeError('Seed manifest identity/checksum mismatch')
                    entry.update(sha256=previous['sha256'],get_url=s3.generate_presigned_url('get_object',Params={'Bucket':previous['bucket'],'Key':previous['key']},ExpiresIn=ttl))
                config['seeds'][seed_key]=entry
        # Exercise the same unauthenticated HTTP path the instance uses before
        # spending on compute. Never include signed URLs in exception output.
        try:
            probe=b'{"preflight":true}'
            request=urllib.request.Request(url('transport-preflight.json','put_object'),data=probe,method='PUT')
            with urllib.request.urlopen(request,timeout=30) as response:
                if response.status!=200: raise RuntimeError('Unexpected upload status')
            with urllib.request.urlopen(url('transport-preflight.json','get_object'),timeout=30) as response:
                if response.read()!=probe: raise RuntimeError('Download mismatch')
        except Exception as exc:
            state['transport_preflight']='failed'; record()
            raise RuntimeError('Signed S3 transport preflight failed: '+type(exc).__name__) from None
        state['transport_preflight']='passed'; record()
        # Keep signed configuration private; pass only one short-lived URL to EC2.
        config_payload=json.dumps(config).encode()
        s3.put_object(Bucket=bucket,Key='private/bootstrap-config.json',Body=config_payload,
                      ServerSideEncryption='AES256',ContentType='application/json')
        user_data=bootstrap(config,url('private/bootstrap-config.json','get_object'),
                            hashlib.sha256(config_payload).hexdigest())
        group=ec2.create_security_group(GroupName=run_id,Description='Private Redis benchmark: no inbound rules',VpcId=subnet['VpcId'])['GroupId']
        state['security_group']=group; record()
        ec2.create_tags(Resources=[group],Tags=tags)
        launch_attempted=True
        launch=ec2.run_instances(ImageId=image['ImageId'],InstanceType='r7g.2xlarge',MinCount=1,MaxCount=1,
            ClientToken=run_id,InstanceInitiatedShutdownBehavior='terminate',UserData=user_data,
            MetadataOptions={'HttpTokens':'required','HttpEndpoint':'enabled'},
            NetworkInterfaces=[{'DeviceIndex':0,'SubnetId':subnet['SubnetId'],'Groups':[group],'AssociatePublicIpAddress':True,'DeleteOnTermination':True}],
            BlockDeviceMappings=[{'DeviceName':image['RootDeviceName'],'Ebs':{'VolumeSize':32,'VolumeType':'gp3','Encrypted':True,'DeleteOnTermination':True}},
                                 {'DeviceName':'/dev/sdf','Ebs':{'VolumeSize':100,'VolumeType':'gp3','Iops':3000,'Throughput':125,'Encrypted':True,'DeleteOnTermination':True}}],
            TagSpecifications=[{'ResourceType':r,'Tags':tags} for r in ('instance','volume')])
        instance=launch['Instances'][0]['InstanceId']; state['instance_id']=instance; state['status']='running'; record()
        s3.put_object(Bucket=bucket,Key='manifest.json',Body=json.dumps(state).encode(),ContentType='application/json')
        print(f'Running {instance}; persistent results: s3://{bucket}/')
        observed=set(); timer_verified=False; started=time.monotonic()
        while time.time()<deadline:
            info=ec2.describe_instances(InstanceIds=[instance])['Reservations'][0]['Instances'][0]
            state['volumes']=sorted(set(state['volumes']) | {x['Ebs']['VolumeId'] for x in info.get('BlockDeviceMappings',[]) if 'Ebs' in x}); record()
            try:
                status=json.loads(s3.get_object(Bucket=bucket,Key='status.json')['Body'].read())
                timer_verified=True
                state['remote_status']=status
                (out/'status.json').write_text(json.dumps(status,indent=2))
            except s3.exceptions.NoSuchKey: pass
            for i in range(len(selected)+1):
                if i in observed: continue
                try: receipt=json.loads(s3.get_object(Bucket=bucket,Key=f'receipt-{i}.json')['Body'].read())
                except s3.exceptions.NoSuchKey: continue
                target=out/f'artifacts-{i}.tar.gz'
                s3.download_file(bucket,target.name,str(target))
                if hashlib.sha256(target.read_bytes()).hexdigest()!=receipt['sha256']: raise RuntimeError('Downloaded archive checksum mismatch')
                verify_archive(target)
                if a.quick:
                    with tarfile.open(target) as archive:
                        try: seed_member=archive.getmember('evidence/seed-manifest.json')
                        except KeyError: seed_member=None
                        if seed_member:
                            if seed_member.size>1024*1024: raise RuntimeError('Oversized seed manifest')
                            (out/'seed-manifest.json').write_bytes(archive.extractfile(seed_member).read())
                (out/f'receipt-{i}.json').write_text(json.dumps(receipt,indent=2))
                observed.add(i)
                print(f'Collected and verified archive {i}.')
            if info['State']['Name']=='terminated': break
            if not timer_verified and time.monotonic()-started>600: raise RuntimeError('Boot/deadline confirmation missing after 10 minutes')
            time.sleep(20)
        state['verified_archives']=sorted(observed)
        state['status']='collected' if len(selected) in observed else 'incomplete'
    finally:
        if launch_attempted and not instance:
            try:
                found=ec2.describe_instances(Filters=[{'Name':'client-token','Values':[run_id]}])['Reservations']
                recovered=[i for reservation in found for i in reservation['Instances']]
                if len(recovered)==1:
                    instance=recovered[0]['InstanceId']; state['instance_id']=instance
                    state['volumes']=[b['Ebs']['VolumeId'] for b in recovered[0].get('BlockDeviceMappings',[]) if 'Ebs' in b]
            except Exception: state['cleanup_error']='Ambiguous launch; inspect EC2 using client-token '+run_id
        if instance:
            try:
                ec2.terminate_instances(InstanceIds=[instance])
                ec2.get_waiter('instance_terminated').wait(InstanceIds=[instance],WaiterConfig={'Delay':10,'MaxAttempts':36})
                state['instance_terminated']=True
                state['volumes_deleted']=[]
                for volume in state['volumes']:
                    try: ec2.describe_volumes(VolumeIds=[volume])
                    except Exception as exc:
                        if getattr(exc,'response',{}).get('Error',{}).get('Code')=='InvalidVolume.NotFound':
                            state['volumes_deleted'].append(volume)
                        else: raise
                state['volume_cleanup_pending']=sorted(set(state['volumes'])-set(state['volumes_deleted']))
            except Exception: state['cleanup_error']='Instance/volume cleanup not fully verified; inspect exact resources in this file'
        if group and (not instance or state.get('instance_terminated')):
            try: ec2.delete_security_group(GroupId=group); state['security_group_deleted']=True
            except Exception: state['security_group_cleanup_pending']=True
        record()
        try: s3.put_object(Bucket=bucket,Key='resources-final.json',Body=json.dumps(state).encode(),ContentType='application/json')
        except Exception: print('Final cleanup record is local only; cloud update unavailable.')
        print(f'Resource record: {out}/resources.json. Result bucket retained: {bucket}')
    if state.get('status')!='collected' or state.get('remote_status',{}).get('status')!='completed':
        raise SystemExit(2)


if __name__=='__main__':
    try: main()
    except Exception as exc:
        # Avoid accidentally displaying signed URL/credential material in tracebacks.
        print('Automatic run failed: '+type(exc).__name__+'. Inspect the saved resource record.',file=__import__('sys').stderr)
        raise SystemExit(1)
