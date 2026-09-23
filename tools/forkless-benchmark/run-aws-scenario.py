#!/usr/bin/env python3
"""Plan or dispatch a named benchmark on an EXISTING SSM-managed EC2 host."""
import argparse
import hashlib
import json
import re
import shlex
import subprocess
import uuid
import sys
from datetime import datetime, timezone
from pathlib import Path
from scenario_catalog import listing, runnable_cases, FIXTURE_FAMILIES


CONFIG_FIELDS = {
    'name': str, 'instance_id': str, 'results_s3': str, 'region': str,
    'redis_binary': str, 'mode': str, 'scenario': str, 'dataset_gib': (int, float),
    'big_key_percent': (int, float), 'big_request_percent': (int, float),
    'small_bytes': int, 'big_bytes': int, 'writes_per_sec': int, 'reads_per_sec': int,
    'clients': int, 'before_seconds': int, 'after_seconds': int,
    'timeout_seconds': int, 'port': int, 'seed': int,
    'data_dir': str,
}


def read_config(path, parser):
    try:
        config = json.loads(Path(path).read_text())
    except (OSError, ValueError): parser.error('Cannot read config as JSON')
    if not isinstance(config, dict): parser.error('Config must be a JSON object')
    # Do not echo unexpected fields: someone might paste credential material.
    if set(config)-set(CONFIG_FIELDS): parser.error('Config contains unsupported fields; use only documented settings, never credentials')
    for key, value in config.items():
        if isinstance(value, bool) or not isinstance(value, CONFIG_FIELDS[key]):
            parser.error('Config setting has an invalid type: '+key)
    return config


def configuration(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', help='JSON settings; explicit CLI flags override them')
    p.add_argument('--scenario', default='custom', help='custom or exact shared-spec case ID')
    p.add_argument('--data-dir', help='Existing remote dedicated data-volume directory (required for documented cases)')
    p.add_argument('--list-scenarios', action='store_true', help='List case IDs and execution support, without AWS access')
    p.add_argument('--name')
    p.add_argument('--instance-id')
    p.add_argument('--results-s3', help='s3://bucket/prefix')
    p.add_argument('--region', default='us-east-2')
    p.add_argument('--redis-binary', help='Absolute remote redis-server path')
    p.add_argument('--mode', choices=['fork', 'hybrid', 'forkless'], default='hybrid')
    p.add_argument('--dataset-gib', type=float, default=1, help='Value payload budget, NOT RSS')
    p.add_argument('--big-key-percent', type=float, default=0.02, help='Percent of key COUNT')
    p.add_argument('--big-request-percent', type=float, default=0.02)
    p.add_argument('--small-bytes', type=int, default=256)
    p.add_argument('--big-bytes', type=int, default=1048576)
    p.add_argument('--writes-per-sec', type=int, default=50000, help='Global offered rate; zero disables SET')
    p.add_argument('--reads-per-sec', type=int, default=0, help='Global offered rate; zero disables GET')
    p.add_argument('--clients', type=int, default=32, help='Connections per enabled operation')
    p.add_argument('--before-seconds', type=int, default=15)
    p.add_argument('--after-seconds', type=int, default=5)
    p.add_argument('--timeout-seconds', type=int, default=1800, help='Whole remote run limit, not EC2 lifetime')
    p.add_argument('--port', type=int, default=16379)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--launch', action='store_true', help='Without this flag, only print the plan')
    preliminary, _ = p.parse_known_args(argv)
    file_config = read_config(preliminary.config, p) if preliminary.config else {}
    p.set_defaults(**file_config)
    a = p.parse_args(argv)
    if a.list_scenarios: return listing()
    if a.scenario != 'custom':
        case = runnable_cases().get(a.scenario)
        if case is None:
            if a.scenario in FIXTURE_FAMILIES or a.scenario in ('all','core','large','arena','extended','profile'):
                p.error('This is a family/batch, not a concrete runnable case. See --list-scenarios for support status.')
            p.error('Unknown scenario; use --list-scenarios for exact case IDs')
        if case.get('fixture') and a.launch:
            p.error('Fixtures require the isolated instrumented build; use --provision for automatic preparation')
        variable_fields = set(CONFIG_FIELDS)-{'name','instance_id','results_s3','region','redis_binary','mode','scenario','timeout_seconds','port','data_dir'}
        flags = {x.split('=',1)[0].removeprefix('--').replace('-','_') for x in argv if x.startswith('--')}
        if variable_fields.intersection(flags | set(file_config)):
            p.error('Documented cases have fixed settings. Use --scenario custom for variable overrides; no silent case changes.')
        if not a.launch:
            return dict(selection=case, launch=False, execution_supported=True,
                        source='SHARED-POC-BENCHMARK-SPEC.md',
                        note='Executable with recorded deviations; not full v2.0 compliance.')
        if not a.data_dir or not a.data_dir.startswith('/') or '\n' in a.data_dir:
            p.error('Documented cases require --data-dir pointing to a dedicated mounted data volume')
    for field in ('name','instance_id','results_s3','redis_binary'):
        if not getattr(a, field): p.error('Required custom-run setting: --'+field.replace('_','-'))
    if a.mode not in ('fork','hybrid','forkless'): p.error('Invalid mode')
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,60}', a.name): p.error('Use 1–60 letters, digits, underscores or hyphens for name')
    if not re.fullmatch(r'i-[0-9a-f]+', a.instance_id): p.error('Invalid instance ID')
    if not re.fullmatch(r's3://[a-z0-9.-]+/[a-zA-Z0-9_./-]+', a.results_s3): p.error('Provide s3://bucket/nonempty-prefix')
    if not a.redis_binary.startswith('/') or '\n' in a.redis_binary: p.error('Use an absolute binary path')
    if not (0 <= a.big_key_percent <= 100 and 0 <= a.big_request_percent <= 100): p.error('Percentages must be 0–100')
    if not (0 < a.dataset_gib <= 1024): p.error('dataset-gib must be >0 and <=1024')
    if not (0 < a.small_bytes < a.big_bytes <= 64 * 1024**2): p.error('Require 0 < small-bytes < big-bytes <=64MiB')
    if min(a.writes_per_sec, a.reads_per_sec) < 0: p.error('Rates must be nonnegative')
    if not (1 <= a.clients <= 1024 and 1 <= a.port <= 65535): p.error('Invalid clients or port')
    if not (1 <= a.before_seconds <= 600 and 1 <= a.after_seconds <= 600 and 60 <= a.timeout_seconds <= 21600): p.error('Invalid durations')
    if a.scenario != 'custom':
        if a.port == 65535: p.error('Documented validation also uses port+1')
        c=vars(a).copy()
        c.update(schema='document-cases/1',case=case,
                 run_id=a.name+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8])
        return c
    f = a.big_key_percent / 100
    count = int(a.dataset_gib * 1024**3 / (a.small_bytes * (1-f) + a.big_bytes * f))
    big = round(count * f)
    small = count - big
    if not count or (a.big_request_percent > 0 and not big) or (a.big_request_percent < 100 and not small):
        p.error('Dataset must contain each key class targeted by requests')
    c = vars(a).copy()
    c.update(schema='configurable-strings/1', small_keys=small, big_keys=big,
             actual_payload_bytes=small*a.small_bytes + big*a.big_bytes,
             actual_big_key_percent=100*big/count,
             run_id=a.name+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8])
    return c


def aws(c, *args):
    # AWS CLI inherits environment credentials/profile. Never print its environment.
    return subprocess.check_output(['aws', '--region', c['region'], *args], text=True)


def main():
    c = configuration()
    print(json.dumps(c, indent=2))
    if not c.get('launch'): return
    # Read-only preflight before uploading or dispatching anything.
    host = json.loads(aws(c, 'ec2', 'describe-instances', '--instance-ids', c['instance_id']))['Reservations'][0]['Instances'][0]
    tags = {t['Key']: t['Value'] for t in host.get('Tags', [])}
    if host['State']['Name'] != 'running' or not all(tags.get(k) for k in ('owner', 'team')):
        raise SystemExit('Instance must be running and have owner/team tags')
    if not host.get('IamInstanceProfile'): raise SystemExit('Instance requires an IAM role for SSM and scoped S3 access')
    managed = json.loads(aws(c, 'ssm', 'describe-instance-information', '--filters', json.dumps([{'Key':'InstanceIds','Values':[c['instance_id']]}])))
    if not any(x.get('PingStatus') == 'Online' for x in managed['InstanceInformationList']): raise SystemExit('SSM agent is not online')
    c['instance'] = {k: host.get(k) for k in ('InstanceId','InstanceType','ImageId','Architecture','Placement','CpuOptions')}
    out = Path(__file__).parent / 'results' / c['run_id']
    out.mkdir(parents=True, exist_ok=False)
    worker = Path(__file__).with_name('spec-worker.py' if c['scenario']!='custom' else 'scenario-worker.py')
    c['worker_sha256'] = hashlib.sha256(worker.read_bytes()).hexdigest()
    sources=[worker]
    if c['scenario']!='custom': sources += [worker.with_name('scenario-worker.py'),worker.with_name('scenario-eio.c')]
    c['source_sha256']={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in sources}
    (out/'setup.json').write_text(json.dumps(c, indent=2)+'\n')
    dest = c['results_s3'].rstrip('/')+'/'+c['run_id']
    aws(c, 's3', 'cp', str(out/'setup.json'), dest+'/input/setup.json', '--only-show-errors')
    for source in sources:
        aws(c, 's3', 'cp', str(source), dest+'/input/'+source.name, '--only-show-errors')
    remote = '/var/tmp/redis-scenario-'+c['run_id']
    q = shlex.quote
    # Only non-secret configuration travels via SSM. No inbound SSH/Redis required.
    script = '\n'.join([
        'set -eu', 'umask 077', f'mkdir {q(remote)}', f'cd {q(remote)}',
        'run_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)',
        f'aws s3 cp {q(dest+"/input/")} . --recursive --only-show-errors',
        *[f"printf '%s  {name}\\n' {q(sha)} | sha256sum -c -" for name,sha in c['source_sha256'].items()],
        'set +e', f'timeout --signal=TERM --kill-after=10s {c["timeout_seconds"]}s python3 {q(worker.name)} setup.json > runner.log 2>&1',
        'rc=$?', 'printf "%s\\n" "$rc" > exit-code.txt',
        'journalctl -k --since "$run_started" --no-pager > kernel.log 2> kernel-log-error.txt',
        'printf "%s\\n" "$?" > kernel-log-exit-code.txt',
        'find -L . -type f ! -name SHA256SUMS -exec sha256sum {} \\; > SHA256SUMS',
        f'aws s3 cp . {q(dest+"/artifacts/")} --recursive --only-show-errors',
        'upload_rc=$?', '[ "$upload_rc" -eq 0 ] || exit "$upload_rc"', 'exit "$rc"'])
    response = json.loads(aws(c, 'ssm', 'send-command', '--instance-ids', c['instance_id'], '--document-name', 'AWS-RunShellScript', '--parameters', json.dumps({'commands':[script], 'executionTimeout':[str(c['timeout_seconds']+900)]})))
    command = response['Command']['CommandId']
    (out/'dispatch.json').write_text(json.dumps({'command_id':command,'s3':dest,'instance_id':c['instance_id']}, indent=2)+'\n')
    print(f'Dispatched {command}. Results: {dest}/artifacts/\nInstance lifecycle is unchanged; this script does NOT terminate EC2.')


if __name__ == '__main__':
    if '--provision' in sys.argv[1:]:
        # Preserve normal environment/profile authentication, never copy secrets into arguments.
        command=[sys.executable,str(Path(__file__).with_name('run-aws-auto.py'))]
        command.extend(x for x in sys.argv[1:] if x!='--provision')
        raise SystemExit(subprocess.call(command))
    main()
