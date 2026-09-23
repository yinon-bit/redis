#!/usr/bin/env python3
"""Offline evidence check. PASS is scoped to selected cases, not suite compliance."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile

spec=importlib.util.spec_from_file_location('launcher',Path(__file__).with_name('run-aws-auto.py'))
launcher=importlib.util.module_from_spec(spec); spec.loader.exec_module(launcher)


def case_check(setup, result):
    if result.get('status')!='completed': return 'FAIL', ['worker did not complete']
    if result.get('cleanup_error'): return 'FAIL', ['worker cleanup error']
    schema=result.get('schema')
    if schema=='configurable-strings/1':
        ok=result.get('rdb_check_exit')==0 and result.get('save_status')=='ok' and not result.get('task_errors') and bool(result.get('metrics')) and not any(r.get('errors') for r in result.get('metrics',[]))
        return ('PASS', ['structural RDB validation only']) if ok else ('FAIL', ['custom validation failed'])
    if schema not in ('document-cases/1','gated-fixtures/1'):
        return 'MISSING', ['unsupported output schema']
    case=setup['case']; attempts=result.get('attempts',[])
    if result.get('case_id')!=case['case_id']: return 'FAIL', ['result case identity mismatch']
    if schema=='document-cases/1' and result.get('seed_validation',{}).get('ok') is not True:
        return 'FAIL', ['seed validation missing/failed']
    if len(attempts)!=case['repetitions']: return 'MISSING', ['repetition count mismatch']
    for a in attempts:
        if a.get('status')!='completed': return 'FAIL', ['failed repetition']
        if schema=='document-cases/1' or case['scenario']=='cancellation_retry':
            if a.get('validation',{}).get('ok') is not True: return 'FAIL', ['reload validation missing/failed']
        if schema=='document-cases/1':
            if not a.get('measurement',{}).get('metrics'): return 'MISSING', ['metrics missing']
            if case.get('lifecycle')=='after_failure' and a.get('previous_rdb_preserved') is not True:
                return 'FAIL', ['previous RDB preservation missing/failed']
            if case.get('lifecycle')=='subsequent' and a.get('prime_validation',{}).get('ok') is not True:
                return 'FAIL', ['prime validation missing/failed']
        elif case['scenario']=='cancellation_retry':
            if not a.get('retry') or not any(e.get('previous_rdb_preserved') is True for e in a.get('events',[])):
                return 'FAIL', ['cancellation preservation/retry evidence missing']
        else:
            if a.get('snapshot_cut_values_valid') is not True: return 'FAIL', ['snapshot-cut check missing/failed']
            live=a.get('live_state_valid') is True or any(e.get('event')=='live_target_valid' for e in a.get('events',[]))
            if not live: return 'FAIL', ['live-state check missing/failed']
    return 'PASS', result.get('limitations',result.get('deviations',[]))


def check(directory):
    directory=Path(directory)
    report={'schema':'benchmark-check/1','status':'MISSING','integrity':[], 'cases':[],'issues':[]}
    state=json.loads((directory/'resources.json').read_text())
    report['run_id']=state['run_id']; report['source_sha256']=state.get('source_sha256')
    report['revision']=state.get('revision'); report['name']=state.get('run_id')
    expected=state['cases']; documents={}; metrics={}
    archives=sorted(directory.glob('artifacts-*.tar.gz'),key=lambda p:int(p.name.split('-')[-1].split('.')[0]))
    if not (directory/f'artifacts-{len(expected)}.tar.gz').exists():
        report['issues'].append('Final archive not collected; run may still be in progress')
    for archive in archives:
        index=int(archive.name.split('-')[-1].split('.')[0]); receipt=directory/f'receipt-{index}.json'
        try:
            launcher.verify_archive(archive)
            if not receipt.exists():
                report['issues'].append('Missing external receipt: '+receipt.name)
            elif hashlib.sha256(archive.read_bytes()).hexdigest()!=json.loads(receipt.read_text())['sha256']:
                raise ValueError('External archive checksum mismatch')
            report['integrity'].append({'archive':archive.name,'status':'PASS' if receipt.exists() else 'MISSING'})
            with tarfile.open(archive) as t:
                for member in t.getmembers():
                    if member.isfile() and member.name.endswith('.json') and '/case-' in member.name:
                        # Read, never extract or execute archive content.
                        if member.size>64*1024*1024: raise ValueError('Oversized JSON evidence')
                        documents[member.name]=json.load(t.extractfile(member))
                    if member.isfile() and member.name.endswith('/metrics.csv'):
                        metrics[member.name]=True
        except Exception as exc:
            report['integrity'].append({'archive':archive.name,'status':'FAIL','error':type(exc).__name__})
    for i,case_id in enumerate(expected):
        base=f'evidence/case-{i}/'
        setup=documents.get(base+'setup.json'); result=documents.get(base+'output/result.json')
        if not setup or not result: status,notes='MISSING',['setup/result not collected']
        elif setup.get('scenario')!=case_id: status,notes='FAIL',['case identity mismatch']
        else: status,notes=case_check(setup,result)
        report['cases'].append({'case_id':case_id,'status':status,'notes':notes,
            'schema':result.get('schema') if result else None,'setup':setup,
            'metrics_files':[p for p in metrics if p.startswith(base)]})
    cleanup=state.get('instance_terminated') is True and state.get('security_group_deleted') is True
    cleanup=cleanup and bool(state.get('volumes')) and set(state['volumes'])==set(state.get('volumes_deleted',[]))
    report['cleanup']='PASS' if cleanup else 'MISSING'
    if not archives: report['issues'].append('No evidence archives collected')
    statuses=[r['status'] for r in report['cases']+report['integrity']]+[report['cleanup']]
    if 'FAIL' in statuses: report['status']='FAIL'
    elif statuses and all(s=='PASS' for s in statuses) and not report['issues']: report['status']='PASS'
    report['scope']='Selected case evidence and recorded cleanup only; not full suite compliance, a fresh cloud audit, or a performance verdict.'
    report['manual_checks']=['Inspect kernel.log for OOM/I/O errors and telemetry for pressure/throttling.',
        'Compare only identical case settings, workload generator and instrumentation; keep repetitions separate.',
        'Do not merge percentiles or treat gated fixture latency as performance latency.']
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('run_directory')
    a=p.parse_args()
    try: report=check(a.run_directory)
    except Exception as exc: report={'schema':'benchmark-check/1','status':'FAIL','error':type(exc).__name__}
    print(json.dumps(report,indent=2))
    return {'PASS':0,'FAIL':1,'MISSING':2}[report['status']]

if __name__=='__main__': raise SystemExit(main())
