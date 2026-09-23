#!/usr/bin/env python3
"""Instrument an isolated source copy, never the user's checkout."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil


def replace_once(text, old, new):
    if text.count(old)!=1: raise ValueError('Fixture adapter does not match this source revision')
    return text.replace(old,new,1)


def prepare(source, target):
    source=Path(source).resolve(); target=Path(target).resolve()
    if target==source or source in target.parents or target in source.parents:
        raise ValueError('Fixture build must be a separate sibling directory')
    if target.exists(): raise ValueError('Refusing to overwrite a fixture build')
    # Check all patches before creating files. Fail closed on incompatible POCs.
    rdb=(source/'src/rdb.c').read_text()
    forkless=(source/'src/forkless.c').read_text()
    debug=(source/'src/debug.c').read_text()
    rdb=replace_once(rdb,'#include "server.h"','#include "server.h"\n#include "fixture-gate.h"')
    rdb=replace_once(rdb,'int rdbSaveRio(int req, rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {',
        'int rdbSaveRio(int req, rio *rdb, int *error, int rdbflags, rdbSaveInfo *rsi) {\n    if (server.in_fork_child) benchFixtureGate("before_visit");')
    rdb=replace_once(rdb,'                                  long long idle, int frequency) {',
        '                                  long long idle, int frequency) {\n    if ((server.in_fork_child || !pthread_equal(pthread_self(),server.main_thread_id)) && benchFixtureTarget(key->ptr)) benchFixtureGate("during_capture");')
    rdb=replace_once(rdb,'    if (delay) debugDelay(delay);\n\n    return 1;',
        '    if (delay) debugDelay(delay);\n    if (server.in_fork_child && benchFixtureTarget(key->ptr)) benchFixtureGate("after_visit");\n\n    return 1;')
    forkless=replace_once(forkless,'#include "server.h"','#include "server.h"\n#include "fixture-gate.h"')
    forkless=replace_once(forkless,
        '        sds encoded = forklessEncodeKv(kv, kvobjGetExpire(kv), dbid);\n        forklessSerializerReadDone();',
        '        int bench_target = benchFixtureTarget(kvobjGetKey(kv));\n        sds encoded = forklessEncodeKv(kv, kvobjGetExpire(kv), dbid);\n        forklessSerializerReadDone();\n        if (bench_target) benchFixtureGate("after_visit");')
    debug=replace_once(debug,'    } else if (!strcasecmp(c->argv[1]->ptr,"forkless-cancel") && c->argc == 2) {',
        '    } else if (!strcasecmp(c->argv[1]->ptr,"bench-cancel") && c->argc == 2) {\n        killRDBChild();\n        addReply(c,shared.ok);\n    } else if (!strcasecmp(c->argv[1]->ptr,"forkless-cancel") && c->argc == 2) {')
    # Exclude generated build products; force a fresh test-only binary.
    shutil.copytree(source,target,ignore=shutil.ignore_patterns('.git','*.o','*.a','*.so','*.d','redis-server','redis-cli','redis-benchmark','redis-check-rdb','redis-check-aof','redis-sentinel','.make-*','results','__pycache__'))
    changes={'rdb.c':rdb,'forkless.c':forkless,'debug.c':debug}
    for name,text in changes.items(): (target/'src'/name).write_text(text)
    shutil.copyfile(Path(__file__).with_name('fixture-gate.h'),target/'src/fixture-gate.h')
    manifest={'instrumented':True,'scope':'correctness fixtures only; never performance comparison',
              'source':str(source),'target':str(target),'files':{}}
    for name in changes:
        manifest['files'][name]={'original_sha256':hashlib.sha256((source/'src'/name).read_bytes()).hexdigest(),
                                 'instrumented_sha256':hashlib.sha256((target/'src'/name).read_bytes()).hexdigest()}
    (target/'fixture-build.json').write_text(json.dumps(manifest,indent=2))
    return manifest


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('source'); p.add_argument('target')
    a=p.parse_args(); print(json.dumps(prepare(a.source,a.target),indent=2))
