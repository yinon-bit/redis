#!/usr/bin/env python3
"""Tiny driver correctness tests; never a performance run or full suite claim."""
import asyncio
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

s=importlib.util.spec_from_file_location('specworker',Path(__file__).with_name('spec-worker.py'))
w=importlib.util.module_from_spec(s); s.loader.exec_module(w)


class FakeResources:
    # Only for small lifecycle smoke tests. Production entrypoint always enforces cgroups.
    def enter(self): pass
    def sample(self): return {'scope':'test-only; no resource limit'}


class Tests(unittest.TestCase):
    def test_random(self):
        self.assertEqual(w.next_random(192837),207998108042727)

    def test_keys(self):
        self.assertEqual(w.key('small',2),'small:000000002')
        self.assertEqual(w.key('big',2),'big:000002')
        self.assertEqual(w.key('hash',2),'hash:0000002')

    @unittest.skipUnless(os.environ.get('SCENARIO_TEST_REDIS'),'Requires test Redis')
    def test_lifecycles(self):
        for lifecycle,workload in [('first','read100'),('subsequent','rw50'),('after_failure','idle'),('after_failure','read100')]:
            if lifecycle=='after_failure' and sys.platform!='linux': continue
            with self.subTest(lifecycle=lifecycle), tempfile.TemporaryDirectory(prefix='spec-worker-smoke-') as tmp:
                c={'redis_binary':os.environ['SCENARIO_TEST_REDIS'],'mode':os.environ.get('SCENARIO_TEST_MODE','fork'),'port':26389,
                   'case':{'small_keys':1000,'big_keys':4,'hash_keys':50,'dataset_id':'mixed',
                           'lifecycle':lifecycle,'workload':workload,'clients':0 if workload=='idle' else 2,
                           'pipeline':16,'before_seconds':.1,'after_seconds':.1,'repetitions':1}}
                result={}
                asyncio.run(w.execute(c,Path(tmp),FakeResources(),result))
                self.assertEqual(result['status'],'completed',result)
                if lifecycle=='after_failure':
                    self.assertTrue(result['attempts'][0]['previous_rdb_preserved'])
                self.assertTrue(result['attempts'][0]['validation']['ok'])


if __name__=='__main__': unittest.main()
