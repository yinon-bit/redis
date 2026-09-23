#!/usr/bin/env python3
"""Small correctness smoke tests, not AWS sizing or performance evidence."""
import asyncio
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

s=importlib.util.spec_from_file_location('fixtures',Path(__file__).with_name('fixture-worker.py'))
f=importlib.util.module_from_spec(s); s.loader.exec_module(f)

class Resources:
    def enter(self): pass
    def sample(self): return {'scope':'smoke; no resource cap'}

class Tests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('FIXTURE_TEST_REDIS'),'Requires isolated instrumented jemalloc Linux build')
    def test_fixtures(self):
        payload=f.w.values()
        cases=[dict(scenario='point_in_time',small_keys=100,big_keys=2,hash_keys=5,serializer_position='before_visit')]
        for variant in ('string_grow','string_shrink','string_boundary','string_inplace','string_setrange','hash_encoding','hash_fields','type_replace'):
            for position in ('before_visit','during_capture','after_visit'):
                cases.append(dict(scenario='arena_transition',variant=variant,serializer_position=position,small_keys=100,target_keys=2))
        for workload in ('idle','write100','rw50'):
            cases.append(dict(scenario='cancellation_retry',small_keys=100,big_keys=4,hash_keys=5,
                dataset_id='mixed',workload=workload,clients=0 if workload=='idle' else 2,
                pipeline=1,before_seconds=.1,after_seconds=.1))
        for mode in os.environ.get('FIXTURE_TEST_MODES','fork,hybrid').split(','):
            for case in cases:
                with self.subTest(mode=mode,case=case), tempfile.TemporaryDirectory(prefix='fixture-smoke-') as tmp:
                    c=dict(redis_binary=os.environ['FIXTURE_TEST_REDIS'],mode=mode,port=26387,fixture=True,case=case)
                    result=asyncio.run(f.attempt(c,Path(tmp),Resources(),payload))
                    self.assertEqual(result['status'],'completed',result)

if __name__=='__main__': unittest.main()
