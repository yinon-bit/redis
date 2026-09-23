#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import unittest
import json
import os
import subprocess
import sys
import tempfile


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


launcher = load('launcher', 'run-aws-scenario.py')
worker = load('worker', 'scenario-worker.py')
BASE = ['--name','test','--instance-id','i-1234','--results-s3','s3://test/prefix',
        '--redis-binary','/opt/redis-server']


class Tests(unittest.TestCase):
    def test_catalog_counts(self):
        catalog = launcher.configuration(['--list-scenarios'])
        cases = catalog['performance_cases']
        self.assertEqual(len(cases),288)
        self.assertEqual(len({c['case_id'] for c in cases}),288)
        self.assertEqual(sum(c['batch']=='core' for c in cases),240)
        self.assertTrue(all(c['status']=='executable_with_deviations' for c in cases))

    def test_case_selection(self):
        args = ['--scenario','standard.mixed.rw50.tight.cpu1.first.bgsave']
        selected = launcher.configuration(args)['selection']
        self.assertEqual(selected['small_keys'],9600000)
        self.assertEqual(selected['hash_keys'],120000)
        self.assertEqual(selected['memory_limit_bytes'],5*1024**3)
        for extra in [['--launch'],['--writes-per-sec','50000']]:
            with self.assertRaises(SystemExit): launcher.configuration(args+extra)
        execution=launcher.configuration(BASE+args+['--launch','--data-dir','/data/bench'])
        self.assertEqual(execution['case']['case_id'],selected['case_id'])
        self.assertEqual(execution['schema'],'document-cases/1')

    def test_config_precedence(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp)/'config.json'
            config.write_text(json.dumps({'writes_per_sec':123,'big_key_percent':5}))
            args = BASE+['--config',str(config)]
            self.assertEqual(launcher.configuration(args)['writes_per_sec'],123)
            self.assertEqual(launcher.configuration(args+['--writes-per-sec','456'])['writes_per_sec'],456)
            config.write_text(json.dumps({'launch':True}))
            with self.assertRaises(SystemExit): launcher.configuration(args)

    def test_default(self):
        c = launcher.configuration(BASE)
        self.assertFalse(c['launch'])
        self.assertEqual(c['writes_per_sec'], 50000)
        self.assertGreater(c['big_keys'], 0)

    def test_composition_not_request_mix(self):
        c = launcher.configuration(BASE+['--big-key-percent','5'])
        self.assertAlmostEqual(c['actual_big_key_percent'],5,places=2)
        self.assertEqual(c['big_request_percent'],.02)
        self.assertLess(abs(c['actual_payload_bytes']-1024**3), 2*1048576)

    def test_idle_and_pure_small(self):
        c = launcher.configuration(BASE+['--big-key-percent','0','--big-request-percent','0',
                                        '--writes-per-sec','0','--reads-per-sec','0'])
        self.assertEqual(c['big_keys'],0)

    def test_invalid(self):
        for args in [['--big-key-percent','101'], ['--writes-per-sec','-1'],
                     ['--big-key-percent','0'], ['--name','../../bad']]:
            with self.assertRaises(SystemExit): launcher.configuration(BASE+args)

    def test_metric(self):
        m = worker.Metric()
        for i in range(1,101): m.add(i)
        self.assertGreaterEqual(m.quantile(.99),99)
        self.assertLess(m.quantile(.99),100)
        self.assertEqual(m.maximum,100)

    @unittest.skipUnless(os.environ.get('SCENARIO_TEST_REDIS'), 'Set SCENARIO_TEST_REDIS for local smoke test')
    def test_real_redis(self):
        c = launcher.configuration(BASE+['--redis-binary',os.environ['SCENARIO_TEST_REDIS'],
                 '--mode','fork','--dataset-gib','.001','--big-key-percent','0',
                 '--big-request-percent','0','--writes-per-sec','100','--reads-per-sec','100',
                 '--clients','2','--before-seconds','1','--after-seconds','1','--port','26379'])
        with tempfile.TemporaryDirectory(prefix='redis-scenario-test-') as tmp:
            path = Path(tmp)/'setup.json'
            path.write_text(json.dumps(c))
            run = subprocess.run([sys.executable,str(Path(__file__).with_name('scenario-worker.py').resolve()),str(path)],
                                 cwd=tmp, capture_output=True, text=True, timeout=30)
            result = json.loads((Path(tmp)/'output/result.json').read_text())
            self.assertEqual(run.returncode,0,(result,run.stderr))
            self.assertEqual(result['status'],'completed')
            self.assertEqual(result['rdb_check_exit'],0)
            self.assertEqual({r['operation'] for r in result['metrics']},{'SET','GET'})


if __name__ == '__main__': unittest.main()
