#!/usr/bin/env python3
"""Offline provisioning-plan tests; never contact AWS."""
import base64
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('auto',Path(__file__).with_name('run-aws-auto.py'))
auto=importlib.util.module_from_spec(spec); spec.loader.exec_module(auto)


class Tests(unittest.TestCase):
    def test_quick(self):
        case='standard.mixed.write100.ample.cpu6.first.bgsave'
        args=auto.arguments(['--name','quick','--scenario',case,'--quick'])
        self.assertEqual(auto.setups(args,'test')[0]['case']['repetitions'],1)
        normal=auto.arguments(['--name','normal','--scenario',case])
        self.assertEqual(auto.setups(normal,'test')[0]['case']['repetitions'],3)
        with self.assertRaises(SystemExit):
            auto.arguments(['--name','test','--scenario',case,'--seed-manifest','x'])

    def test_seed_identity(self):
        spec=importlib.util.spec_from_file_location('seed',Path(__file__).with_name('seed-cache.py'))
        seed=importlib.util.module_from_spec(spec); spec.loader.exec_module(seed)
        from scenario_catalog import runnable_cases
        cases=runnable_cases()
        a=cases['standard.mixed.write100.ample.cpu6.first.bgsave']
        b=cases['standard.mixed.read100.ample.cpu6.first.bgsave']
        self.assertEqual(seed.key(a),seed.key(b))
        self.assertNotEqual(seed.key(a),seed.key(dict(a,small_keys=a['small_keys']+1)))
        self.assertNotEqual(seed.key(a),seed.key(dict(a,payload_generator='changed')))

    def test_mode_sequence(self):
        case='standard.mixed.write100.ample.cpu6.first.bgsave'
        args=auto.arguments(['--name','test','--scenario',case,'--mode','fork','--then-mode','hybrid','--hours','4'])
        rows=auto.setups(args,'test')
        self.assertEqual([r['mode'] for r in rows],['fork','hybrid'])
        self.assertEqual(rows[0]['case'],rows[1]['case'])
        self.assertNotEqual(rows[0]['run_id'],rows[1]['run_id'])

    def test_fixture_selection(self):
        from scenario_catalog import fixture_cases
        self.assertEqual(len(fixture_cases()),109)
        case='fixture.arena.string_boundary.during_capture.ample.cpu6'
        args=auto.arguments(['--name','test','--scenario',case])
        self.assertTrue(auto.setups(args,'test')[0]['case']['fixture'])
        with self.assertRaises(SystemExit):
            auto.arguments(['--name','test','--scenario',case,'--mode','forkless'])

    def test_selection_reproducible(self):
        args=auto.arguments(['--name','test','--random-cases','3'])
        self.assertEqual(len(args.scenario),3)
        self.assertEqual(args.scenario,auto.arguments(['--name','test','--random-cases','3']).scenario)
        self.assertFalse(args.launch)
        self.assertEqual(auto.setups(args,'run')[0]['case']['repetitions'],3)

    def test_reject_invalid(self):
        for extra in (['--hours','12'],['--team','Wrong Team'],['--case-timeout','7200']):
            with self.assertRaises(SystemExit): auto.arguments(['--name','test','--random-cases','3']+extra)
        with self.assertRaises(SystemExit): auto.arguments(['--name','test','--scenario','slow_sync_pressure'])

    def test_bootstrap_deadline(self):
        data=auto.bootstrap({'deadline_epoch':1800000000,'setups':[]})
        text=gzip.decompress(data).decode()
        self.assertIn('--on-calendar=',text)
        self.assertIn('shutdown -h now',text)
        self.assertNotIn('AWS_SECRET_ACCESS_KEY',text)
        self.assertLess(len(data),16385)

    def test_remote_bootstrap_config(self):
        data=auto.bootstrap({'deadline_epoch':1800000000,'setups':['private-marker'*100000]},
                            'https://example.invalid/config', 'a'*64)
        script=gzip.decompress(data).decode()
        self.assertNotIn('private-marker', script)
        self.assertIn('Configuration checksum mismatch', script)
        self.assertLess(len(data),16385)
        with self.assertRaises(ValueError):
            auto.bootstrap({'deadline_epoch':1800000000}, 'https://example.invalid/config')

    def test_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            config=Path(tmp)/'config.json'
            config.write_text(json.dumps({'writes_per_sec':50000,'big_key_percent':5,'mode':'fork'}))
            args=auto.arguments(['--name','test','--config',str(config)])
            setup=auto.setups(args,'test')[0]
            self.assertEqual(setup['mode'],'fork')
            self.assertEqual(setup['writes_per_sec'],50000)

    def test_source_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'source.tar.gz'
            checksum=auto.source_archive(path)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),checksum)
            with tarfile.open(path) as t:
                names=t.getnames()
                self.assertIn('src/forkless.c',names)
                self.assertIn('tools/forkless-benchmark/spec-worker.py',names)
                self.assertIn('tools/forkless-benchmark/fixture-worker.py',names)
                self.assertIn('tools/forkless-benchmark/prepare-fixture-build.py',names)
                self.assertFalse(any('results/' in x or '.aws/' in x or x.endswith('.pem') for x in names))

    def test_verify_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'result.tar.gz'
            for valid in (True,False):
                with tarfile.open(path,'w:gz') as t:
                    content=b'test'
                    expected=hashlib.sha256(content if valid else b'wrong').hexdigest()
                    for name,data in [('evidence/result.json',content),('evidence/SHA256SUMS',(expected+'  result.json\n').encode())]:
                        entry=tarfile.TarInfo(name); entry.size=len(data); t.addfile(entry,io.BytesIO(data))
                if valid: auto.verify_archive(path)
                else:
                    with self.assertRaises(RuntimeError): auto.verify_archive(path)


if __name__=='__main__': unittest.main()
