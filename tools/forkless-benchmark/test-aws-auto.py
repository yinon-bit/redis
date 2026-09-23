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
