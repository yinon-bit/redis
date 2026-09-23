#!/usr/bin/env python3
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

s=importlib.util.spec_from_file_location('checker',Path(__file__).with_name('check-results.py'))
c=importlib.util.module_from_spec(s); s.loader.exec_module(c)

class Tests(unittest.TestCase):
    def fixture(self,directory):
        state=dict(run_id='test',cases=['fixture.snapshot.base.ample.cpu6'],instance_terminated=True,
            security_group_deleted=True,volumes=['vol-test'],volumes_deleted=['vol-test'])
        (directory/'resources.json').write_text(json.dumps(state))
        setup={'scenario':state['cases'][0],'case':{'case_id':state['cases'][0],'scenario':'point_in_time','repetitions':1}}
        result={'schema':'gated-fixtures/1','case_id':state['cases'][0],'status':'completed','attempts':[
            {'status':'completed','snapshot_cut_values_valid':True,'live_state_valid':True}]}
        self.bundle(directory,setup,result)
        return setup,result

    def bundle(self,directory,setup,result):
        contents={'case-0/setup.json':json.dumps(setup).encode(),'case-0/output/result.json':json.dumps(result).encode()}
        contents['SHA256SUMS']=''.join(hashlib.sha256(data).hexdigest()+'  '+name+'\n' for name,data in contents.items()).encode()
        archive=directory/'artifacts-1.tar.gz'
        with tarfile.open(archive,'w:gz') as t:
            for name,data in contents.items():
                entry=tarfile.TarInfo('evidence/'+name);entry.size=len(data);t.addfile(entry,io.BytesIO(data))
        (directory/'receipt-1.json').write_text(json.dumps({'sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}))

    def test_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);self.fixture(p);self.assertEqual(c.check(p)['status'],'PASS')

    def test_missing_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);self.fixture(p);(p/'receipt-1.json').unlink();self.assertEqual(c.check(p)['status'],'MISSING')

    def test_bad_checksum(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);self.fixture(p);(p/'receipt-1.json').write_text('{"sha256":"bad"}')
            self.assertEqual(c.check(p)['status'],'FAIL')

    def test_false_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);setup,result=self.fixture(p);result['attempts'][0]['snapshot_cut_values_valid']=False
            self.bundle(p,setup,result);self.assertEqual(c.check(p)['status'],'FAIL')

    def test_missing_repetition(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);setup,result=self.fixture(p);setup['case']['repetitions']=3
            self.bundle(p,setup,result);self.assertEqual(c.check(p)['status'],'MISSING')

    def test_missing_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp);self.fixture(p);s=json.loads((p/'resources.json').read_text());s['volumes_deleted']=[]
            (p/'resources.json').write_text(json.dumps(s));self.assertEqual(c.check(p)['status'],'MISSING')

if __name__=='__main__': unittest.main()
