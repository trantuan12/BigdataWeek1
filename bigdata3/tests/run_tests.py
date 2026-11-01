"""Run real tests serially and record results without changing operational data."""
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]


class EvidenceResult(unittest.TextTestResult):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.records=[]

    def addSuccess(self,test):
        super().addSuccess(test)
        self.records.append({'test':test.id(),'status':'PASS'})

    def addFailure(self,test,err):
        super().addFailure(test,err)
        self.records.append({'test':test.id(),'status':'FAIL'})

    def addError(self,test,err):
        super().addError(test,err)
        self.records.append({'test':test.id(),'status':'ERROR'})

    def addSkip(self,test,reason):
        super().addSkip(test,reason)
        self.records.append({'test':test.id(),'status':'SKIP','reason':reason})


suite=unittest.defaultTestLoader.discover(str(ROOT/'tests'),pattern='test_*.py')
result=unittest.TextTestRunner(verbosity=2,resultclass=EvidenceResult).run(suite)
config=json.loads((ROOT/'config/team.json').read_text(encoding='utf-8'))
path=Path(config['evidence_dir'])/'implementation_tests.json'
path.write_text(json.dumps({'passed':result.wasSuccessful() and not result.skipped,
    'tests_run':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
    'skipped':len(result.skipped),'tests':result.records,
    'scope':'Synthetic test rows use separate Delta paths; operational table snapshots remain unchanged.'},indent=2)+'\n',encoding='utf-8')
sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
