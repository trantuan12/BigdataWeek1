"""Refuse misleading catalog metadata and release provenance."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from check_role_e import catalog_issues, expected_binding, lineage_issues
from common import read_json


class RoleEReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ev = ROOT / read_json(ROOT / 'config/team.json')['evidence_dir']
        cls.state = read_json(ev / 'state.json')
        cls.binding = read_json(ev / 'release_candidate/snapshot_binding.json')
        cls.catalog = read_json(ev / 'release_candidate/catalog.json')
        cls.silver_columns = read_json(ROOT / 'schemas/silver_schema.json')['columns']
        cls.gold_columns = read_json(ROOT / 'schemas/gold_schema.json')['columns']
        import json
        cls.events = [json.loads(line) for line in (ev / 'lineage.jsonl').read_text().splitlines() if line]

    def test_expected_values_are_derived_from_state_not_candidate(self):
        changed = dict(self.binding, gold_source_silver_version=0, code_sha256='0' * 64)
        wanted = expected_binding(self.state)
        self.assertEqual(wanted['gold_source_silver_version'], self.state['silver_final_version'])
        self.assertNotEqual(changed['gold_source_silver_version'], wanted['gold_source_silver_version'])
        self.assertNotEqual(changed['code_sha256'], wanted['code_sha256'])

    def test_catalog_rejects_counts_units_boolean_versions_and_bad_retention(self):
        def inspect(catalog):
            return catalog_issues(catalog, self.binding, self.state,
                                  self.catalog['datasets'][0]['row_count'], self.catalog['datasets'][1]['row_count'],
                                  self.silver_columns, self.gold_columns)
        self.assertEqual(inspect(self.catalog), [])
        mutations = [lambda c: c['datasets'][0].update(row_count=999999),
                     lambda c: c['datasets'][0].update(version=True),
                     lambda c: c['datasets'][1]['columns'][4].update(unit='Fahrenheit'),
                     lambda c: c['retention'].update(time_basis='simulated AS_OF'),
                     lambda c: c.update(trusted_manifest_sha256='0' * 64)]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                changed = deepcopy(self.catalog)
                mutation(changed)
                self.assertTrue(inspect(changed))

    def test_lineage_rejects_missing_terminal_and_wrong_release_snapshot(self):
        self.assertEqual(lineage_issues(self.events, self.state)[0], [])
        self.assertTrue(lineage_issues(self.events[:-1], self.state)[0])
        changed = deepcopy(self.events)
        for event in changed:
            if event['job']['name'] == 'release' and event['eventType'] == 'COMPLETE':
                event['inputs'][0]['name'] = self.state['silver_path'] + '@999'
        self.assertIn('RELEASE_SNAPSHOT_INPUTS', lineage_issues(changed, self.state)[0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
