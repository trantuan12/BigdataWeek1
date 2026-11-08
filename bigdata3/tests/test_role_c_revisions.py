"""Unit tests for Role C revision ordering, merge decisions, replay idempotence, and D cross-check."""
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'tools')]
from common import load_csv, load_registry, read_json, validate_rows
from check_role_c import (apply_merge_reference, expected_benchmark_from_gold,
                          precedes_in_order, select_incoming_winners)


class RoleCRevisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = load_registry(ROOT / 'data/handoff/sensors.csv')
        cls.baseline = validate_rows(load_csv(ROOT / 'data/handoff/curated.csv'), cls.registry, True)
        cls.incoming = validate_rows(load_csv(ROOT / 'data/changes/changes_batch02.csv'), cls.registry, False)
        ev = ROOT / read_json(ROOT / 'config/team.json')['evidence_dir']
        cls.gold_v1 = read_json(ev / 'gold_after_batch02.json')
        cls.bench = read_json(ev / 'benchmark_report.json')

    def test_incoming_winners_and_reordering_determinism(self):
        winners, superseded = select_incoming_winners(self.incoming)
        rev_winners, _ = select_incoming_winners(list(reversed(self.incoming)))
        self.assertEqual(len(self.incoming), 140)
        self.assertEqual(len(winners), 130)
        self.assertEqual(len(superseded), 10)
        self.assertEqual(winners, rev_winners)
        self.assertEqual((winners['R010001']['source_row'], winners['R010001']['temperature_c']), (1, '19.10'))
        self.assertEqual((winners['R002001']['source_row'], winners['R002001']['temperature_c']), (101, '20.10'))

    def test_merge_decisions_and_second_application_noop(self):
        merged_1, _, _, counts_1, traces = apply_merge_reference(self.baseline, self.incoming)
        self.assertEqual(counts_1, {'updated_newer': 20, 'unchanged_stale': 10, 'inserted': 100})
        self.assertEqual(len(merged_1), 9710)
        self.assertEqual(traces['R003001']['decision'], 'unchanged_stale')
        self.assertEqual(traces['R003001']['selected']['source_row'], 3001)
        self.assertEqual(traces['R003001']['selected']['temperature_c'], '18.10')
        merged_2, _, _, counts_2, _ = apply_merge_reference(list(merged_1.values()), self.incoming)
        self.assertEqual(merged_2, merged_1)
        self.assertEqual(counts_2, {'unchanged_equal': 120, 'unchanged_stale': 10})

    def test_strict_update_predicate_precedence(self):
        base = {'record_id': 'R1', 'ingest_time_utc': '2026-02-06T00:30:00Z',
                'source_object': 'b.csv', 'source_row': 5}
        newer = dict(base, ingest_time_utc='2026-02-06T00:31:00Z', source_object='z.csv', source_row=99)
        stale = dict(base, ingest_time_utc='2026-02-06T00:29:00Z', source_object='a.csv', source_row=1)
        same_time_better_obj = dict(base, source_object='a.csv', source_row=99)
        same_time_worse_obj = dict(base, source_object='c.csv', source_row=1)
        same_time_obj_better_row = dict(base, source_row=4)
        same_time_obj_worse_row = dict(base, source_row=6)
        self.assertTrue(precedes_in_order(newer, base))
        self.assertFalse(precedes_in_order(stale, base))
        self.assertTrue(precedes_in_order(same_time_better_obj, base))
        self.assertFalse(precedes_in_order(same_time_worse_obj, base))
        self.assertTrue(precedes_in_order(same_time_obj_better_row, base))
        self.assertFalse(precedes_in_order(same_time_obj_worse_row, base))
        self.assertFalse(precedes_in_order(dict(base), base))

    def test_cross_check_d_benchmark_equivalence(self):
        expected = expected_benchmark_from_gold(self.gold_v1, self.bench['benchmark_copies'], '2026-02-06')
        self.assertEqual(expected, self.bench['query_result'])
        self.assertEqual(sum(r['n_observations'] for r in expected), 10000)


if __name__ == '__main__':
    unittest.main(verbosity=2)
