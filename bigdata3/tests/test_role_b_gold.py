"""Role B unit tests: Gold rollup semantics and the stale/tied ordering used to review Role C.
Standard library only; no Delta table is written."""
from decimal import Decimal
from pathlib import Path
import sqlite3
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'tools')]
from check_role_b import build_gold_static_audit, merge_predicate_sql, pick_winner, precedes
from verify_results import aggregate


def obs(key, site, temp, ordinal, day='2026-02-02', sensor='S01', late=False):
    return dict(record_id=key, sensor_id=sensor, site=site, event_time_utc=f'{day}T10:00:00Z',
                ingest_time_utc=f'{day}T10:20:00Z' if late else f'{day}T10:05:00Z',
                temperature_c=temp, is_late=late, source_object='a.csv', source_row=ordinal,
                source_sha256='a' * 64)


class RoleBGoldTests(unittest.TestCase):
    def test_weighted_rollup_differs_from_mean_of_means_when_counts_differ(self):
        rows = [obs('R000001', 'zone-1', '10.00', 1), obs('R000002', 'zone-1', '20.00', 2, sensor='S02'),
                obs('R000003', 'zone-1', '30.00', 3, sensor='S03', late=True),
                obs('R000004', 'zone-2', '40.00', 4, sensor='S06')]
        gold = aggregate(rows)
        self.assertEqual([g['n_observations'] for g in gold], [3, 1])
        weighted = sum(Decimal(g['sum_temperature_c']) for g in gold) / sum(g['n_observations'] for g in gold)
        naive = sum(Decimal(g['mean_temperature_c']) for g in gold) / len(gold)
        self.assertEqual(weighted, Decimal('25'))          # 100 / 4
        self.assertEqual(naive, Decimal('30'))             # (20 + 40) / 2
        self.assertEqual(gold[0]['late_rate'], '0.333333')  # late rows stay in the denominator
        self.assertEqual(gold[0]['n_sensors'], 3)

    def test_strict_threshold_excludes_exactly_25(self):
        gold = aggregate([obs('R000001', 'zone-1', '25.00', 1), obs('R000002', 'zone-1', '25.01', 2)])
        self.assertEqual(gold[0]['n_above_25'], 1)

    def test_build_gold_static_audit(self):
        audit = build_gold_static_audit()
        self.assertEqual(audit['forbidden_calls_found'], [])
        self.assertTrue(audit['groupBy_keys_site_event_date'] and audit['strict_threshold_gt_25']
                        and audit['count_distinct_sensor'])

    def test_tie_and_stale_ordering(self):
        current = dict(ingest_time_utc='2026-02-03T01:03:30Z', source_object='observations_a.csv', source_row=3005)
        stale = dict(ingest_time_utc='2026-02-03T01:03:00Z', source_object='lab3/source_batch02.csv', source_row=125)
        self.assertFalse(precedes(stale, current))
        a = dict(ingest_time_utc='2026-02-02T16:52:30Z', source_object='lab3/source_batch02.csv', source_row=105)
        b = dict(a, source_row=140)
        self.assertIs(pick_winner([a, b]), a)
        self.assertIs(pick_winner([b, a]), a)
        self.assertFalse(precedes(dict(a), a))

    def test_actual_merge_predicate_text_executes(self):
        db = sqlite3.connect(':memory:')
        db.execute('CREATE TABLE t(ingest_time_utc, source_object, source_row)')
        db.execute('CREATE TABLE s(ingest_time_utc, source_object, source_row)')
        db.execute("INSERT INTO t VALUES ('2026-02-06 00:30:00', 'b.csv', 5)")
        db.execute("INSERT INTO s VALUES ('2026-02-06 00:30:00', 'b.csv', 5)")
        self.assertEqual(db.execute(f'SELECT ({merge_predicate_sql()}) FROM s, t').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
