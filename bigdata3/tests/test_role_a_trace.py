"""Regressions for the incorrect source ordinal and enumeration-order example."""
import csv
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from check_role_a import source_projection, revision_order


class RoleATraceTests(unittest.TestCase):
    def test_source_projection_matches_three_physical_ordinals(self):
        for source, ordinal, key, temperature in (
                ('observations_a.csv', 381, 'R000381', '26.10'),
                ('observations_b.jsonl', 1, 'R005001', '18.10'),
                ('lab3/source_batch02.csv', 1, 'R010001', '19.10')):
            with self.subTest(source=source):
                row = source_projection(source, ordinal)
                self.assertEqual((row['record_id'], row['temperature_c']), (key, temperature))
                self.assertEqual(set(row), {'record_id', 'sensor_id', 'event_time_utc', 'ingest_time_utc', 'temperature_c'})

    def test_last_dictionary_row_is_not_the_tied_winner(self):
        with (ROOT / 'data/changes/changes_batch02.csv').open(encoding='utf-8', newline='') as handle:
            rows = [row for row in csv.DictReader(handle) if row['record_id'] == 'R010001']
        last = {row['record_id']: row for row in rows}['R010001']
        selected = min(rows, key=revision_order)
        self.assertEqual((last['source_row'], last['temperature_c']), ('131', '20.10'))
        self.assertEqual((selected['source_row'], selected['temperature_c']), ('1', '19.10'))
        self.assertEqual(selected, min(reversed(rows), key=revision_order))

    def test_invalid_source_and_ordinal_reject(self):
        for source, ordinal in (('../outside.csv', 1), ('observations_a.csv', 0),
                                ('observations_a.csv', True), ('observations_a.csv', 999999)):
            with self.subTest(source=source, ordinal=ordinal), self.assertRaises(ValueError):
                source_projection(source, ordinal)


if __name__ == '__main__':
    unittest.main(verbosity=2)
