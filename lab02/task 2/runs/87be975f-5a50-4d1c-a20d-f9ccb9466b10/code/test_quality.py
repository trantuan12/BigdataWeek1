"""Counterexamples for Task 2 populations, NULL handling and winner policy."""
import json
import random
import unittest
from pathlib import Path

import duckdb

from run_tasks import TASK2, load_policy, rows


def observation(record_id="R000001", **changes):
    row = dict(record_id=record_id, sensor_id="S01", event_time="2026-02-02T00:00:00Z",
               ingest_time="2026-02-02T00:01:00Z", reading="20", unit="C",
               operator_email="", source_object="source-a", source_row="1",
               source_sha256="a" * 64, parse_ok="true", raw_payload="restricted")
    row.update(changes)
    return row


def database(observations):
    con = duckdb.connect()
    fields = list(observation())
    con.execute("CREATE TABLE raw_envelopes (" + ",".join(f + " VARCHAR" for f in fields) + ")")
    if observations:
        con.executemany("INSERT INTO raw_envelopes VALUES (" + ",".join("?" for _ in fields) + ")",
                        [[r[f] for f in fields] for r in observations])
    con.execute("CREATE TABLE registry AS SELECT 'S01' AS sensor_id, 'test-site' AS site")
    con.execute("CREATE TABLE qa_reference (record_id VARCHAR, reference_c DECIMAL(8,2))")
    load_policy(con, json.loads((TASK2 / "contract.json").read_text(encoding="utf-8")))
    con.execute((TASK2 / "code/typed.sql").read_text(encoding="utf-8"))
    con.execute((TASK2 / "code/quality.sql").read_text(encoding="utf-8"))
    return con


class QualityTests(unittest.TestCase):
    def test_null_is_failure_and_zero_denominators_stay_zero(self):
        with database([observation(reading=None)]) as con:
            metrics = {r["rule_id"]: r for r in rows(con, "SELECT * FROM metric_counts WHERE phase='before'")}
            for rule in ["Q_REQUIRED", "Q_VALUE"]:
                self.assertEqual((metrics[rule]["numerator"], metrics[rule]["denominator"]), (0, 1))
        with database([]) as con:
            for r in rows(con, "SELECT * FROM metric_counts"):
                self.assertEqual((r["numerator"], r["denominator"]), (0, 0))

    def test_conversion_finiteness_units_privacy_and_unrounded_range(self):
        records = [observation("R000001", reading="32", unit="F"),
                   observation("R000002", reading="NaN", source_row="2"),
                   observation("R000003", unit="K", source_row="3"),
                   observation("R000004", operator_email="", source_row="4"),
                   observation("R000005", reading="-30.00001", source_row="5"),
                   observation("R000006", reading="Infinity", source_row="6")]
        with database(records) as con:
            got = con.execute("SELECT record_id, temperature_c FROM profile_candidate ORDER BY record_id").fetchall()
            self.assertEqual([r[0] for r in got], ["R000001", "R000004"])
            self.assertEqual(float(got[0][1]), 0.0)
            self.assertFalse(con.execute("SELECT value_ok FROM evaluated_winners WHERE record_id='R000005'").fetchone()[0])

    def test_latest_invalid_never_falls_back_and_ties_are_stable(self):
        records = [observation(source_row="1"),
                   observation(reading="100", ingest_time="2026-02-02T00:02:00Z", source_row="2"),
                   observation("R000002", source_object="source-z", source_row="3"),
                   observation("R000002", source_object="source-a", source_row="4"),
                   observation("R000002", source_object="source-a", source_row="5")]
        expected = [("R000001", "source-a", 2), ("R000002", "source-a", 4)]
        for iteration in range(3):
            random.Random(iteration).shuffle(records)
            with database(records) as con:
                self.assertEqual(con.execute("SELECT record_id, source_object, source_row FROM winners ORDER BY record_id").fetchall(), expected)
                self.assertEqual(con.execute("SELECT record_id FROM profile_candidate").fetchall(), [("R000002",)])
                self.assertEqual(con.execute("SELECT duplicate_excess FROM intake_counts").fetchone()[0], 3)

    def test_prekey_exclusions_chronology_and_timeliness_cohort(self):
        records = [observation(record_id=" ", ingest_time="bad", source_row="1"),
                   observation("R000002", parse_ok="false", source_row="2"),
                   observation("R000003", event_time="2026-02-02T00:05:00Z", source_row="3"),
                   observation("R000004", ingest_time="2026-02-02T00:16:00Z", source_row="4"),
                   observation("R000005", source_row="5"),
                   observation("R000006", ingest_time="2026-2-02T00:01:00Z", source_row="6")]
        with database(records) as con:
            profile = rows(con, "SELECT * FROM intake_counts")[0]
            self.assertEqual(profile["raw_count"], 6)
            self.assertEqual(profile["parse_failures"], 1)
            self.assertEqual(profile["key_rejected_count"], 2)
            self.assertEqual(profile["winner_count"], 3)
            timely = rows(con, "SELECT * FROM metric_counts WHERE phase='before' AND rule_id='Q_TIMELY'")[0]
            self.assertEqual((timely["numerator"], timely["denominator"], timely["excluded"]), (1, 2, 1))
            self.assertEqual(con.execute("SELECT count(*) FROM profile_candidate WHERE is_late").fetchone()[0], 1)

    def test_reference_coverage_separate_from_agreement(self):
        with database([observation(reading="20")]) as con:
            con.execute("INSERT INTO qa_reference VALUES ('R000001', 21), ('R999999', 20)")
            result = {r["rule_id"]: r for r in rows(con, "SELECT * FROM metric_counts WHERE rule_id LIKE 'Q_QA_%'")}
            self.assertEqual((result["Q_QA_COVERAGE"]["numerator"], result["Q_QA_COVERAGE"]["denominator"]), (1, 2))
            self.assertEqual((result["Q_QA_AGREEMENT"]["numerator"], result["Q_QA_AGREEMENT"]["denominator"]), (0, 1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
