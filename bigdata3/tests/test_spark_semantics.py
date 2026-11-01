"""Small live Spark/Delta fixtures, isolated from operational snapshots."""
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
import sys
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'templates')]
from common import read_json
from session import make_spark
from run_lab import transport_df, snapshot, version
from student_impl import build_gold, choose_revisions, merge_silver, benchmark_query


def row(key, value='25.00', source='a.csv', ordinal=1, ingest='2026-02-06T00:15:00Z',
        event='2026-02-06T00:00:00Z', site='zone-1', sensor='S01', late=False):
    return dict(record_id=key, sensor_id=sensor, site=site, event_time_utc=event,
        ingest_time_utc=ingest, temperature_c=value, is_late=late,
        source_object=source, source_row=ordinal, source_sha256='a'*64)


class SparkSemanticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = read_json(ROOT/'config/team.json')
        cls.spark = make_spark(cls.config)
        cls.test_root = (cls.config['lakehouse_root'].rstrip('/') + '/' + cls.config['group_id']
                         + '/' + cls.config['run_label'] + '/isolated-tests/' + str(uuid.uuid4()))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_winner_all_ordering_components_and_reordering(self):
        from pyspark.sql import functions as F
        rows = [row('R900001', source='z.csv', ordinal=1),
                row('R900001', source='a.csv', ordinal=9),
                row('R900001', source='a.csv', ordinal=2),
                row('R900001', source='0.csv', ordinal=1, ingest='2026-02-06T00:14:00Z'),
                row('R900002', source='b.csv', ordinal=5)]
        frame = transport_df(self.spark, rows)
        selected = choose_revisions(frame)
        actual = selected.orderBy('record_id').collect()
        other = choose_revisions(frame.orderBy(F.col('source_row').desc()).repartition(3)).orderBy('record_id').collect()
        self.assertEqual(actual, other)
        self.assertEqual(selected.columns, frame.columns)
        self.assertEqual([(r.record_id,r.source_object,r.source_row) for r in actual],
                         [('R900001','a.csv',2),('R900002','b.csv',5)])

    def measurements(self):
        return [row('R910001', '25.00'),
                row('R910002', '26.00', ordinal=2, sensor='S02', late=True, ingest='2026-02-06T00:15:01Z'),
                row('R910003', '0.00', ordinal=3, site='zone-2', sensor='S06'),
                row('R910004', '-1.25', ordinal=4, event='2026-02-05T00:00:00Z', ingest='2026-02-05T00:15:00Z')]

    def test_gold_exact_values_types_and_threshold(self):
        output = build_gold(transport_df(self.spark, self.measurements()))
        records = output.orderBy('site','event_date').collect()
        selected = next(r for r in records if r.site=='zone-1' and str(r.event_date)=='2026-02-06')
        self.assertEqual((selected.n_observations, selected.sum_temperature_c, selected.mean_temperature_c,
            selected.min_temperature_c, selected.max_temperature_c, selected.n_late, selected.late_rate,
            selected.n_above_25, selected.n_sensors),
            (2,Decimal('51.00'),Decimal('25.500000'),Decimal('25.00'),Decimal('26.00'),1,Decimal('0.500000'),1,2))
        expected = {'site':'string','event_date':'date','n_observations':'bigint',
            'sum_temperature_c':'decimal(18,2)','mean_temperature_c':'decimal(18,6)',
            'min_temperature_c':'decimal(18,2)','max_temperature_c':'decimal(18,2)',
            'n_late':'bigint','late_rate':'decimal(18,6)','n_above_25':'bigint','n_sensors':'bigint'}
        self.assertEqual(dict(output.dtypes),expected)
        self.assertEqual(output.columns,list(expected))
        self.assertEqual(build_gold(transport_df(self.spark,self.measurements()).limit(0)).count(),0)

    def test_benchmark_uses_date_filter_and_counts_replicas(self):
        frame = transport_df(self.spark,self.measurements()).crossJoin(self.spark.range(2).withColumnRenamed('id','replica_id'))
        actual = benchmark_query(frame).orderBy('site').collect()
        self.assertEqual([(r.site,r.n_observations,r.sum_temperature_c,r.n_late) for r in actual],
                         [('zone-1',4,Decimal('102.00'),2),('zone-2',2,Decimal('0.00'),0)])

    def test_live_merge_stale_ties_inserts_and_equal_noop(self):
        initial = [row('R920001', '10.00', ordinal=1),
                   row('R920002', '11.00', source='b.csv', ordinal=2),
                   row('R920003', '12.00', ordinal=8),
                   row('R920004', '13.00', ordinal=4),
                   row('R920005', '14.00', ordinal=5)]
        changes = [row('R920001', '20.00', ordinal=10, ingest='2026-02-06T00:16:00Z', late=True),
                   row('R920002', '21.00', source='a.csv', ordinal=20),
                   row('R920003', '22.00', ordinal=7),
                   row('R920004', '23.00', ordinal=40, ingest='2026-02-06T00:14:00Z'),
                   row('R920005', '15.00', ordinal=5),
                   row('R920006', '24.00', ordinal=60)]
        path = self.test_root+'/merge'
        transport_df(self.spark,initial).write.format('delta').mode('errorifexists').save(path)
        baseline_version = version(self.spark,path)
        winners = choose_revisions(transport_df(self.spark,changes))
        merge_silver(self.spark,path,winners)
        first_version = version(self.spark,path)
        first = snapshot(self.spark,path,first_version).orderBy('record_id').collect()
        self.assertEqual([r.temperature_c for r in first],
                         [Decimal(v) for v in ['20','21','22','13','14','24']])
        self.assertEqual((first[1].source_object,first[2].source_row),('a.csv',7))
        merge_silver(self.spark,path,winners)
        self.assertEqual(first,snapshot(self.spark,path,version(self.spark,path)).orderBy('record_id').collect())
        self.assertEqual([r.temperature_c for r in snapshot(self.spark,path,baseline_version).orderBy('record_id').collect()],
                         [Decimal(v) for v in ['10','11','12','13','14']])


if __name__ == '__main__':
    unittest.main(verbosity=2)
