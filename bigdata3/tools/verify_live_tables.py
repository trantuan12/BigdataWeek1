"""Read pinned Delta snapshots and independently verify their actual values."""
import argparse
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'templates')]
from common import read_json, write_json, load_registry, logical_sha256
from session import make_spark
from run_lab import snapshot, rows_from_silver, receipt_digest, serial, small_records
from student_impl import build_gold


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',default='config/team.json')
    args=parser.parse_args()
    config=read_json(args.config)
    ev=Path(config['evidence_dir'])
    state=read_json(ev/'state.json')
    spark=make_spark(config)
    checks=[]

    def check(name,condition):
        checks.append({'check_id':name,'passed':bool(condition)})
        if not condition:
            raise ValueError('LIVE_VERIFICATION_FAILED:'+name)

    try:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        registry=load_registry(ROOT/'data/handoff/sensors.csv')
        current=rows_from_silver(spark,state['silver_path'],state['silver_final_version'],registry)
        replay=rows_from_silver(spark,state['silver_path'],state['silver_replay_version'],registry)
        initial=rows_from_silver(spark,state['silver_path'],state['silver_initial_version'],registry)
        inc=read_json(ev/'incremental.json')
        check('LIVE_initial_handoff',logical_sha256(initial)==state['handoff_logical_sha256'])
        check('LIVE_final_Silver',logical_sha256(current)==inc['logical_sha256_after'])
        check('LIVE_replay_Silver',logical_sha256(replay)==logical_sha256(current))
        bronze=snapshot(spark,state['bronze_path'],state['bronze_final_version'])
        check('LIVE_unique_physical_receipts',bronze.count()==bronze.select('source_object','source_row','source_sha256').distinct().count()
              ==inc['bronze_before_replay'])
        check('LIVE_full_Bronze_replay',receipt_digest(spark,state['bronze_path'],state['bronze_final_version'])==
              receipt_digest(spark,state['bronze_path'],state['bronze_replay_version'])==inc['bronze_receipt_sha256_after_replay'])
        silver=snapshot(spark,state['silver_path'],state['silver_final_version'])
        check('LIVE_Silver_key_and_UTC_date',silver.select('record_id').distinct().count()==len(current)
              and silver.where(F.col('event_date')!=F.to_date('event_time_utc')).count()==0)
        recomputed=sorted([serial(r) for r in small_records(build_gold(silver))],key=lambda r:(r['site'],r['event_date']))
        gold=snapshot(spark,state['gold_path'],state['gold_final_version'])
        actual=sorted([serial(r) for r in small_records(gold)],key=lambda r:(r['site'],r['event_date']))
        check('LIVE_pinned_Gold_recomputation',actual==recomputed==read_json(ev/'release_candidate/gold.json'))
        expected_gold={'site':'string','event_date':'date','n_observations':'bigint',
            'sum_temperature_c':'decimal(18,2)','mean_temperature_c':'decimal(18,6)',
            'min_temperature_c':'decimal(18,2)','max_temperature_c':'decimal(18,2)',
            'n_late':'bigint','late_rate':'decimal(18,6)','n_above_25':'bigint','n_sensors':'bigint'}
        check('LIVE_Gold_exact_types',dict(gold.dtypes)==expected_gold and gold.columns==list(expected_gold))
        expected_silver={'record_id':'string','sensor_id':'string','site':'string',
            'event_time_utc':'timestamp','ingest_time_utc':'timestamp','temperature_c':'decimal(8,2)',
            'is_late':'boolean','source_object':'string','source_row':'bigint','source_sha256':'string','event_date':'date'}
        check('LIVE_Silver_exact_types',dict(silver.dtypes)==expected_silver and silver.columns==list(expected_silver))
        tables={}
        for name,frame in [('bronze',bronze),('silver',silver),('gold',gold)]:
            path=state[name+'_path']
            table=DeltaTable.forPath(spark,path)
            history=[serial(r.asDict(recursive=True)) for r in table.history().select(
                'version','timestamp','operation','operationParameters','operationMetrics').collect()]
            detail=table.detail().select('format','numFiles','sizeInBytes','partitionColumns').first().asDict()
            tables[name]={'path':path,'pinned_version':state[name+'_final_version'],
                'schema':frame.schema.jsonValue(),'rows_at_pinned_version':frame.count(),
                'latest_detail':detail,'history':history}
        write_json(ev/'live_table_verification.json',{'passed':True,'checks':checks,'tables':tables,
            'source_silver_version_for_gold':state['gold_source_silver_version'],
            'application_id':spark.sparkContext.applicationId,
            'scope':'Additional technical verification; no assertion of a human review or owner publication.'})
        print(f'PASS: {len(checks)} live Delta checks; exact schemas and histories recorded.')
    except Exception:
        write_json(ev/'live_table_verification_error.json',{'passed':False,'checks':checks})
        raise
    finally:
        spark.stop()


if __name__=='__main__':
    main()
