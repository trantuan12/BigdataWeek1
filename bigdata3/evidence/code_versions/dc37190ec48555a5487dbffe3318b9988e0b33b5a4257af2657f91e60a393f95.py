"""Run after incremental. Small Gold snapshot only is collected for plotting.
The SQL samples are supplied analytical checks, not the assessed build_gold code.
"""
import argparse,json,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from common import read_json,write_json,emit_lineage,sha256_file
from session import make_spark
from run_lab import snapshot,serial
p=argparse.ArgumentParser();p.add_argument('--config',default='config/local.json');a=p.parse_args()
c=read_json(a.config);ev=Path(c['evidence_dir']);state=read_json(ev/'state.json');spark=make_spark(c)
run_id=str(uuid.uuid4())
inputs=[(c.get('store_id','local'),state['gold_path']+'@'+str(state['gold_final_version']))]
emit_lineage(ev/'lineage.jsonl','START',run_id,'analytics',inputs,[])
try:
    snapshot(spark,state['gold_path'],state['gold_final_version']).createOrReplaceTempView('gold')
    queries={
       'Q1_site_day':'''SELECT site,event_date,n_observations,mean_temperature_c,n_late,late_rate
                          FROM gold ORDER BY event_date,site''',
       'Q2_weighted_day':'''SELECT event_date, SUM(n_observations) AS n_observations,
                          CAST(SUM(sum_temperature_c)/SUM(n_observations) AS DECIMAL(18,6)) AS weighted_mean_c,
                          CAST(AVG(mean_temperature_c) AS DECIMAL(18,6)) AS naive_mean_of_means_c
                          FROM gold GROUP BY event_date ORDER BY event_date''',
       'Q3_above25_rate':'''SELECT site,event_date,n_observations,n_above_25,
                         CAST(n_above_25/n_observations AS DECIMAL(18,6)) AS above25_rate
                         FROM gold ORDER BY event_date,above25_rate DESC,site'''}
    out={}
    for key,sql in queries.items():
        rows=[serial(r.asDict()) for r in spark.sql(sql).collect()]
        write_json(ev/(key+'.json'),{'gold_path':state['gold_path'],
            'gold_version':state['gold_final_version'],
            'source_silver_version':state['gold_source_silver_version'],
            'sql':sql,'rows':rows});out[key]=rows
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    total=sum(r['n_observations'] for r in out['Q2_weighted_day'])
    annotation=(f"Silver v{state['gold_source_silver_version']} / Gold v{state['gold_final_version']} | "
        f"n={total:,} retained observations | UTC\n"
        "Synthetic teaching data; inherited QA disagreements remain; late means lag > 900 s.")
    rows=out['Q2_weighted_day'];fig,ax=plt.subplots(figsize=(10,5.4))
    ax.plot([r['event_date'] for r in rows],[float(r['weighted_mean_c']) for r in rows],marker='o')
    ax.plot([r['event_date'] for r in rows],[float(r['naive_mean_of_means_c']) for r in rows],
            marker='s',linestyle='--',label='Unweighted mean of site means')
    ax.lines[0].set_label('Observation-weighted mean')
    for r in rows:
        ax.annotate(f"n={r['n_observations']:,}",(r['event_date'],float(r['weighted_mean_c'])),
                    xytext=(0,9),textcoords='offset points',ha='center',fontsize=8)
    ax.set(title='Synthetic telemetry: observation-weighted mean',xlabel='UTC event date',ylabel='Temperature (C)')
    ax.legend();ax.grid(alpha=.2);ax.margins(y=.25)
    ax.tick_params(axis='x',rotation=20);fig.text(.02,.025,annotation,fontsize=8)
    fig.tight_layout(rect=(0,.12,1,1));fig.savefig(ev/'daily_mean.png',dpi=160);plt.close(fig)
    rows=out['Q1_site_day'];sites=sorted(set(r['site'] for r in rows));fig,ax=plt.subplots(figsize=(10,5.4))
    for site in sites:
        rs=[r for r in rows if r['site']==site]
        ax.plot([r['event_date'] for r in rs],[float(r['late_rate']) for r in rs],marker='o',
                label=f"{site} (n={sum(r['n_observations'] for r in rs):,})")
    ax.set(title='Synthetic telemetry: late-arrival fraction',xlabel='UTC event date',ylabel='Late observations / observations')
    ax.grid(alpha=.2);ax.set_ylim(bottom=0)
    ax.tick_params(axis='x',rotation=20);ax.legend();fig.text(.02,.025,annotation,fontsize=8)
    fig.tight_layout(rect=(0,.12,1,1));fig.savefig(ev/'late_fraction.png',dpi=160);plt.close(fig)
    output_files=list(queries)+['daily_mean','late_fraction']
    outputs=[('local-evidence',str(ev/(name+('.json' if name in queries else '.png')))) for name in output_files]
    emit_lineage(ev/'lineage.jsonl','COMPLETE',run_id,'analytics',inputs,outputs)
    write_json(ev/'run_analytics.json',{'run_id':run_id,'job':'analytics','status':'COMPLETE',
        'gold_version':state['gold_final_version'],'source_silver_version':state['gold_source_silver_version'],
        'code_sha256':sha256_file(__file__),'outputs':[p for _,p in outputs]})
    print('Saved 3 SQL results and 2 version-labelled plots.')
except Exception:
    emit_lineage(ev/'lineage.jsonl','FAIL',run_id,'analytics',inputs,[])
    raise
finally:spark.stop()
