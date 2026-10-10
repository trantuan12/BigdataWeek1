import json
from pathlib import Path

runs = ['r1-c1', 'r1-c4', 'r2-c4', 'r2-c1', 'r3-c1', 'r3-c4']
csv_lines = ['run,phase,concurrency,objects,successes,hash_failures,wall_s,MiB_s,p95_ms']

c1_put_mib, c1_get_mib = [], []
c4_put_mib, c4_get_mib = [], []

for r in runs:
    p = Path(f'evidence/{r}.jsonl')
    lines = [json.loads(x) for x in p.read_text(encoding='utf-8-sig').strip().split('\n') if x.strip()]
    summaries = [x for x in lines if x.get('kind') == 'summary']
    for s in summaries:
        phase = s['phase']
        phase_rows = [x for x in lines if x.get('op') == phase and x.get('kind') != 'summary']
        objs = len(phase_rows)
        succ = s['successes']
        hash_fails = sum(1 for x in phase_rows if phase == 'get' and not x.get('hash_ok', False))
        wall = s['wall_s']
        mib = s['goodput_MiB_s']
        p95 = s['p95_success_ms']
        c = s['concurrency']
        
        if c == 1:
            if phase == 'put': c1_put_mib.append(mib)
            else: c1_get_mib.append(mib)
        else:
            if phase == 'put': c4_put_mib.append(mib)
            else: c4_get_mib.append(mib)
            
        csv_lines.append(f"{s['run']},{phase},{c},{objs},{succ},{hash_fails},{wall:.4f},{mib:.2f},{p95:.2f}")

Path('benchmark-summary.csv').write_text('\n'.join(csv_lines) + '\n', encoding='utf-8')
print("=== BENCHMARK SUMMARY CSV ===")
print('\n'.join(csv_lines))

import statistics
print("\n=== COMPARISON ANALYSIS ===")
med_c1_put = statistics.median(c1_put_mib)
med_c1_get = statistics.median(c1_get_mib)
med_c4_put = statistics.median(c4_put_mib)
med_c4_get = statistics.median(c4_get_mib)

print(f"Median PUT Goodput:  c=1: {med_c1_put:.2f} MiB/s | c=4: {med_c4_put:.2f} MiB/s | Ratio (c4/c1): {med_c4_put/med_c1_put:.2f}x")
print(f"Median GET Goodput:  c=1: {med_c1_get:.2f} MiB/s | c=4: {med_c4_get:.2f} MiB/s | Ratio (c4/c1): {med_c4_get/med_c1_get:.2f}x")
