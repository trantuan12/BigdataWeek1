The data-integrity check is `python tools/check_inputs.py --trusted-manifest <TA-copy>`.
It does not require Spark and does not solve the assessed functions.

The task runner produces these public test records:
- preflight: native Delta schema rejection and worker-placement probe;
- incremental: 3 isolated invalid-batch cases, physical receipt replay,
  Silver business-state replay, reordered winners and actual historical read;
- release: 1 valid plus 4 rejected snapshot/review/code-binding cases.

TA-only expected rows and KPI answers are NOT included here. The independent
checks compare full values and provenance; a count alone is not an equivalence proof.

## Completed implementation checks

`python tests/run_tests.py` executes 20 tests, including release metadata refusal,
missing keys, version/hash binding, all revision tie-breakers, actual isolated
Delta merges and replay, exact Gold values/types and benchmark filtering. Three
source-trace regressions verify physical ordinals, reject invalid references and
show why the last R010001 row in a dictionary is not the revision-order winner.
Three release-review regressions reject candidate-derived expected values,
tampered catalog counts/units/retention and incomplete or mismatched lineage.
The executed result and any failures/skips are recorded in `evidence/implementation_tests.json`.

`tools/check_role_b.py --config config/team.json` records predictions, verifies
Silver/Gold modeling (`B01`–`B04`), conservation of counts/sums, observation-weighted
daily means versus naive site-mean averages, and cross-checks role C's stale and
same-time tied revision outcomes (`B_CHECK_C`). Its results are in
`evidence/role_B_prediction.json`, `role_B_gold_model.json`, and `role_B_check.json`.
`python -m unittest discover -s tests -p "test_role_b_*.py"` runs the standard-library
unit tests for Role B (`evidence/role_B_unit_tests.json`).

`tools/check_role_c.py --config config/team.json` records predictions, verifies
`M01`–`M08` incoming winner selection, conditional Silver merge, Bronze/Silver
replay idempotence, time travel, invalid-batch rejection, and cross-checks role D's
six A/B evaluations (`C_CHECK_D`). Its results are in `evidence/role_C_prediction.json`
and `role_C_check.json`.

`tools/check_role_d.py --config config/team.json` records a prediction, then checks
build_gold's weighted mean and observation denominator, the measured A/B answer,
scan metrics and operational/benchmark separation. It also reviews the technical
input and release checks for roles A/E. These records describe program execution.

`tools/check_role_a.py --config config/team.json --trusted-manifest
/instructor/trusted_lab3_inputs.json` records predictions, rechecks the runtime
and resolves three source ordinals against the actual pinned Delta snapshots.
Its results are in `evidence/role_A_check.json` and `role_A_source_trace.json`.

`tools/check_role_e.py --config config/team.json --trusted-manifest
/instructor/trusted_lab3_inputs.json` reviews actual pinned Silver/Gold, five
required gate cases, refusal edge cases, catalog, lineage and role A's input
identity. It writes `release_review.json` and never writes operational Delta data.
