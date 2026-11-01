"""Five lakehouse transformations implementing the fixed course contracts.
Use the function signatures exactly: src/run_lab.py imports this module.
All functions operate on Spark DataFrames except validate_release_binding.
"""

def build_gold(silver_df):
    """T2. Group retained observations by site and UTC event_date.
    Return exactly these columns:
      site, event_date, n_observations, sum_temperature_c,
      mean_temperature_c, min_temperature_c, max_temperature_c,
      n_late, late_rate, n_above_25, n_sensors.
    Use count of observations; SUM / COUNT for the mean; temperature > 25
    (strict inequality); COUNT(DISTINCT sensor_id); late flag as 0/1.
    Cast sums/min/max to decimal(18,2), mean/rate to decimal(18,6).
    No collect(), Pandas, Python UDF, or reference-sample join here.
    """
    from pyspark.sql import functions as F

    grouped = silver_df.groupBy('site', 'event_date').agg(
        F.count(F.lit(1)).cast('bigint').alias('n_observations'),
        F.sum('temperature_c').cast('decimal(18,2)').alias('sum_temperature_c'),
        F.min('temperature_c').cast('decimal(18,2)').alias('min_temperature_c'),
        F.max('temperature_c').cast('decimal(18,2)').alias('max_temperature_c'),
        F.sum(F.when(F.col('is_late'), 1).otherwise(0)).cast('bigint').alias('n_late'),
        F.sum(F.when(F.col('temperature_c') > F.lit(25), 1).otherwise(0))
            .cast('bigint').alias('n_above_25'),
        F.countDistinct('sensor_id').cast('bigint').alias('n_sensors'),
    )
    return grouped.select(
        'site', 'event_date', 'n_observations', 'sum_temperature_c',
        (F.col('sum_temperature_c') / F.col('n_observations'))
            .cast('decimal(18,6)').alias('mean_temperature_c'),
        'min_temperature_c', 'max_temperature_c', 'n_late',
        (F.col('n_late') / F.col('n_observations'))
            .cast('decimal(18,6)').alias('late_rate'),
        'n_above_25', 'n_sensors',
    )


def choose_revisions(changes_df):
    """T3. Select exactly one incoming row per record_id.
    Ordering: ingest_time_utc DESC, source_object ASC, source_row ASC.
    Use a deterministic Window and ROW_NUMBER. Return original columns;
    do not leak the helper rank column into a Delta table.
    """
    from pyspark.sql import Window, functions as F

    ordering = Window.partitionBy('record_id').orderBy(
        F.col('ingest_time_utc').desc(),
        F.col('source_object').asc(),
        F.col('source_row').asc(),
    )
    return (changes_df.withColumn('_revision_rank', F.row_number().over(ordering))
            .where(F.col('_revision_rank') == 1)
            .select(*changes_df.columns))


def merge_silver(spark, silver_path, winners_df):
    """T3. Merge strictly preceding revisions into Silver; return None.
    Match by record_id. Update only if the incoming row precedes the stored
    row in the SAME ordering as choose_revisions; insert missing business keys.
    Equal provenance and ingest timestamp is a no-op. Stale data must not
    replace a newer value. The runner validates the WHOLE batch first.
    """
    from delta.tables import DeltaTable

    precedes = '''
        s.ingest_time_utc > t.ingest_time_utc
        OR (s.ingest_time_utc = t.ingest_time_utc
            AND s.source_object < t.source_object)
        OR (s.ingest_time_utc = t.ingest_time_utc
            AND s.source_object = t.source_object
            AND s.source_row < t.source_row)
    '''
    (DeltaTable.forPath(spark, silver_path).alias('t')
     .merge(winners_df.alias('s'), 't.record_id = s.record_id')
     .whenMatchedUpdateAll(condition=precedes)
     .whenNotMatchedInsertAll()
     .execute())


def benchmark_query(df):
    """T4. Evaluate the same filtered aggregate for both layouts:
    filter event_date = DATE '2026-02-06'; group by site; return
    site, n_observations, sum_temperature_c (decimal(18,2)), n_late.
    Do not use replica_id in the GROUP BY. Do not call collect() here.
    The supplied runner materializes this bounded aggregate for timing.
    """
    from pyspark.sql import functions as F

    selected_day = df.filter(F.expr("event_date = DATE '2026-02-06'"))
    # Every replica contributes to the workload; the grouping key is only site.
    return (selected_day
            .groupBy('site').agg(
                F.count(F.lit(1)).cast('bigint').alias('n_observations'),
                F.sum('temperature_c').cast('decimal(18,2)').alias('sum_temperature_c'),
                F.sum(F.when(F.col('is_late'), 1).otherwise(0))
                    .cast('bigint').alias('n_late'),
            ).select('site', 'n_observations', 'sum_temperature_c', 'n_late'))


def validate_release_binding(draft, expected):
    """T5. Return (passed: bool, reasons: list[str]) for a release binding.
    Require nonempty author, reviewer, steward and reviewer != author.
    Bind handoff_logical_sha256, code_sha256, silver_path, silver_version,
    gold_path, gold_version and gold_source_silver_version to expected.
    Also require draft.gold_source_silver_version == draft.silver_version.
    Rejection reasons: REVIEW, METADATA, VERSION_BINDING, CODE_BINDING,
    or HANDOFF_BINDING. Missing keys must reject, never crash/pass.
    This is a course release check, not a digital signature or IAM control.
    """
    import re

    reasons = []
    if not isinstance(draft, dict) or not isinstance(expected, dict):
        return False, ['METADATA', 'VERSION_BINDING', 'CODE_BINDING', 'HANDOFF_BINDING']

    identities = [draft.get(k) for k in ('author', 'reviewer', 'steward')]
    if not all(isinstance(v, str) and v.strip() for v in identities):
        reasons.append('METADATA')
    author, reviewer = identities[:2]
    if (isinstance(author, str) and isinstance(reviewer, str)
            and author.strip().casefold() == reviewer.strip().casefold()):
        reasons.append('REVIEW')

    def matches_hash(key):
        value, wanted = draft.get(key), expected.get(key)
        return (isinstance(value, str)
                and re.fullmatch(r'[a-f0-9]{64}', value) is not None
                and value == wanted)

    if not matches_hash('handoff_logical_sha256'):
        reasons.append('HANDOFF_BINDING')
    if not matches_hash('code_sha256'):
        reasons.append('CODE_BINDING')

    version_ok = True
    for key in ('silver_path', 'gold_path'):
        value = draft.get(key)
        if not isinstance(value, str) or not value.strip() or value != expected.get(key):
            version_ok = False
    for key in ('silver_version', 'gold_version', 'gold_source_silver_version'):
        value, wanted = draft.get(key), expected.get(key)
        if (type(value) is not int or value < 0 or type(wanted) is not int
                or value != wanted):
            version_ok = False
    if draft.get('gold_source_silver_version') != draft.get('silver_version'):
        version_ok = False
    if not version_ok:
        reasons.append('VERSION_BINDING')
    return not reasons, reasons
