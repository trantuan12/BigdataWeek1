-- Role A reproduces Role D's selected-row trace using stored source ordinals.
SELECT c.record_id,c.source_object,c.source_row,c.source_sha256,
       e.version_rank,e.unit,e.value_num,e.temperature_unrounded,c.temperature_c
FROM curated c JOIN evaluated e USING(source_object,source_row)
ORDER BY c.record_id LIMIT 1;

-- Quarantine trace includes the complete overlapping diagnosis and primary reason.
SELECT q.record_id,q.source_object,q.source_row,q.source_sha256,
       q.primary_reason,q.all_reasons,t.parse_ok
FROM quarantine q JOIN typed t USING(source_object,source_row)
ORDER BY q.source_object,q.source_row LIMIT 1;
