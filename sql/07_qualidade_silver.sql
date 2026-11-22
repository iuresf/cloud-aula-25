-- Troque :run_id pelo run_id investigado no Athena.
SELECT category, dataset_type, business_key, occurrence_id, kept_occurrence_id,
       origin_uri, source_position, record_hash, detail, values_json
FROM qualidade_silver_anomalias
WHERE run_id = ':run_id'
ORDER BY dataset_type, category, business_key, source_position;
