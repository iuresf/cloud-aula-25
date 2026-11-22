-- O resumo é JSON operacional no Results. Esta consulta resume anomalias Parquet.
SELECT run_id, attempt_id, dataset_type, category, COUNT(*) AS ocorrencias
FROM qualidade_silver_anomalias
WHERE run_id = ':run_id'
GROUP BY run_id, attempt_id, dataset_type, category
ORDER BY dataset_type, category;
