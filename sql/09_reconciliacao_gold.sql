-- Consulte o URI reconciliation_uri do manifesto Gold no S3.
-- O JSON inclui expected_value, actual_value, difference e status de cada check.
SELECT etl_run_id, COUNT(*) AS itens, CAST(SUM(valor_bruto) AS DECIMAL(18,2)) AS valor_bruto,
       CAST(SUM(receita_liquida) AS DECIMAL(18,2)) AS receita_liquida
FROM fato_itens_venda
WHERE etl_run_id = ':run_id'
GROUP BY etl_run_id;
