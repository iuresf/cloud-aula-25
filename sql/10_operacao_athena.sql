-- Execute um statement por vez no Athena, após selecionar o database a25_iceberg
-- e o workgroup a25-iceberg.

-- 1. Esquema estrela: receita por tempo, região, categoria e canal.
SELECT
  t.ano,
  t.mes,
  r.regiao,
  p.categoria,
  c.sk_canal AS canal,
  COUNT(DISTINCT f.pedido_id) AS pedidos,
  SUM(f.quantidade) AS unidades,
  CAST(SUM(f.valor_bruto) AS DECIMAL(18,2)) AS valor_bruto,
  CAST(SUM(f.receita_liquida) AS DECIMAL(18,2)) AS receita_liquida
FROM fato_itens_venda f
JOIN dim_tempo t ON t.sk_tempo = f.sk_tempo
JOIN dim_regiao r ON r.sk_regiao = f.sk_regiao
JOIN dim_produto p ON p.sk_produto = f.sk_produto
JOIN dim_canal c ON c.sk_canal = f.sk_canal
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5;

-- 2. Conferência de grão das duas fatos: não junte sessões e itens antes de agregar.
SELECT 'fato_itens_venda' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT item_id) AS chaves_unicas
FROM fato_itens_venda
UNION ALL
SELECT 'fato_sessoes', COUNT(*), COUNT(DISTINCT CONCAT(sessao, '|', CAST(data AS VARCHAR), '|', sk_canal))
FROM fato_sessoes;

-- 3. Snapshots Iceberg atuais da fato principal.
SELECT committed_at, snapshot_id, parent_id, operation, summary
FROM "fato_itens_venda$snapshots"
ORDER BY committed_at;

-- 4. Time travel: substitua pelo snapshot inicial exibido na consulta anterior.
-- SELECT pedido_id, item_id, resultado, receita_liquida
-- FROM fato_itens_venda FOR VERSION AS OF <SNAPSHOT_INICIAL>
-- WHERE pedido_id = 'P1042'
-- ORDER BY item_id;

-- 5. Comparação com o estado atual depois do delta.
SELECT pedido_id, item_id, resultado, receita_liquida, etl_run_id
FROM fato_itens_venda
WHERE pedido_id = 'P1042'
ORDER BY item_id;

-- 6. Todas as anomalias Silver persistidas. DUPLICATE_IDENTICAL ocorre no lote inicial.
SELECT run_id, attempt_id, dataset_type, category, business_key,
       source_position, origin_uri, kept_occurrence_id, detail
FROM qualidade_silver_anomalias
ORDER BY detected_at DESC, dataset_type, source_position;

-- 7. Somente erros que bloqueiam a Silver. KEY_CONFLICT e MISSING_REFERENCE
-- também são persistidos antes da falha do job.
SELECT run_id, attempt_id, dataset_type, category, business_key,
       source_position, detail, values_json
FROM qualidade_silver_anomalias
WHERE category IN ('NULL_REQUIRED', 'INVALID_TYPE', 'INVALID_DOMAIN',
                   'MISSING_REFERENCE', 'KEY_CONFLICT')
ORDER BY detected_at DESC, dataset_type, source_position;

-- 8. Contagem de duplicações e falhas por execução.
SELECT run_id, attempt_id, dataset_type, category, COUNT(*) AS ocorrencias
FROM qualidade_silver_anomalias
GROUP BY 1,2,3,4
ORDER BY run_id DESC, dataset_type, category;

-- 9. Reconciliação observável pela Gold: totais e chaves da execução publicada.
SELECT etl_run_id,
       COUNT(*) AS itens,
       COUNT(DISTINCT item_id) AS itens_unicos,
       CAST(SUM(valor_bruto) AS DECIMAL(18,2)) AS valor_bruto,
       CAST(SUM(receita_liquida) AS DECIMAL(18,2)) AS receita_liquida
FROM fato_itens_venda
GROUP BY etl_run_id
ORDER BY etl_run_id DESC;
