-- Q4: Descobrir IDs reais dos snapshots Iceberg; nao use IDs inventados.
SELECT committed_at, snapshot_id, operation FROM "fato_itens_venda$snapshots" ORDER BY committed_at;
SELECT committed_at, snapshot_id, operation FROM "pagamentos$snapshots" ORDER BY committed_at;
-- Q5: Antes do estorno: SUBSTITUA <SNAPSHOT_INICIAL> pelo ID copiado acima.
-- SELECT pedido_id, item_id, resultado, receita_liquida
-- FROM fato_itens_venda FOR VERSION AS OF <SNAPSHOT_INICIAL> WHERE pedido_id='P1042';
-- Q6: Comparar o mesmo pedido com o snapshot atual:
SELECT pedido_id, item_id, resultado, receita_liquida
FROM fato_itens_venda WHERE pedido_id='P1042';
