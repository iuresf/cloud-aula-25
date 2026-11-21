-- Q1: Qual receita liquida por mes, regiao, categoria e canal?
SELECT t.ano, t.mes, r.regiao, p.categoria, c.sk_canal AS canal,
       COUNT(DISTINCT f.pedido_id) AS pedidos, SUM(f.quantidade) AS unidades,
       SUM(f.valor_bruto) AS valor_bruto,
       SUM(f.receita_liquida) AS receita_liquida
FROM fato_itens_venda f
JOIN dim_tempo t ON f.sk_tempo=t.sk_tempo
JOIN dim_regiao r ON f.sk_regiao=r.sk_regiao
JOIN dim_produto p ON f.sk_produto=p.sk_produto
JOIN dim_canal c ON f.sk_canal=c.sk_canal
GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5;
