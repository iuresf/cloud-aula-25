-- Q10: Conversao sem multiplicar sessoes pelos itens vendidos.
WITH v AS (
 SELECT sk_tempo, sk_canal, COUNT(*) AS sessoes, SUM(houve_checkout) AS sessoes_checkout
 FROM fato_sessoes GROUP BY 1,2
), p AS (
 SELECT sk_tempo, sk_canal, COUNT(DISTINCT CASE WHEN receita_liquida>0 THEN pedido_id END) AS pedidos_pagos
 FROM fato_itens_venda GROUP BY 1,2
)
SELECT t.data,c.sk_canal,v.sessoes,v.sessoes_checkout,COALESCE(p.pedidos_pagos,0) AS pedidos_pagos,
       ROUND(100.0*COALESCE(p.pedidos_pagos,0)/NULLIF(v.sessoes,0),2) AS conversao_pct
FROM v LEFT JOIN p ON v.sk_tempo=p.sk_tempo AND v.sk_canal=p.sk_canal
JOIN dim_tempo t ON t.sk_tempo=v.sk_tempo
JOIN dim_canal c ON c.sk_canal=v.sk_canal ORDER BY 1,2;
