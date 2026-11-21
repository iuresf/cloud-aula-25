-- Q7: O MERGE foi idempotente? Reexecute "python runner.py replay" e cheque.
SELECT COUNT(*) AS linhas, COUNT(DISTINCT item_id) AS chaves,
       CAST(SUM(receita_liquida) AS DECIMAL(14,2)) AS receita
FROM fato_itens_venda;
-- Q8: Checar historico de eventos de pagamento sem duplicacao.
SELECT COUNT(*) AS eventos, COUNT(DISTINCT evento_pagamento_id) AS ids_unicos
FROM historico_pagamentos;
-- Q9: Gold tem commits por tabela: consulte o historico de cada uma separadamente.
SELECT committed_at, snapshot_id FROM "dim_cliente$snapshots" ORDER BY committed_at;
SELECT committed_at, snapshot_id FROM "fato_itens_venda$snapshots" ORDER BY committed_at;
