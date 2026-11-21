-- Q2: Quais eventos levaram a mudanca do pedido P1042? Historia do NEGOCIO, nao snapshots.
SELECT pedido_id, evento_pagamento_id, resultado, atualizado_em
FROM historico_pagamentos
WHERE pedido_id='P1042' ORDER BY atualizado_em;
-- Q3: Identificar carrinho/checkout e a sessao responsavel.
SELECT evento_id, sessao, evento, pedido_id, data, canal_id
FROM eventos WHERE pedido_id='P1042' ORDER BY data, evento_id;
