-- Chave de negócio -> arquivos Bronze usados na linha Gold.
SELECT item_id, pedido_id, etl_run_id, origem_item, origem_pedido, origem_cliente,
       origem_produto, origem_pagamento
FROM fato_itens_venda
WHERE pedido_id='P1042'
ORDER BY item_id;

-- Cada URI aponta para uma chave Bronze. Use etl_run_id para localizar o manifesto
-- Gold e o manifesto Silver da mesma execução. O Silver contém VersionId e SHA-256.
-- python3 runner.py trace-gold confere todas as linhas das 7 tabelas Gold e os hashes.
