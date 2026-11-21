# Estado da validação — Aula 25

Em 20/11/2026, `terraform init`, `terraform validate`, compilação Python e testes offline passaram. A integração AWS Academy foi executada e documentada em `RESULTADOS_AWS.md`, com evidências JSON em `evidencias/`.

A implantação testou carga inicial, delta, reenvio, consultas dimensionais, time travel e falha controlada. O Lake Formation retornou `AccessDenied` e permaneceu desabilitado neste laboratório.

Em 21/11/2026, a revisão do rastreamento foi implantada com credenciais renovadas. `pytest`, compilação Python, `terraform fmt -check` e `terraform validate` passaram. Carga inicial, delta, reenvio, time travel, falha controlada e auditoria das sete tabelas Gold foram executados na AWS Academy. O rastreamento confirmou cada origem até `VersionId` e SHA-256 do Bronze e confrontou os ponteiros Iceberg com o Glue Catalog. Os 11 recursos foram destruídos; o estado Terraform e as listagens AWS do projeto ficaram vazios. Veja `RESULTADOS_AWS.md` e `evidencias/20261121T033113-rastreabilidade-gold.json`.

## Evolução recuperável — 21/11/2026

Nesta revisão local, `pytest -q` passou com 22 testes, `terraform validate -no-color` passou e os scripts Python foram compilados. A versão recuperável ainda não foi implantada na AWS Academy; as evidências AWS acima documentam somente a versão anterior.

`terraform plan -input=false -no-color` consultou a conta AWS e concluiu: **73 to add, 0 to change, 0 to destroy**. Nenhuma alteração foi aplicada.
