# Plano de atualização dos slides da Aula 25

Conceitos novos antes do walkthrough:
- Data Lake vs Lakehouse: capacidades efetivas e limites do Data Lake tradicional
- Atomicidade, consistência, isolamento e durabilidade na tabela Iceberg
- O que Iceberg acrescenta ao S3/Glue/Athena
- Critério para escolher simples Parquet vs Iceberg

Depois das referências:
- Problema de negócio -> necessidades -> dados e grão da estrela -> delta
- ACID e falha -> idempotência e time travel -> execução do laboratório
- Pré-requisitos -> Terraform -> Bronze -> Silver -> Gold -> Athena
- Demonstração de falha e reenvio -> evidências -> destroy

Correções: nomes antigos `03_lakehouse` e afirmações de que a estrela não é implementada devem ser substituídos pelo projeto `cloud-aula-25` real.

Execução atual: a orquestração deste projeto usa AWS Step Functions no lugar do Glue Workflow da aula 24. A figura em `figuras/fluxo-lakehouse.svg` mostra os recursos implantados e o encadeamento de metadados. Os resultados reais estão em `RESULTADOS_AWS.md`.
