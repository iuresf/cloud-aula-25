# Validação AWS Academy — Aula 25

Execução em 20/11/2026 (America/Fortaleza), conta `795636653176`, região `us-east-1`, identidade `assumed-role/voclabs/user5422286=Testar_aluno` e role de jobs/orquestração `arn:aws:iam::795636653176:role/LabRole`.

## Recursos observados

Terraform criou quatro buckets S3 (Bronze, Silver, Gold e Results), o database Glue `a25_iceberg`, dois jobs Glue 4.0, a máquina Step Functions `a25-iceberg-etl` e o workgroup Athena engine 3. O Glue Catalog mostrou 14 tabelas: sete Silver e, em Gold, cinco dimensões mais `fato_itens_venda` e `fato_sessoes`.

O Lake Formation não foi ativado. `GetDataLakeSettings` e `ListResources` retornaram `AccessDeniedException` para a identidade do laboratório. O módulo Terraform `governance` permite habilitá-lo em conta com permissões adequadas.

## Ingestão corporativa — teste de 21/11/2026

O Terraform criou Cognito, API Gateway REST com WAF, três Lambdas, SQS e DLQs, DynamoDB, CloudTrail, dashboard, workgroups Athena, manutenção Iceberg desabilitada e a orquestração existente. A AWS Academy negou `iam:CreateRole`; por isso as Lambdas usaram a `LabRole` preexistente. Em uma conta corporativa, o módulo aceita uma role dedicada de menor privilégio.

O fluxo OAuth2 `client_credentials` obteve token Cognito e `POST /v1/ingestions` retornou `201`, `ingestion_id`, URL pré-assinada, chave Bronze e cabeçalho `x-amz-checksum-sha256`. O upload de `pedidos_lote_001.csv` produziu a seguinte trilha antes da execução ETL:

- `ingestion_id`: `7cd2dc8e-f18a-4dd7-9f25-03cabd28b3b2`.
- origem versionada: `a25-iceberg-bronze-795636653176-us-east-1/ingestion/erp/2026/11/21/7cd2dc8e-f18a-4dd7-9f25-03cabd28b3b2/pedidos_lote_001.csv/P5pqPlnEpeqT1RJxrJkgPO0GxgyXlP_i`.
- manifesto: `results/manifestos/bronze/ingestion-7cd2dc8e-f18a-4dd7-9f25-03cabd28b3b2.json`.
- ledger: `ORCHESTRATED`, com ARN da execução Step Functions `ing-7cd2dc8e-f18a-4dd7-9f25-03cabd28b3b2`.

A execução foi interrompida antes do Gold porque o teste enviou somente um dataset; o job dimensional exige o conjunto completo de fontes. A infraestrutura foi removida após o teste.

## Validação posterior do fluxo de erro de ingestão

Após a avaliação da implantação, o contrato de envio foi reforçado com `size_bytes` e a validação de chegada passou a distinguir erro permanente de falha operacional. As massas versionadas em `dados/erros/` simulam `expected-size` ausente, tamanho divergente e SHA-256 divergente. Em cada cenário, o teste local envia um evento S3 encapsulado em SQS à Lambda `receipt`, confirma a cópia à Quarantine, o estado `REJECTED` e a razão no DynamoDB, sem devolver a mensagem à fila. A indisponibilidade de SQS e da Step Functions é testada como retentativa, para que a política das DLQs possa atuar.

Esta validação foi executada contra o código-fonte das Lambdas que o Terraform empacota, sem recriar a infraestrutura AWS Academy já destruída. Uma nova implantação deve executar `pytest tests/test_ingestion_contract.py -q` antes do `terraform apply` e repetir os três envios pela API para registrar evidências AWS atuais.

### Matriz de evidências

| Verificação | Executada na AWS Academy | Evidência |
|---|---|---|
| `terraform apply` e recursos do lakehouse | Sim | Glue Catalog, Athena, Step Functions e buckets descritos neste relatório |
| Carga válida Bronze → Silver → Gold | Sim | totais Athena, snapshots e manifestos Iceberg |
| API Cognito → URL pré-assinada → Bronze → ledger | Sim | `ingestion_id` `7cd2dc8e-f18a-4dd7-9f25-03cabd28b3b2`, `VersionId`, manifesto e estado `ORCHESTRATED` |
| Três arquivos atuais de erro de ingestão | Não | testes locais S3/SQS simulados, com Quarantine e ledger stub |
| Logs CloudWatch, objetos Quarantine e DLQs desses três erros | Não | não havia infraestrutura ativa após o `terraform destroy` |

Portanto, não há como afirmar que os três arquivos de erro apareceram nos buckets, DLQs ou logs da AWS Academy: eles ainda precisam de uma nova implantação curta para essa evidência. O comportamento esperado e validado em teste local é `REJECTED` + objeto Quarantine para erro permanente; falha operacional retorna a mensagem para a fila até a DLQ.

## Respostas observadas no Athena

| Cenário | Itens | Receita total | Receita de P1042 |
|---|---:|---:|---:|
| Carga inicial | 5 | R$ 249,70 | R$ 129,90 |
| Delta com estorno | 5 | R$ 198,80 | R$ 0,00 |
| Reenvio do delta | 5 chaves distintas | R$ 198,80 | R$ 0,00 |
| Falha controlada | 5 | R$ 198,80 | R$ 0,00 |

- O primeiro snapshot da `fato_itens_venda` foi `9221215859180835813`. No time travel, I01 = R$ 59,90 e I02 = R$ 70,00, ambos com pagamento aprovado.
- Após o delta, o snapshot foi `4015876205134453433`. No estado atual, I01 e I02 têm `resultado=estornado` e receita zero.
- O reenvio criou o snapshot `2292387254169245383`; manteve cinco chaves e R$ 198,80.
- A falha por pagamento inválido terminou em `FAILED`, com `ValueError: Contrato invalido: pagamentos`. Nenhum novo snapshot apareceu em pagamentos, histórico de pagamentos ou fato após a falha.
- `historico_pagamentos` contém PG001 aprovado em 20/10/2026 e PG005 estornado em 22/10/2026 para P1042. Esses eventos de negócio são distintos dos snapshots físicos Iceberg.
- A estrela distribuiu receita por mês, região, categoria e canal. Após o estorno, Nordeste/Acessórios/app = R$ 79,00 e Sudeste/Vestuário/app = R$ 119,80; os segmentos de P1042 ficaram com R$ 0,00.
- A conversão veio da fato de sessões agregada antes da junção com pedidos: em 20/10, app = 100%, site = 0%; em 21/10, app = 100%; em 22/10, site = 0%.

## Rastreabilidade

As evidências JSON em `evidencias/` contêm consultas Athena, IDs de execução da Step Functions e o manifesto Bronze. A cópia em `evidencias/aws-manifestos/` guarda os manifestos operacionais antes da destruição: objetos Bronze com chave, `VersionId` e SHA-256; tabelas Silver/Gold com `metadata_location`; e `run_id` ligando Silver à execução Gold. Os arquivos `.metadata.json` e `.avro` próprios do Iceberg residiam nos buckets Silver/Gold durante a implantação.

## Limites técnicos

O `MERGE` foi idempotente quanto ao estado lógico, mas cada execução publicou novos snapshots por tabela. A execução Silver → Gold pela Step Functions não constitui uma transação ACID global. A Bronze é relida integralmente em cada execução; o processamento é incremental lógico por chave, não CDC físico. O time travel depende da retenção dos snapshots.

## Encerramento

Após copiar as evidências, `terraform destroy` concluiu com **0 recursos restantes no estado**. A AWS CLI retornou listas vazias para buckets com prefixo `a25-iceberg-`, database `a25_iceberg`, máquina Step Functions `a25-iceberg-etl` e workgroup Athena `a25-iceberg`. Os jobs Glue foram removidos pelo Terraform. A primeira tentativa de destruição falhou porque o script usava `aws s3api rb`; corrigi para `aws s3api delete-bucket` e a segunda tentativa terminou com sucesso.

## Nova validação da linhagem Gold — 21/11/2026

Com novas credenciais temporárias válidas, o Terraform criou novamente os **11 recursos padrão** na mesma conta e região. O código Gold agora registra `etl_run_id` em todas as linhas, as origens agregadas nas dimensões `tempo`, `canal` e `regiao`, a origem do produto na fato de vendas e os arquivos dos eventos na fato de sessões.

| Etapa | Step Functions / Glue | Itens | Receita | Linhas Gold auditadas | Objetos Bronze conferidos |
|---|---|---:|---:|---:|---:|
| Inicial | `SUCCEEDED` | 5 | R$ 249,70 | 21 | 6 |
| Delta | `SUCCEEDED` | 5 | R$ 198,80 | 23 | 8 |
| Reenvio | `SUCCEEDED` | 5 | R$ 198,80 | 23 | 8 |
| Pagamento inválido | `FAILED` no Silver, como esperado | 5 | R$ 198,80 | 23, sem alteração | 8 |

O comando `runner.py trace-gold` consultou **todas as linhas das sete tabelas Gold** via Athena. Para cada origem registrada, localizou a chave no manifesto Silver da execução, leu o objeto Bronze pelo `VersionId` e recalculou o SHA-256. Também confirmou que o `metadata_location` das 14 tabelas Iceberg no Glue Catalog coincide com os manifestos Silver e Gold. A evidência completa, com chaves de cada linha, `run_id`, URIs, versões, hashes e conteúdo dos manifestos, está em [20261121T033113-rastreabilidade-gold.json](evidencias/20261121T033113-rastreabilidade-gold.json).

Exemplo: o item `I01` no Gold após o reenvio tem `etl_run_id=327ea69dfac9483988b444551d4cb8b9`; sua origem de pagamento é `pagamentos/lote_002.jsonl`. O manifesto Silver liga essa chave a um `VersionId` e SHA-256 verificados diretamente no S3. Os arquivos de item, pedido, cliente e produto também constam na mesma linha. As consultas Athena sobre P1042 mostraram `I01=R$ 59,90` e `I02=R$ 70,00` no primeiro snapshot; no estado atual, ambos têm receita zero e resultado `estornado`.

O reenvio gerou novos `VersionId` para os arquivos do delta, enquanto preservou cinco itens e R$ 198,80. A falha de contrato (`ValueError: Contrato invalido: pagamentos`) ocorreu antes de novos commits: os snapshots de pagamentos, histórico e fato permaneceram com os mesmos IDs do reenvio, e a auditoria Gold retornou o mesmo `run_id`.

O Lake Formation continuou indisponível para a identidade do laboratório: `lakeformation:GetDataLakeSettings` retornou `AccessDeniedException`. O módulo opcional permaneceu desligado.

Após os testes, `terraform apply` do plano de destruição removeu os 11 recursos. `terraform state list` retornou vazio, e as consultas AWS não encontraram buckets com prefixo `a25-iceberg-`, database `a25_iceberg`, jobs `a25-iceberg-*`, máquina `a25-iceberg-etl` ou workgroup `a25-iceberg`.
