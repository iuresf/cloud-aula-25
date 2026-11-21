#!/usr/bin/env python3
"""Gera o inventário visual completo dos recursos declarados pelo Terraform."""
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parent
W, COL, GAP, X0 = 2520, 760, 45, 70
parts = []


def add(value):
    parts.append(value)


def text(x, y, value, size=18, weight=400, color="#243b53"):
    add(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}">{escape(value)}</text>')


def rect(x, y, w, h, fill="#fff", stroke="#cbd8e6", radius=16, dash=""):
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="2" stroke-dasharray="{dash}"/>')


def arrow(x1, y1, x2, y2, label="", color="#177e9d", dash=""):
    add(f'<path d="M{x1} {y1} L{x2} {y2}" fill="none" stroke="{color}" stroke-width="3" stroke-dasharray="{dash}" marker-end="url(#arrow)"/>')
    if label:
        text((x1 + x2) / 2, (y1 + y2) / 2 - 8, label, 14, 600, color)


def flow_node(x, y, w, title, detail, fill="#ffffff", stroke="#84a8c8"):
    rect(x, y, w, 82, fill, stroke, 14)
    text(x + 15, y + 32, title, 17, 750)
    text(x + 15, y + 58, detail, 14, 450, "#506983")


def card(x, y, service, address, title, lines, status="TF", fill="#fff"):
    rect(x, y, COL, 138, fill, "#9fb7ce" if status == "TF" else "#b7afcf", 16, "7 5" if status == "OPT" else "")
    text(x + 20, y + 29, service, 15, 700, "#54708b")
    text(x + 20, y + 57, title, 21, 750)
    rect(x + COL - 77, y + 15, 56, 27, "#daf2e4" if status == "TF" else "#eeeaf7", "none", 14)
    text(x + COL - 63, y + 35, status, 13, 750, "#245947" if status == "TF" else "#675a83")
    text(x + 20, y + 82, address, 14, 650, "#38546f")
    for index, line in enumerate(lines):
        text(x + 20, y + 108 + index * 21, line, 15, 400, "#506983")


sections = [
    ("ROOT E ARMAZENAMENTO", [
        ("Terraform / AWS", "data.aws_caller_identity.me", "Identidade da conta", ["account_id aplicado aos nomes dos buckets"]),
        ("Terraform data + S3", "terraform_data.bucket[bronze]", "Bucket Bronze", ["buckets.sh create/destroy", "versionamento de objetos"]),
        ("Terraform data + S3", "terraform_data.bucket[silver]", "Bucket Silver", ["warehouse Iceberg Silver", "buckets.sh create/destroy"]),
        ("Terraform data + S3", "terraform_data.bucket[gold]", "Bucket Gold", ["warehouse Iceberg Gold", "buckets.sh create/destroy"]),
        ("Terraform data + S3", "terraform_data.bucket[results]", "Bucket Results", ["scripts, manifestos e resultados Athena"]),
        ("Terraform data + S3", "terraform_data.bucket[quarantine]", "Bucket Quarantine", ["arquivos que violam contrato ou hash"]),
        ("Terraform data + S3", "terraform_data.bucket[audit]", "Bucket Audit", ["destino do CloudTrail", "versionamento de objetos"]),
        ("S3 lifecycle", "aws_s3_bucket_lifecycle_configuration.layers[bronze]", "Ciclo Bronze", ["aborta multipart incompleto em 7 dias"]),
        ("S3 lifecycle", "aws_s3_bucket_lifecycle_configuration.layers[silver] + [gold]", "Ciclo Silver e Gold", ["aborta multipart incompleto em 7 dias"]),
        ("S3 lifecycle", "aws_s3_bucket_lifecycle_configuration.layers[results] + [quarantine] + [audit]", "Ciclo Results, Quarantine e Audit", ["aborta multipart incompleto em 7 dias"]),
        ("AWS Glue Data Catalog", "aws_glue_catalog_database.this", "Database a25_iceberg", ["catálogo de tabelas e schemas Iceberg"]),
    ]),
    ("ETL, CATÁLOGO E CONSUMO", [
        ("S3 object", "aws_s3_object.script[silver]", "Script Silver", ["results/jobs/silver.py"]),
        ("S3 object", "aws_s3_object.script[gold]", "Script Gold", ["results/jobs/gold.py"]),
        ("AWS Glue", "aws_glue_job.job[silver]", "Job a25-iceberg-silver", ["Glue 4 / Iceberg / 2 workers", "MERGE e validação Bronze → Silver"]),
        ("AWS Glue", "aws_glue_job.job[gold]", "Job a25-iceberg-gold", ["dimensões e fatos", "MERGE Silver → Gold"]),
        ("AWS Step Functions", "aws_sfn_state_machine.etl", "State machine a25-iceberg-etl", ["integração Glue .sync", "Silver → Gold"]),
        ("Amazon Athena", "aws_athena_workgroup.this", "Workgroup técnico", ["engine 3; saída results/athena/"]),
        ("Amazon Athena", "aws_athena_workgroup.consumer", "Workgroup consumidor", ["engine 3; saída results/athena-consumer/"]),
        ("Athena named query", "aws_athena_named_query.vendas_mensais_v1", "Consulta certificada", ["vendas mensais por região e canal"]),
    ]),
    ("INGESTÃO: ARQUIVOS, IDENTIDADE, FILAS E LAMBDAS", [
        ("Archive provider", "data.archive_file.lambda[api]", "Pacote Lambda API", ["zip de lambdas/api_ingestion.py"]),
        ("Archive provider", "data.archive_file.lambda[receipt]", "Pacote Lambda Receipt", ["zip de lambdas/receipt.py"]),
        ("Archive provider", "data.archive_file.lambda[orchestrator]", "Pacote Lambda Orchestrator", ["zip de lambdas/orchestrator.py"]),
        ("Amazon DynamoDB", "aws_dynamodb_table.ingestion", "Ledger de ingestão", ["PK object_version; PITR", "estado RECEIVED / ORCHESTRATED"]),
        ("Amazon Cognito", "aws_cognito_user_pool.ingestors", "User pool", ["identidade dos produtores"]),
        ("Amazon Cognito", "aws_cognito_resource_server.ingestion", "Resource server", ["scope ingestion/write"]),
        ("Amazon Cognito", "aws_cognito_user_pool_domain.ingestors", "Domínio OAuth2", ["endpoint client_credentials"]),
        ("Amazon Cognito", "aws_cognito_user_pool_client.machine", "Machine client", ["segredo e fluxo client_credentials"]),
        ("Amazon SQS", "aws_sqs_queue.object_events", "Fila object-events", ["eventos S3 Bronze; visibility 180 s"]),
        ("Amazon SQS", "aws_sqs_queue.object_dlq", "DLQ object-events", ["retenção 14 dias; 5 tentativas"]),
        ("Amazon SQS", "aws_sqs_queue.ready", "Fila ready", ["entrega para orquestração"]),
        ("Amazon SQS", "aws_sqs_queue.ready_dlq", "DLQ ready", ["retenção 14 dias; 5 tentativas"]),
        ("SQS policy", "aws_sqs_queue_policy.object_events", "Política S3 → SQS", ["permite s3.amazonaws.com SendMessage"]),
        ("AWS Lambda", "aws_lambda_function.api", "Lambda API ingestion", ["valida contrato e cria URL pré-assinada"]),
        ("AWS Lambda", "aws_lambda_function.receipt", "Lambda Receipt", ["recalcula SHA-256; manifesto; quarentena"]),
        ("AWS Lambda", "aws_lambda_function.orchestrator", "Lambda Orchestrator", ["inicia Step Functions por ingestion_id"]),
        ("Lambda mapping", "aws_lambda_event_source_mapping.receipt", "SQS object-events → Receipt", ["batch 10; falhas parciais"]),
        ("Lambda mapping", "aws_lambda_event_source_mapping.orchestrator", "SQS ready → Orchestrator", ["batch 10; falhas parciais"]),
    ]),
    ("BORDA DA API E EVENTOS S3", [
        ("API Gateway REST", "aws_api_gateway_rest_api.ingestion", "API a25-iceberg-ingestion", ["borda pública protegida"]),
        ("API Gateway REST", "aws_api_gateway_resource.ingestions", "Recurso /ingestions", ["rota de criação de sessão"]),
        ("API Gateway REST", "aws_api_gateway_authorizer.cognito", "Authorizer Cognito", ["valida access token"]),
        ("API Gateway REST", "aws_api_gateway_method.ingestion", "Método POST", ["scope obrigatório ingestion/write"]),
        ("API Gateway REST", "aws_api_gateway_integration.ingestion", "Integração proxy", ["POST → Lambda API"]),
        ("API Gateway REST", "aws_api_gateway_deployment.ingestion", "Deployment", ["redeploy por hash da rota"]),
        ("API Gateway REST", "aws_api_gateway_stage.ingestion", "Stage v1", ["X-Ray habilitado"]),
        ("API Gateway REST", "aws_api_gateway_method_settings.ingestion", "Throttling POST", ["25 req/s; burst 50; métricas"]),
        ("AWS WAFv2", "aws_wafv2_web_acl.ingestion", "Web ACL", ["rate limit por IP", "AWSManagedRulesCommonRuleSet"]),
        ("WAF association", "aws_wafv2_web_acl_association.ingestion", "WAF → stage v1", ["associação ao API Gateway REST"]),
        ("Lambda permission", "aws_lambda_permission.api_gateway", "Permissão API Gateway", ["API Gateway invoca Lambda API"]),
        ("S3 notification", "aws_s3_bucket_notification.bronze", "Evento Bronze", ["prefixo ingestion/ → SQS object-events"]),
    ]),
    ("OBSERVABILIDADE, AUDITORIA E MANUTENÇÃO", [
        ("CloudWatch Logs", "aws_cloudwatch_log_group.ingestion[api]", "Log group API", ["/aws/lambda/a25-iceberg-api-ingestion", "retenção 30 dias"]),
        ("CloudWatch Logs", "aws_cloudwatch_log_group.ingestion[receipt]", "Log group Receipt", ["/aws/lambda/a25-iceberg-receipt", "retenção 30 dias"]),
        ("CloudWatch Logs", "aws_cloudwatch_log_group.ingestion[orchestrator]", "Log group Orchestrator", ["/aws/lambda/a25-iceberg-orchestrator", "retenção 30 dias"]),
        ("CloudWatch Alarm", "aws_cloudwatch_metric_alarm.dlq_messages[object_dlq]", "Alarme object-events DLQ", ["ApproximateNumberOfMessagesVisible > 0"]),
        ("CloudWatch Alarm", "aws_cloudwatch_metric_alarm.dlq_messages[ready_dlq]", "Alarme ready DLQ", ["ApproximateNumberOfMessagesVisible > 0"]),
        ("CloudWatch Alarm", "aws_cloudwatch_metric_alarm.lambda_errors[api] + [receipt] + [orchestrator]", "Alarmes Lambda Errors", ["um alarme por Lambda; Errors > 0"]),
        ("CloudWatch", "aws_cloudwatch_dashboard.lakehouse", "Dashboard lakehouse", ["métricas de erro das 3 Lambdas"]),
        ("S3 bucket policy", "aws_s3_bucket_policy.audit", "Política do Audit", ["CloudTrail GetBucketAcl / PutObject"]),
        ("AWS CloudTrail", "aws_cloudtrail.lakehouse", "Trilha a25-iceberg-audit", ["eventos gerenciais + objetos Bronze", "validação de arquivos de log"]),
        ("S3 object", "aws_s3_object.script", "Script de manutenção", ["results/jobs/maintenance.py"]),
        ("AWS Glue", "aws_glue_job.iceberg", "Job manutenção Iceberg", ["compacta; expira snapshots; remove órfãos"]),
        ("Amazon EventBridge", "aws_cloudwatch_event_rule.weekly", "Regra semanal", ["cron domingo 04:00 UTC; inicialmente DISABLED"]),
        ("IAM", "aws_iam_role.scheduler", "Role EventBridge", ["criada somente se maintenance enabled"], "OPT", "#f5f2fb"),
        ("IAM", "aws_iam_role_policy.scheduler", "Política do agendador", ["glue:StartJobRun; condicional"], "OPT", "#f5f2fb"),
        ("EventBridge target", "aws_cloudwatch_event_target.weekly", "Alvo Glue manutenção", ["usa role scheduler; condicional"], "OPT", "#f5f2fb"),
    ]),
    ("GOVERNANÇA LAKE FORMATION — MÓDULO OPCIONAL", [
        ("Lake Formation", "aws_lakeformation_resource.layer[silver]", "Location Silver", ["S3 Silver registrado no Lake Formation"], "OPT", "#f5f2fb"),
        ("Lake Formation", "aws_lakeformation_resource.layer[gold]", "Location Gold", ["S3 Gold registrado no Lake Formation"], "OPT", "#f5f2fb"),
        ("Lake Formation", "aws_lakeformation_permissions.database", "Permissão de database", ["ALL para a role configurada"], "OPT", "#f5f2fb"),
        ("Lake Formation", "aws_lakeformation_permissions.table", "Permissão de tabelas", ["ALL no wildcard de tabelas"], "OPT", "#f5f2fb"),
        ("Lake Formation LF-Tag", "aws_lakeformation_lf_tag.classification", "Tag classification", ["valores internal, restricted"], "OPT", "#f5f2fb"),
        ("Lake Formation LF-Tag", "aws_lakeformation_lf_tag.domain", "Tag domain", ["valor commerce"], "OPT", "#f5f2fb"),
    ]),
]

height = 930 + sum(90 + ((len(items) + 2) // 3) * 160 for _, items in sections)
add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" viewBox="0 0 {W} {height}" role="img" aria-labelledby="title desc">')
add('<title id="title">Inventário completo do Terraform do lakehouse da aula 25</title>')
add('<desc id="desc">Fluxo completo de ingestão, caminhos de erro, DLQs, quarentena e inventário de cada recurso ou expansão for_each declarada no Terraform.</desc>')
add('<defs><marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto"><path d="M1 1 L9 5 L1 9" fill="none" stroke="#177e9d" stroke-width="2"/></marker></defs>')
rect(0, 0, W, height, "#f4f8fc", "none", 0)
text(70, 70, "Aula 25 · Inventário completo do Terraform Lakehouse", 39, 750)
text(70, 110, "Cada cartão mapeia diretamente um recurso declarado; expansões for_each informam todas as instâncias.", 19, 400, "#557087")
rect(70, 140, 2380, 650, "#e8f1fa", "#c5d7e8", 18)
text(95, 180, "FLUXO OPERACIONAL — CAMINHO PRINCIPAL E DESVIOS", 19, 750, "#51708c")
text(95, 210, "Seta azul: processamento normal. Seta vermelha tracejada: rejeição ou erro. Seta laranja tracejada: retentativa e retenção em DLQ.", 16, 450, "#557087")

flow_node(95, 260, 230, "Produtor externo", "client_credentials")
flow_node(370, 260, 230, "Cognito", "token + scope write", "#ffffff", "#84a8c8")
flow_node(645, 260, 270, "API Gateway REST + WAF", "autoriza, limita e bloqueia", "#ffffff", "#84a8c8")
flow_node(960, 260, 230, "Lambda API", "valida metadados", "#ffffff", "#84a8c8")
flow_node(1235, 260, 260, "S3 Bronze /ingestion", "original, VersionId, checksum", "#ffffff", "#84a8c8")
flow_node(1540, 260, 230, "SQS object-events", "buffer e 5 tentativas", "#ffffff", "#84a8c8")
flow_node(1815, 260, 230, "Lambda Receipt", "hash, manifesto, ledger", "#ffffff", "#84a8c8")
flow_node(2090, 260, 270, "SQS ready", "evento pronto para ETL", "#ffffff", "#84a8c8")

arrow(325, 301, 365, 301, "token")
arrow(600, 301, 640, 301, "Bearer")
arrow(915, 301, 955, 301, "invoke")
arrow(1190, 301, 1230, 301, "URL pré-assinada")
arrow(1495, 301, 1535, 301, "ObjectCreated")
arrow(1770, 301, 1810, 301, "batch")
arrow(2045, 301, 2085, 301, "manifesto")

flow_node(95, 445, 260, "Resposta 4xx", "token, WAF ou contrato API", "#fff2f2", "#d77878")
flow_node(420, 445, 280, "S3 Quarantine", "metadados/hash inválidos", "#fff2f2", "#d77878")
flow_node(765, 445, 280, "DLQ object-events", "Receipt falha após 5 tentativas", "#fff8e8", "#d99a35")
flow_node(1110, 445, 280, "DLQ ready", "Orchestrator falha após 5 tentativas", "#fff8e8", "#d99a35")
flow_node(1455, 445, 280, "CloudWatch alarms", "Errors Lambda e mensagens DLQ", "#fff8e8", "#d99a35")
flow_node(1800, 445, 280, "DynamoDB ledger", "RECEIVED / ORCHESTRATED", "#ffffff", "#84a8c8")
flow_node(2145, 445, 215, "CloudTrail", "audita Bronze", "#ffffff", "#84a8c8")

arrow(780, 342, 225, 440, "nega", "#c84d4d", "8 7")
arrow(1075, 342, 560, 440, "falha contrato/hash", "#c84d4d", "8 7")
arrow(1655, 342, 905, 440, "falha repetida", "#d98d21", "8 7")
arrow(2190, 342, 1250, 440, "falha repetida", "#d98d21", "8 7")
arrow(1925, 342, 1940, 440, "grava", "#177e9d")
arrow(1365, 342, 2250, 440, "data events", "#177e9d", "5 6")
arrow(905, 527, 1570, 527, "alarme", "#d98d21", "5 6")
arrow(1250, 527, 1570, 527, "alarme", "#d98d21", "5 6")

flow_node(355, 640, 260, "Lambda Orchestrator", "nome idempotente ingestion_id", "#ffffff", "#84a8c8")
flow_node(700, 640, 280, "Step Functions", "Silver .sync → Gold .sync", "#ffffff", "#84a8c8")
flow_node(1065, 640, 250, "Glue Silver", "MERGE por chave", "#ffffff", "#84a8c8")
flow_node(1400, 640, 250, "Glue Gold", "dimensões e fatos", "#ffffff", "#84a8c8")
flow_node(1735, 640, 250, "S3 Gold + Catalog", "snapshots Iceberg", "#ffffff", "#84a8c8")
flow_node(2070, 640, 250, "Athena Consumer", "consulta certificada", "#ffffff", "#84a8c8")
arrow(2225, 342, 485, 635, "SQS mapping")
arrow(615, 681, 695, 681, "StartExecution")
arrow(980, 681, 1060, 681, "run Silver")
arrow(1315, 681, 1395, 681, "run Gold")
arrow(1650, 681, 1730, 681, "commit")
arrow(1985, 681, 2065, 681, "consulta")
text(95, 760, "Reprocessamento: mensagens nas DLQs devem ser investigadas e reenviadas manualmente após a causa ser corrigida. Quarantine preserva o original rejeitado e a razão.", 16, 500, "#506983")

y = 865
for section, items in sections:
    text(70, y, section, 20, 750, "#56718b")
    y += 26
    for idx, item in enumerate(items):
        col, row = idx % 3, idx // 3
        service, address, title, lines, *rest = item
        card(X0 + col * (COL + GAP), y + row * 160, service, address, title, lines, *(rest or []))
    y += ((len(items) + 2) // 3) * 160 + 42

rect(70, height - 105, 2380, 65, "#fff", "#cbd8e6", 16)
text(95, height - 76, "TF = recurso criado em apply; OPT = count/condicional. Dados locais, variáveis e outputs não criam recursos AWS e não aparecem como cartões.", 16, 500, "#4d667e")
add("</svg>")
(ROOT / "fluxo-lakehouse.svg").write_text("\n".join(parts) + "\n", encoding="utf-8")
