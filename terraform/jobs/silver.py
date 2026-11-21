"""Aula 25: objetos Bronze -> tabelas Iceberg v2 Silver, com MERGE idempotente."""
import sys
import json
import re
import hashlib
import io
from datetime import datetime, timezone
from uuid import uuid4
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE", "SILVER", "RESULTS", "DATABASE", "RUN_ID", "BRONZE_MANIFEST", "OPERATIONS_TABLE"])
ctx = GlueContext(SparkContext.getOrCreate())
spark = ctx.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(ctx)
job.init(args["JOB_NAME"], args)
s3 = boto3.client("s3")
glue = boto3.client("glue")
db = args["DATABASE"]
cat = f"glue_catalog.{db}"
started_at = datetime.now(timezone.utc).isoformat()
attempt_id = int(sys.argv[sys.argv.index("--ATTEMPT_ID") + 1]) if "--ATTEMPT_ID" in sys.argv else 1
job_run_id = sys.argv[sys.argv.index("--JOB_RUN_ID") + 1] if "--JOB_RUN_ID" in sys.argv else None
bronze_manifest = json.loads(s3.get_object(Bucket=args["RESULTS"], Key=args["BRONZE_MANIFEST"])["Body"].read())
DATASETS = {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"}
if bronze_manifest.get("layer") != "bronze" or bronze_manifest.get("operation") not in {"initial", "incremental"} or not bronze_manifest.get("batch_id"):
    raise ValueError("Contrato do manifesto Bronze inválido")
files = bronze_manifest.get("files")
if not isinstance(files, list) or not files:
    raise ValueError("Manifesto Bronze vazio")
def stage_manifest_objects(client, bronze_bucket, results_bucket, run_id, files):
    """Lê o VersionId indicado e entrega ao Spark só os bytes verificados."""
    staged = {name: [] for name in DATASETS}
    provenance = {}
    seen = set()
    for uploaded in files:
        key, version, expected = (uploaded.get(k) for k in ("s3_key", "version_id", "sha256"))
        dataset = uploaded.get("dataset_type") or key.split("/")[0]
        if dataset not in DATASETS or not key or not version or not re.fullmatch(r"[a-f0-9]{64}", expected or ""):
            raise ValueError("Arquivo Bronze inválido")
        if key.split("/")[0] not in {dataset, "ingestion"} or (key.startswith("ingestion/") and key.split("/")[1] != dataset):
            raise ValueError("dataset_type incompatível com s3_key")
        if (key, version) in seen:
            raise ValueError("Objeto duplicado no manifesto")
        seen.add((key, version))
        obj = client.get_object(Bucket=bronze_bucket, Key=key, VersionId=version)
        body = obj["Body"].read()
        if obj.get("VersionId") != version or hashlib.sha256(body).hexdigest() != expected:
            raise ValueError(f"Versão ou SHA-256 Bronze divergente: {key}")
        if uploaded.get("size_bytes") is not None and len(body) != uploaded["size_bytes"]:
            raise ValueError(f"Tamanho Bronze divergente: {key}")
        suffix = ".csv" if dataset in {"pedidos", "itens", "clientes", "produtos"} else ".jsonl"
        stage_key = f"staging/silver/{run_id}/{dataset}/{hashlib.sha256((key + version).encode()).hexdigest()}{suffix}"
        client.put_object(Bucket=results_bucket, Key=stage_key, Body=body, Metadata={"source-sha256": expected})
        stage_uri = f"s3://{results_bucket}/{stage_key}"
        staged[dataset].append(stage_uri)
        provenance[stage_uri] = f"s3://{bronze_bucket}/{key}?versionId={version}"
    return staged, provenance


staged, provenance = stage_manifest_objects(s3, args["BRONZE"], args["RESULTS"], args["RUN_ID"], files)

previous_manifest = None
if bronze_manifest["operation"] == "incremental":
    pointer = boto3.client("dynamodb").get_item(TableName=args["OPERATIONS_TABLE"],
        Key={"run_id": {"S": "PUBLISHED"}}, ConsistentRead=True).get("Item", {})
    previous_uri = pointer.get("silver_manifest", {}).get("S")
    if not previous_uri or not previous_uri.startswith(f"s3://{args['RESULTS']}/"):
        raise ValueError("Lote incremental sem versão Silver publicada")
    previous_key = previous_uri.split(f"s3://{args['RESULTS']}/", 1)[1]
    previous_manifest = json.loads(s3.get_object(Bucket=args["RESULTS"], Key=previous_key)["Body"].read())
    if previous_manifest.get("status") != "SUCCEEDED":
        raise ValueError("Versão Silver publicada inválida")

present = {name for name, paths in staged.items() if paths}
if bronze_manifest["operation"] == "initial" and present != DATASETS:
    raise ValueError(f"Lote inicial incompleto: {sorted(DATASETS - present)}")
if bronze_manifest["operation"] == "incremental":
    for name in DATASETS - present:
        target = "historico_pagamentos" if name == "pagamentos" else name
        try:
            glue.get_table(DatabaseName=db, Name=target)
        except glue.exceptions.EntityNotFoundException:
            raise ValueError(f"Incremental sem estado Silver anterior: {name}")

SCHEMAS = {
    "pedidos": "pedido_id STRING, data_pedido DATE, cliente_id STRING, canal_id STRING, status STRING, origem_arquivo STRING",
    "itens": "item_id STRING, pedido_id STRING, produto_id STRING, quantidade INT, preco_unitario DECIMAL(12,2), origem_arquivo STRING",
    "clientes": "cliente_id STRING, cliente STRING, regiao_id STRING, regiao STRING, estado STRING, origem_arquivo STRING",
    "produtos": "produto_id STRING, produto STRING, categoria STRING, origem_arquivo STRING",
    "historico_pagamentos": "evento_pagamento_id STRING, pedido_id STRING, resultado STRING, atualizado_em TIMESTAMP, origem_arquivo STRING",
    "pagamentos": "pedido_id STRING, resultado STRING, atualizado_em TIMESTAMP, origem_arquivo STRING",
    "eventos": "evento_id STRING, sessao STRING, evento STRING, pedido_id STRING, data DATE, canal_id STRING, origem_arquivo STRING",
}
KEYS = {
    "pedidos": "pedido_id", "itens": "item_id", "clientes": "cliente_id", "produtos": "produto_id",
    "historico_pagamentos": "evento_pagamento_id", "pagamentos": "pedido_id", "eventos": "evento_id"
}


def check(df, condition, label):
    if df.filter(condition).limit(1).count() > 0:
        raise ValueError(f"Contrato invalido: {label}")


def source_column(df):
    pairs = []
    for path, original in provenance.items():
        pairs.extend([F.lit(path), F.lit(original)])
    lookup = F.create_map(*pairs)
    return df.withColumn("origem_arquivo", lookup[F.regexp_replace(F.input_file_name(), "^s3a://", "s3://")])


def load_csv(name, schema):
    return source_column(spark.read.option("header", "true").option("sep", ";").schema(schema).csv(staged[name]))


def load_json(name, schema):
    return source_column(spark.read.schema(schema).json(staged[name]))


def previous(name):
    snapshot = previous_manifest["snapshot_ids"].get(name)
    if not snapshot:
        raise ValueError(f"Snapshot Silver anterior ausente: {name}")
    return spark.read.format("iceberg").option("snapshot-id", snapshot).load(f"{cat}.{name}")


def dedup_identical(df, key):
    unique = df.dropDuplicates()
    if unique.groupBy(key).count().filter("count > 1").limit(1).count():
        raise ValueError(f"Duas versoes conflitantes da chave {key} no mesmo estado da fonte")
    return unique


def put_table(name, df):
    fq = f"{cat}.{name}"
    loc = f"s3://{args['SILVER']}/{name}/"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fq} ({SCHEMAS[name]}) USING iceberg "
              f"LOCATION '{loc}' TBLPROPERTIES ('format-version'='2')")
    columns = re.findall(r"([a-z_]+)\s+(?:STRING|DATE|TIMESTAMP|INT|DECIMAL\(\d+,\d+\))", SCHEMAS[name])
    # Overwrite completo da tabela a partir do snapshot publicado + lote. O
    # commit Iceberg anterior permanece acessível enquanto não expirar.
    df.select(*columns).writeTo(fq).overwritePartitions()



if "pedidos" in present:
    # TODAS as validacoes da carga ocorrem antes do primeiro commit Iceberg.
    pedidos = load_csv("pedidos", "pedido_id STRING, data_pedido STRING, cliente_id STRING, canal_id STRING, status STRING")
    pedidos = pedidos.select(F.trim("pedido_id").alias("pedido_id"), F.to_date("data_pedido").alias("data_pedido"),
                             F.trim("cliente_id").alias("cliente_id"), F.lower(F.trim("canal_id")).alias("canal_id"),
                             F.lower(F.trim("status")).alias("status"), "origem_arquivo")
    check(pedidos, "pedido_id IS NULL OR data_pedido IS NULL OR cliente_id IS NULL OR canal_id IS NULL OR status NOT IN ('pago','cancelado')", "pedidos")
    pedidos = dedup_identical(pedidos, "pedido_id")
else:
    pedidos = previous("pedidos")
if "itens" in present:
    itens = load_csv("itens", "item_id STRING, pedido_id STRING, produto_id STRING, quantidade STRING, preco_unitario STRING")
    itens = itens.select("item_id", "pedido_id", "produto_id", F.col("quantidade").cast("int").alias("quantidade"),
                         F.regexp_replace("preco_unitario", ",", ".").cast("decimal(12,2)").alias("preco_unitario"), "origem_arquivo")
    check(itens, "item_id IS NULL OR pedido_id IS NULL OR produto_id IS NULL OR quantidade IS NULL OR quantidade <= 0 OR preco_unitario IS NULL OR preco_unitario < 0", "itens")
    itens = dedup_identical(itens, "item_id")
else:
    itens = previous("itens")
if "clientes" in present:
    clientes = load_csv("clientes", "cliente_id STRING, cliente STRING, regiao_id STRING, regiao STRING, estado STRING").dropDuplicates()
else:
    clientes = previous("clientes")
if "produtos" in present:
    produtos = load_csv("produtos", "produto_id STRING, produto STRING, categoria STRING").dropDuplicates()
else:
    produtos = previous("produtos")
check(clientes, "cliente_id IS NULL OR cliente IS NULL OR regiao_id IS NULL OR regiao IS NULL OR estado IS NULL", "clientes")
check(produtos, "produto_id IS NULL OR produto IS NULL OR categoria IS NULL", "produtos")
clientes = dedup_identical(clientes, "cliente_id")
produtos = dedup_identical(produtos, "produto_id")

if "pagamentos" in present:
    hist = load_json("pagamentos", "evento_pagamento_id STRING, pedido_id STRING, resultado STRING, atualizado_em STRING")
    hist = hist.withColumn("atualizado_em", F.to_timestamp("atualizado_em"))
    check(hist, "evento_pagamento_id IS NULL OR pedido_id IS NULL OR atualizado_em IS NULL OR resultado IS NULL OR resultado NOT IN ('aprovado','estornado','pendente','sem_cobranca')", "pagamentos")
    hist = dedup_identical(hist, "evento_pagamento_id")
    w = Window.partitionBy("pedido_id").orderBy(F.col("atualizado_em").desc(), F.col("evento_pagamento_id").desc())
    pagamentos = hist.withColumn("rn", F.row_number().over(w)).filter("rn=1").select("pedido_id", "resultado", "atualizado_em", "origem_arquivo")
else:
    hist = previous("historico_pagamentos")
    pagamentos = previous("pagamentos")
if "eventos" in present:
    eventos = load_json("eventos", "evento_id STRING, sessao STRING, evento STRING, pedido_id STRING, dia STRING, canal_id STRING")
    eventos = eventos.select("evento_id", "sessao", "evento", "pedido_id", F.to_date("dia").alias("data"), "canal_id", "origem_arquivo")
    check(eventos, "evento_id IS NULL OR sessao IS NULL OR evento IS NULL OR data IS NULL OR canal_id IS NULL", "eventos")
    eventos = dedup_identical(eventos, "evento_id")
else:
    eventos = previous("eventos")
# O lote incremental altera apenas suas chaves. Para validar FKs e calcular
# a versão corrente de pagamentos, combinamos com o estado Silver anterior.
if bronze_manifest["operation"] == "incremental":
    for name in ("pedidos", "itens", "clientes", "produtos", "eventos"):
        if name in present:
            incoming = locals()[name]
            try:
                old = previous(name)
            except Exception as exc:
                raise ValueError(f"Incremental sem tabela anterior: {name}") from exc
            key = KEYS[name]
            combined = incoming.unionByName(old.join(incoming.select(key), key, "left_anti"))
            if name == "pedidos": pedidos = combined
            elif name == "itens": itens = combined
            elif name == "clientes": clientes = combined
            elif name == "produtos": produtos = combined
            else: eventos = combined
    if "pagamentos" in present:
        old_hist = previous("historico_pagamentos")
        hist = hist.unionByName(old_hist.join(hist.select("evento_pagamento_id"), "evento_pagamento_id", "left_anti"))
        w = Window.partitionBy("pedido_id").orderBy(F.col("atualizado_em").desc(), F.col("evento_pagamento_id").desc())
        pagamentos = hist.withColumn("rn", F.row_number().over(w)).filter("rn=1").select("pedido_id", "resultado", "atualizado_em", "origem_arquivo")

# Integridade referencial analitica no ETL (Iceberg nao garante FKs).
for missing, label in [
    (itens.join(pedidos.select("pedido_id"), "pedido_id", "left_anti"), "item sem pedido"),
    (itens.join(produtos.select("produto_id"), "produto_id", "left_anti"), "item sem produto"),
    (pedidos.join(clientes.select("cliente_id"), "cliente_id", "left_anti"), "pedido sem cliente"),
]:
    if missing.limit(1).count(): raise ValueError(label)

payload = {"pedidos": pedidos, "itens": itens, "clientes": clientes, "produtos": produtos,
           "historico_pagamentos": hist, "pagamentos": pagamentos, "eventos": eventos}
for table, frame in payload.items():
    if ("pagamentos" if table == "historico_pagamentos" else table) in present:
        put_table(table, frame)

# Manifesto OPERACIONAL de rastreabilidade, separado dos manifests Avro internos do Iceberg.
inputs=[{"key": f["s3_key"], "version_id": f["version_id"], "sha256": f["sha256"], "dataset_type": f.get("dataset_type") or f["s3_key"].split("/")[0]} for f in files]
if previous_manifest:
    inputs = list({(item["key"], item["version_id"]): item for item in previous_manifest["bronze_objects"] + inputs}.values())

metadados={}
snapshots={}
for name in payload:
    table=glue.get_table(DatabaseName=db,Name=name)["Table"]
    if name in present or (name == "historico_pagamentos" and "pagamentos" in present):
        metadados[name]=table.get("Parameters",{}).get("metadata_location")
        snapshots[name]=str(spark.sql(f"SELECT snapshot_id FROM {cat}.{name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0])
    else:
        metadados[name]=previous_manifest["iceberg_metadata_locations"][name]
        snapshots[name]=previous_manifest["snapshot_ids"][name]
key=f"manifestos/silver/{args['RUN_ID']}.json"
s3.put_object(Bucket=args["RESULTS"],Key=key,
              Body=json.dumps({"layer":"silver","run_id":args["RUN_ID"],"bronze_manifest":f"s3://{args['RESULTS']}/{args['BRONZE_MANIFEST']}","bronze_objects":inputs,"iceberg_metadata_locations":metadados,"snapshot_ids":snapshots,"batch_id":bronze_manifest["batch_id"],"attempt_id":attempt_id,"tables_updated":sorted(present | ({"historico_pagamentos"} if "pagamentos" in present else set())),"started_at":started_at,"finished_at":datetime.now(timezone.utc).isoformat(),"status":"SUCCEEDED","glue_job_run_id":job_run_id},indent=2).encode(),
              ContentType="application/json")
job.commit()
