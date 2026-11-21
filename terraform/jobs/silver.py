"""Aula 25: objetos Bronze -> tabelas Iceberg v2 Silver, com MERGE idempotente."""
import sys
import json
import re
from datetime import datetime, timezone
from uuid import uuid4
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE", "SILVER", "RESULTS", "DATABASE", "RUN_ID", "BRONZE_MANIFEST"])
ctx = GlueContext(SparkContext.getOrCreate())
spark = ctx.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(ctx)
job.init(args["JOB_NAME"], args)
s3 = boto3.client("s3")
glue = boto3.client("glue")
db = args["DATABASE"]
cat = f"glue_catalog.{db}"
bronze_manifest = json.loads(s3.get_object(Bucket=args["RESULTS"], Key=args["BRONZE_MANIFEST"])["Body"].read())
for uploaded in bronze_manifest["files"]:
    head = s3.head_object(Bucket=args["BRONZE"], Key=uploaded["s3_key"])
    if head.get("VersionId") != uploaded["version_id"] or head.get("Metadata", {}).get("sha256") != uploaded["sha256"]:
        raise ValueError(f"Objeto Bronze alterado após manifesto: {uploaded['s3_key']}")

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


def load_csv(name, schema):
    return spark.read.option("header", "true").option("sep", ";").schema(schema) \
        .option("recursiveFileLookup", "true").csv(f"s3://{args['BRONZE']}/{name}/") \
        .withColumn("origem_arquivo", F.input_file_name())


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
    view = f"incoming_{name}"
    df.select(*re.findall(r"([a-z_]+)\s+(?:STRING|DATE|TIMESTAMP|INT|DECIMAL\(\d+,\d+\))", SCHEMAS[name])).createOrReplaceTempView(view)
    key = KEYS[name]
    spark.sql(f"MERGE INTO {fq} AS t USING {view} AS s ON t.{key}=s.{key} "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")


# TODAS as validacoes da carga ocorrem antes do primeiro commit Iceberg.
pedidos = load_csv("pedidos", "pedido_id STRING, data_pedido STRING, cliente_id STRING, canal_id STRING, status STRING")
pedidos = pedidos.select(F.trim("pedido_id").alias("pedido_id"), F.to_date("data_pedido").alias("data_pedido"),
                         F.trim("cliente_id").alias("cliente_id"), F.lower(F.trim("canal_id")).alias("canal_id"),
                         F.lower(F.trim("status")).alias("status"), "origem_arquivo")
check(pedidos, "pedido_id IS NULL OR data_pedido IS NULL OR cliente_id IS NULL OR canal_id IS NULL OR status NOT IN ('pago','cancelado')", "pedidos")
pedidos = dedup_identical(pedidos, "pedido_id")

itens = load_csv("itens", "item_id STRING, pedido_id STRING, produto_id STRING, quantidade STRING, preco_unitario STRING")
itens = itens.select("item_id", "pedido_id", "produto_id", F.col("quantidade").cast("int").alias("quantidade"),
                     F.regexp_replace("preco_unitario", ",", ".").cast("decimal(12,2)").alias("preco_unitario"), "origem_arquivo")
check(itens, "item_id IS NULL OR pedido_id IS NULL OR produto_id IS NULL OR quantidade IS NULL OR quantidade <= 0 OR preco_unitario IS NULL OR preco_unitario < 0", "itens")
itens = dedup_identical(itens, "item_id")

clientes = load_csv("clientes", "cliente_id STRING, cliente STRING, regiao_id STRING, regiao STRING, estado STRING").dropDuplicates()
produtos = load_csv("produtos", "produto_id STRING, produto STRING, categoria STRING").dropDuplicates()
check(clientes, "cliente_id IS NULL OR cliente IS NULL OR regiao_id IS NULL OR regiao IS NULL OR estado IS NULL", "clientes")
check(produtos, "produto_id IS NULL OR produto IS NULL OR categoria IS NULL", "produtos")
clientes = dedup_identical(clientes, "cliente_id")
produtos = dedup_identical(produtos, "produto_id")

hist = spark.read.schema("evento_pagamento_id STRING, pedido_id STRING, resultado STRING, atualizado_em STRING") \
    .option("recursiveFileLookup", "true").json(f"s3://{args['BRONZE']}/pagamentos/").withColumn("origem_arquivo", F.input_file_name())
hist = hist.withColumn("atualizado_em", F.to_timestamp("atualizado_em"))
check(hist, "evento_pagamento_id IS NULL OR pedido_id IS NULL OR atualizado_em IS NULL OR resultado IS NULL OR resultado NOT IN ('aprovado','estornado','pendente','sem_cobranca')", "pagamentos")
hist = dedup_identical(hist, "evento_pagamento_id")
w = Window.partitionBy("pedido_id").orderBy(F.col("atualizado_em").desc(), F.col("evento_pagamento_id").desc())
pagamentos = hist.withColumn("rn", F.row_number().over(w)).filter("rn=1").select("pedido_id", "resultado", "atualizado_em", "origem_arquivo")

eventos = spark.read.schema("evento_id STRING, sessao STRING, evento STRING, pedido_id STRING, dia STRING, canal_id STRING") \
    .option("recursiveFileLookup", "true").json(f"s3://{args['BRONZE']}/eventos/").withColumn("origem_arquivo", F.input_file_name())
eventos = eventos.select("evento_id", "sessao", "evento", "pedido_id", F.to_date("dia").alias("data"), "canal_id", "origem_arquivo")
check(eventos, "evento_id IS NULL OR sessao IS NULL OR evento IS NULL OR data IS NULL OR canal_id IS NULL", "eventos")
eventos = dedup_identical(eventos, "evento_id")

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
    put_table(table, frame)

# Manifesto OPERACIONAL de rastreabilidade, separado dos manifests Avro internos do Iceberg.
inputs=[]
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=args["BRONZE"]):
    for obj in page.get("Contents", []):
        head=s3.head_object(Bucket=args["BRONZE"],Key=obj["Key"])
        inputs.append({"key":obj["Key"],"version_id":head.get("VersionId"),
                       "sha256":head.get("Metadata",{}).get("sha256"),"etag":head["ETag"]})
metadados={}
for name in payload:
    table=glue.get_table(DatabaseName=db,Name=name)["Table"]
    metadados[name]=table.get("Parameters",{}).get("metadata_location")
key=f"manifestos/silver/{args['RUN_ID']}.json"
s3.put_object(Bucket=args["RESULTS"],Key=key,
              Body=json.dumps({"layer":"silver","run_id":args["RUN_ID"],"bronze_manifest":f"s3://{args['RESULTS']}/{args['BRONZE_MANIFEST']}","bronze_objects":inputs,"iceberg_metadata_locations":metadados},indent=2).encode(),
              ContentType="application/json")
job.commit()
