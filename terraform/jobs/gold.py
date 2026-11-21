"""Aula 25: Silver Iceberg -> dimensoes, fato de itens e fato de sessoes Gold."""
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
from pyspark.sql import functions as F

args=getResolvedOptions(sys.argv,["JOB_NAME","SILVER","GOLD","RESULTS","DATABASE","RUN_ID"])
ctx=GlueContext(SparkContext.getOrCreate())
spark=ctx.spark_session
spark.conf.set("spark.sql.session.timeZone","UTC")
job=Job(ctx);job.init(args["JOB_NAME"],args)
db=args["DATABASE"];cat=f"glue_catalog.{db}"
s3=boto3.client("s3"); glue=boto3.client("glue")
source_silver_run_id = args["RUN_ID"]
if "--SOURCE_SILVER_RUN_ID" in sys.argv:
    source_silver_run_id = sys.argv[sys.argv.index("--SOURCE_SILVER_RUN_ID") + 1]
silver_key = f"manifestos/silver/{source_silver_run_id}.json"
silver_manifest = json.loads(s3.get_object(Bucket=args["RESULTS"], Key=silver_key)["Body"].read())
snapshot_ids = silver_manifest["snapshot_ids"]
for name in ("pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"):
    if not snapshot_ids.get(name):
        raise ValueError(f"Snapshot Silver ausente: {name}")
started_at = datetime.now(timezone.utc).isoformat()
attempt_id = int(sys.argv[sys.argv.index("--ATTEMPT_ID") + 1]) if "--ATTEMPT_ID" in sys.argv else 1
job_run_id = sys.argv[sys.argv.index("--JOB_RUN_ID") + 1] if "--JOB_RUN_ID" in sys.argv else None



def read(name):
    return spark.read.format("iceberg").option("snapshot-id", snapshot_ids[name]).load(f"{cat}.{name}")

p=read("pedidos").withColumnRenamed("origem_arquivo", "origem_pedido")
i=read("itens").withColumnRenamed("origem_arquivo", "origem_item")
c=read("clientes").withColumnRenamed("origem_arquivo", "origem_cliente")
pr=read("produtos").withColumnRenamed("origem_arquivo", "origem_produto")
pg=read("pagamentos").withColumnRenamed("origem_arquivo", "origem_pagamento")
e=read("eventos")

# Chaves de dimensoes sao estaveis e deterministicas, sem geracao aleatoria.
datas=(p.select(F.col("data_pedido").alias("data"),F.col("origem_pedido").alias("origem"))
       .union(e.select("data",F.col("origem_arquivo").alias("origem"))))
tempo=(datas.groupBy("data")
       .agg(F.concat_ws(",",F.sort_array(F.collect_set("origem"))).alias("origem_arquivos"))
       .select(F.date_format("data","yyyyMMdd").cast("int").alias("sk_tempo"),
               "data", F.year("data").alias("ano"),F.month("data").alias("mes"),"origem_arquivos"))
cliente=c.select(F.col("cliente_id").alias("sk_cliente"),"cliente","estado", "regiao_id", "origem_cliente").dropDuplicates(["sk_cliente"])
produto=pr.select(F.col("produto_id").alias("sk_produto"),"produto","categoria", "origem_produto").dropDuplicates(["sk_produto"])
canal=(p.select(F.col("canal_id").alias("sk_canal"),F.col("origem_pedido").alias("origem"))
       .union(e.select(F.col("canal_id").alias("sk_canal"),F.col("origem_arquivo").alias("origem")))
       .groupBy("sk_canal")
       .agg(F.concat_ws(",",F.sort_array(F.collect_set("origem"))).alias("origem_arquivos")))
regiao=(c.groupBy(F.col("regiao_id").alias("sk_regiao"))
        .agg(F.min("regiao").alias("regiao"),
             F.concat_ws(",",F.sort_array(F.collect_set("origem_cliente"))).alias("origem_arquivos")))

joined=(i.join(p,"pedido_id").join(c,"cliente_id")
        .join(pr.select("produto_id","origem_produto"),"produto_id")
        .join(pg.select("pedido_id","resultado","origem_pagamento"),"pedido_id","left"))
fato=(joined.select("item_id","pedido_id",F.date_format("data_pedido","yyyyMMdd").cast("int").alias("sk_tempo"),
                    F.col("data_pedido"),F.col("cliente_id").alias("sk_cliente"),F.col("produto_id").alias("sk_produto"),
                    F.col("canal_id").alias("sk_canal"),F.col("regiao_id").alias("sk_regiao"),
                    "quantidade","preco_unitario","status",F.coalesce("resultado",F.lit("sem_pagamento")).alias("resultado"),
                    "origem_item", "origem_pedido", "origem_cliente", "origem_produto", "origem_pagamento")
      .withColumn("valor_bruto",(F.col("quantidade")*F.col("preco_unitario")).cast("decimal(14,2)"))
      .withColumn("receita_liquida",F.when((F.col("status")=="pago") & (F.col("resultado")=="aprovado"),
                                            F.col("valor_bruto")).otherwise(F.lit(0)).cast("decimal(14,2)")))
# Sessao e outro fato: nao misturar eventos com itens, evitando multiplicacao de receita.
sessoes=(e.groupBy("sessao","data","canal_id")
         .agg(F.max(F.when(F.col("evento")=="checkout",1).otherwise(0)).alias("houve_checkout"),
              F.concat_ws(",", F.sort_array(F.collect_set("origem_arquivo"))).alias("origem_eventos"))
         .select(F.concat_ws("_","sessao",F.date_format("data","yyyyMMdd"),"canal_id").alias("sk_sessao"),
                 F.date_format("data","yyyyMMdd").cast("int").alias("sk_tempo"),
                 F.col("canal_id").alias("sk_canal"),"houve_checkout", "origem_eventos"))

TABLES={
 "dim_tempo": (tempo,"sk_tempo", "sk_tempo INT, data DATE, ano INT, mes INT, origem_arquivos STRING",None),
 "dim_cliente": (cliente,"sk_cliente","sk_cliente STRING, cliente STRING, estado STRING, regiao_id STRING, origem_cliente STRING",None),
 "dim_produto": (produto,"sk_produto","sk_produto STRING, produto STRING, categoria STRING, origem_produto STRING",None),
 "dim_canal": (canal,"sk_canal","sk_canal STRING, origem_arquivos STRING",None),
 "dim_regiao": (regiao,"sk_regiao","sk_regiao STRING, regiao STRING, origem_arquivos STRING",None),
 "fato_itens_venda": (fato,"item_id","item_id STRING, pedido_id STRING, sk_tempo INT, data_pedido DATE, sk_cliente STRING, sk_produto STRING, sk_canal STRING, sk_regiao STRING, quantidade INT, preco_unitario DECIMAL(12,2), status STRING, resultado STRING, origem_item STRING, origem_pedido STRING, origem_cliente STRING, origem_produto STRING, origem_pagamento STRING, valor_bruto DECIMAL(14,2), receita_liquida DECIMAL(14,2)","months(data_pedido)"),
 "fato_sessoes": (sessoes,"sk_sessao","sk_sessao STRING, sk_tempo INT, sk_canal STRING, houve_checkout INT, origem_eventos STRING",None),
}

# Validação da candidata antes de qualquer commit por tabela.
for name, (frame, key, _schema, _partition) in TABLES.items():
    if frame.limit(1).count() == 0:
        raise ValueError(f"Gold candidata vazia: {name}")
    if frame.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"Chave Gold nula: {name}")
    if frame.groupBy(key).count().filter("count > 1").limit(1).count():
        raise ValueError(f"Chave Gold duplicada: {name}")
if fato.filter("receita_liquida < 0 OR valor_bruto < 0").limit(1).count():
    raise ValueError("Indicadores financeiros negativos")

for name,(df,key,schema,partition) in TABLES.items():
    df=df.withColumn("etl_run_id",F.lit(args["RUN_ID"]))
    schema=f"{schema}, etl_run_id STRING"
    location=f"s3://{args['GOLD']}/{name}/"
    partition_sql=f" PARTITIONED BY ({partition})" if partition else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {cat}.{name} ({schema}) USING iceberg{partition_sql} "
              f"LOCATION '{location}' TBLPROPERTIES ('format-version'='2')")
    cols=re.findall(r"([a-z_]+)\s+(?:STRING|DATE|TIMESTAMP|INT|DECIMAL\(\d+,\d+\))", schema)
    # Cada tabela é substituída por um snapshot completo da candidata.
    # O ponteiro publicado só muda depois das sete validações e commits.
    df.select(*cols).writeTo(f"{cat}.{name}").overwritePartitions()


metadados={}
gold_snapshots={}
for name in TABLES:
    tb=glue.get_table(DatabaseName=db,Name=name)["Table"]
    metadados[name]=tb.get("Parameters",{}).get("metadata_location")
    gold_snapshots[name]=str(spark.sql(f"SELECT snapshot_id FROM {cat}.{name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0])
    if spark.read.format("iceberg").option("snapshot-id", gold_snapshots[name]).load(f"{cat}.{name}").limit(1).count() == 0:
        raise ValueError(f"Snapshot Gold vazio: {name}")
key=f"manifestos/gold/{args['RUN_ID']}.json"
s3.put_object(Bucket=args["RESULTS"],Key=key,Body=json.dumps({"layer":"gold","run_id":args["RUN_ID"],"iceberg_metadata_locations":metadados,
 "silver_operational_manifest":f"s3://{args['RESULTS']}/{silver_key}",
 "source_silver_run_id":source_silver_run_id,"batch_id":silver_manifest["batch_id"],"snapshot_ids":gold_snapshots,"status":"CANDIDATE","attempt_id":attempt_id,"glue_job_run_id":job_run_id,"started_at":started_at,"finished_at":datetime.now(timezone.utc).isoformat(),
 "atomicity_scope":"per-table","grain":"item_id in fato_itens_venda"},indent=2).encode(),ContentType="application/json")
job.commit()
