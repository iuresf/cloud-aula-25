"""Bronze versionado -> Silver Iceberg com auditoria de qualidade por ocorrência."""
import sys, json, re, hashlib
from datetime import datetime, timezone
from functools import reduce
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE", "SILVER", "RESULTS", "DATABASE", "RUN_ID", "BRONZE_MANIFEST", "OPERATIONS_TABLE"])
ctx = GlueContext(SparkContext.getOrCreate()); spark = ctx.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(ctx); job.init(args["JOB_NAME"], args)
s3, glue, ddb = boto3.client("s3"), boto3.client("glue"), boto3.client("dynamodb")
db, cat = args["DATABASE"], f"glue_catalog.{args['DATABASE']}"
started_at = datetime.now(timezone.utc).isoformat()
attempt_id = int(sys.argv[sys.argv.index("--ATTEMPT_ID") + 1]) if "--ATTEMPT_ID" in sys.argv else 1
job_run_id = sys.argv[sys.argv.index("--JOB_RUN_ID") + 1] if "--JOB_RUN_ID" in sys.argv else None
DATASETS = {"pedidos", "itens", "clientes", "produtos", "pagamentos", "eventos"}
KEYS = {"pedidos":"pedido_id", "itens":"item_id", "clientes":"cliente_id", "produtos":"produto_id", "historico_pagamentos":"evento_pagamento_id", "pagamentos":"pedido_id", "eventos":"evento_id"}
SCHEMAS = {
 "pedidos":"pedido_id STRING, data_pedido DATE, cliente_id STRING, canal_id STRING, status STRING, origem_arquivo STRING",
 "itens":"item_id STRING, pedido_id STRING, produto_id STRING, quantidade INT, preco_unitario DECIMAL(12,2), origem_arquivo STRING",
 "clientes":"cliente_id STRING, cliente STRING, regiao_id STRING, regiao STRING, estado STRING, origem_arquivo STRING",
 "produtos":"produto_id STRING, produto STRING, categoria STRING, origem_arquivo STRING",
 "historico_pagamentos":"evento_pagamento_id STRING, pedido_id STRING, resultado STRING, atualizado_em TIMESTAMP, origem_arquivo STRING",
 "pagamentos":"pedido_id STRING, resultado STRING, atualizado_em TIMESTAMP, origem_arquivo STRING",
 "eventos":"evento_id STRING, sessao STRING, evento STRING, pedido_id STRING, data DATE, canal_id STRING, origem_arquivo STRING"}

def stage_manifest_objects(client, bronze_bucket, results_bucket, run_id, files):
    """Obtém somente a versão contratada e não faz segunda leitura para qualidade."""
    staged, provenance, seen = ({x: [] for x in DATASETS}, {}, set())
    for item in files:
        key, version, expected = item.get("s3_key"), item.get("version_id"), item.get("sha256")
        dataset = item.get("dataset_type") or (key or "").split("/")[0]
        if dataset not in DATASETS or not key or not version or not re.fullmatch(r"[a-f0-9]{64}", expected or "") or (key, version) in seen: raise ValueError("Arquivo Bronze inválido ou duplicado")
        seen.add((key, version)); obj = client.get_object(Bucket=bronze_bucket, Key=key, VersionId=version); body = obj["Body"].read()
        if obj.get("VersionId") != version or hashlib.sha256(body).hexdigest() != expected: raise ValueError(f"Versão ou SHA-256 Bronze divergente: {key}")
        if item.get("size_bytes") is not None and len(body) != item["size_bytes"]: raise ValueError(f"Tamanho Bronze divergente: {key}")
        suffix = ".csv" if dataset in {"pedidos","itens","clientes","produtos"} else ".jsonl"
        stage_key = f"staging/silver/{run_id}/{dataset}/{hashlib.sha256((key + version).encode()).hexdigest()}{suffix}"
        client.put_object(Bucket=results_bucket, Key=stage_key, Body=body, Metadata={"source-sha256":expected})
        uri=f"s3://{results_bucket}/{stage_key}"; staged[dataset].append(uri); provenance[uri]=f"s3://{bronze_bucket}/{key}?versionId={version}"
    return staged, provenance

bronze_manifest=json.loads(s3.get_object(Bucket=args["RESULTS"],Key=args["BRONZE_MANIFEST"])["Body"].read())
files=bronze_manifest.get("files")
if bronze_manifest.get("layer") != "bronze" or bronze_manifest.get("operation") not in {"initial","incremental"} or not bronze_manifest.get("batch_id") or not isinstance(files,list) or not files: raise ValueError("Contrato do manifesto Bronze inválido")
staged, provenance = stage_manifest_objects(s3,args["BRONZE"],args["RESULTS"],args["RUN_ID"],files)
present={x for x,paths in staged.items() if paths}
if bronze_manifest["operation"] == "initial" and present != DATASETS: raise ValueError("Lote inicial incompleto")
previous_manifest=None
if bronze_manifest["operation"] == "incremental":
    pointer=ddb.get_item(TableName=args["OPERATIONS_TABLE"],Key={"run_id":{"S":"PUBLISHED"}},ConsistentRead=True).get("Item",{})
    previous_uri=pointer.get("silver_manifest",{}).get("S")
    if not previous_uri or not previous_uri.startswith(f"s3://{args['RESULTS']}/"): raise ValueError("Lote incremental sem versão Silver publicada")
    previous_key=previous_uri.split(f"s3://{args['RESULTS']}/",1)[1]
    previous_manifest=json.loads(s3.get_object(Bucket=args["RESULTS"],Key=previous_key)["Body"].read())
    if previous_manifest.get("status") != "SUCCEEDED": raise ValueError("Versão Silver publicada inválida")

def physical_records(paths, schema, csv_file=False):
    """A posição vem de linhas físicas: JSONL e CSV de linha única são contratos explícitos."""
    frames=[]
    for path in paths:
        rdd=spark.read.text(path).rdd.zipWithIndex().map(lambda item:(item[0][0],int(item[1])+1))
        raw=spark.createDataFrame(rdd,["_raw_record","source_position"])
        if csv_file: raw, parsed = raw.filter("source_position > 1"), F.from_csv("_raw_record",schema,{"sep":";","mode":"PERMISSIVE"})
        else: parsed=F.from_json("_raw_record",schema)
        frames.append(raw.select(parsed.alias("r"),"source_position","_raw_record").select("r.*","source_position","_raw_record").withColumn("origem_arquivo",F.lit(provenance[path])))
    return reduce(lambda a,b:a.unionByName(b),frames)

anomalies=[]; metrics={}; file_quality={}
def hash_rows(df,dataset,business):
    return df.withColumn("record_hash",F.sha2(F.to_json(F.struct(*[F.col(x) for x in business])),256)).withColumn("occurrence_id",F.sha2(F.concat_ws("|",F.lit(dataset),"origem_arquivo",F.col("source_position").cast("string"),"record_hash"),256))
def record_anomaly(df,dataset,category,key,detail,kept=None):
    business=[x for x in df.columns if x not in {"_raw_record","source_position","origem_arquivo","record_hash","occurrence_id","_rn","_kept"}]
    anomalies.append(df.select(F.lit(args["RUN_ID"]).alias("run_id"),F.lit(attempt_id).alias("attempt_id"),F.lit(dataset).alias("dataset_type"),F.lit(category).alias("category"),F.col(key).cast("string").alias("business_key"),"occurrence_id",(F.col(kept) if kept else F.lit(None).cast("string")).alias("kept_occurrence_id"),F.col("origem_arquivo").alias("origin_uri"),F.col("source_position").cast("long"),"record_hash",F.to_json(F.struct(*[F.col(x) for x in business])).alias("values_json"),F.lit(detail).alias("detail"),F.lit(started_at).alias("detected_at")))
def persist_quality(status):
    prefix=f"qualidade/silver/run_id={args['RUN_ID']}/attempt_id={attempt_id}"
    anomaly_prefix=f"qualidade/silver/anomalias/run_id={args['RUN_ID']}/attempt_id={attempt_id}"
    if anomalies: reduce(lambda a,b:a.unionByName(b),anomalies).write.mode("overwrite").parquet(f"s3://{args['RESULTS']}/{anomaly_prefix}")
    summary_uri=f"s3://{args['RESULTS']}/{prefix}/resumo.json"; anomalies_uri=f"s3://{args['RESULTS']}/{anomaly_prefix}"
    s3.put_object(Bucket=args["RESULTS"],Key=f"{prefix}/resumo.json",ContentType="application/json",Body=json.dumps({"quality_schema_version":"1","run_id":args["RUN_ID"],"attempt_id":attempt_id,"batch_id":bronze_manifest["batch_id"],"validation_status":status,"datasets":metrics,"anomalies_uri":anomalies_uri},default=str).encode())
    bronze_summary=[]
    for item in files:
        dataset=item.get("dataset_type") or item["s3_key"].split("/")[0]
        origin=f"s3://{args['BRONZE']}/{item['s3_key']}?versionId={item['version_id']}"
        bronze_summary.append({"batch_id":bronze_manifest["batch_id"],"run_id":args["RUN_ID"],"dataset_type":dataset,"s3_key":item["s3_key"],"version_id":item["version_id"],"sha256":item["sha256"],"size_bytes":item.get("size_bytes"),"source_system":item.get("source_system"),"schema_version":item.get("schema_version"),"input_rows":file_quality.get(origin),"status":status,"validation_errors":metrics.get(dataset,{}).get("invalid_rows",0)+metrics.get(dataset,{}).get("conflicting_rows",0)})
    s3.put_object(Bucket=args["RESULTS"],Key=f"qualidade/bronze/run_id={args['RUN_ID']}/resumo.json",ContentType="application/json",Body=json.dumps({"quality_schema_version":"1","batch_id":bronze_manifest["batch_id"],"run_id":args["RUN_ID"],"status":status,"files":bronze_summary},default=str).encode())
    return summary_uri,anomalies_uri
def fail(message): persist_quality("FAIL"); raise ValueError(message)
def validate(df,dataset,key,condition,category,detail):
    bad=df.filter(condition); count=bad.count()
    if count: record_anomaly(bad,dataset,category,key,detail); metrics[dataset]["invalid_rows"]+=count; fail(f"Contrato inválido: {detail}")
def dedup(df,dataset,key,business):
    df=hash_rows(df,dataset,business); conflict_keys=df.groupBy(key).agg(F.countDistinct("record_hash").alias("n")).filter("n > 1").select(key); conflicts=df.join(conflict_keys,key)
    n=conflicts.count()
    if n: record_anomaly(conflicts,dataset,"KEY_CONFLICT",key,"mesma chave com valores de negócio distintos"); metrics[dataset]["conflicting_rows"]=n; fail(f"Duas versões conflitantes da chave {key}")
    window=Window.partitionBy(key,"record_hash").orderBy(F.col("origem_arquivo"),F.col("source_position"),F.col("occurrence_id"))
    ranked=df.withColumn("_rn",F.row_number().over(window)).withColumn("_kept",F.first("occurrence_id").over(window)); discarded=ranked.filter("_rn > 1"); n=discarded.count()
    if n: record_anomaly(discarded,dataset,"DUPLICATE_IDENTICAL",key,"conteúdo idêntico; ordem origem, posição e ocorrência", "_kept")
    metrics[dataset]["duplicate_rows"]=n
    return ranked.filter("_rn = 1").drop("_rn","_kept")
def load(name,schema,transforms,business,csv_file=False):
    frame=physical_records(staged[name],schema,csv_file).select(*transforms,"origem_arquivo","source_position","_raw_record")
    metrics[name]={"input_rows":frame.count(),"duplicate_rows":0,"invalid_rows":0,"conflicting_rows":0}
    # Coleta apenas agregados por arquivo, nunca os registros de entrada.
    for row in frame.groupBy("origem_arquivo").count().collect(): file_quality[row["origem_arquivo"]]=row["count"]
    return hash_rows(frame,name,business)
def previous(name):
    snapshot=previous_manifest["snapshot_ids"].get(name)
    if not snapshot: raise ValueError(f"Snapshot Silver anterior ausente: {name}")
    return spark.read.format("iceberg").option("snapshot-id",snapshot).load(f"{cat}.{name}")
def change_metrics(dataset,incoming,old,key,business):
    """Conta mudanças contra o snapshot anterior por chave e valores de negócio."""
    old_hash=old.withColumn("_old_hash",F.sha2(F.to_json(F.struct(*[F.col(x) for x in business])),256)).select(key,"_old_hash")
    compared=incoming.select(key,"record_hash").join(old_hash,key,"left")
    metrics[dataset]["inserted_rows"]=compared.filter("_old_hash IS NULL").count()
    metrics[dataset]["unchanged_rows"]=compared.filter("_old_hash = record_hash").count()
    metrics[dataset]["updated_rows"]=compared.filter("_old_hash IS NOT NULL AND _old_hash <> record_hash").count()

if "pedidos" in present:
    pedidos=load("pedidos","pedido_id STRING, data_pedido STRING, cliente_id STRING, canal_id STRING, status STRING",[F.trim("pedido_id").alias("pedido_id"),F.to_date("data_pedido").alias("data_pedido"),F.trim("cliente_id").alias("cliente_id"),F.lower(F.trim("canal_id")).alias("canal_id"),F.lower(F.trim("status")).alias("status")],["pedido_id","data_pedido","cliente_id","canal_id","status"],True); validate(pedidos,"pedidos","pedido_id","pedido_id IS NULL OR data_pedido IS NULL OR cliente_id IS NULL OR canal_id IS NULL","NULL_REQUIRED","campos obrigatórios de pedidos"); validate(pedidos,"pedidos","pedido_id","status NOT IN ('pago','cancelado')","INVALID_DOMAIN","status de pedido"); pedidos=dedup(pedidos,"pedidos","pedido_id",["pedido_id","data_pedido","cliente_id","canal_id","status"])
else: pedidos=previous("pedidos")
if "itens" in present:
    itens=load("itens","item_id STRING, pedido_id STRING, produto_id STRING, quantidade STRING, preco_unitario STRING",["item_id","pedido_id","produto_id",F.col("quantidade").cast("int").alias("quantidade"),F.regexp_replace("preco_unitario",",", ".").cast("decimal(12,2)").alias("preco_unitario")],["item_id","pedido_id","produto_id","quantidade","preco_unitario"],True); validate(itens,"itens","item_id","item_id IS NULL OR pedido_id IS NULL OR produto_id IS NULL OR quantidade IS NULL OR preco_unitario IS NULL","INVALID_TYPE","tipos de item"); validate(itens,"itens","item_id","quantidade <= 0 OR preco_unitario < 0","INVALID_DOMAIN","domínio de item"); itens=dedup(itens,"itens","item_id",["item_id","pedido_id","produto_id","quantidade","preco_unitario"])
else: itens=previous("itens")
if "clientes" in present:
    clientes=load("clientes","cliente_id STRING, cliente STRING, regiao_id STRING, regiao STRING, estado STRING",["cliente_id","cliente","regiao_id","regiao","estado"],["cliente_id","cliente","regiao_id","regiao","estado"],True); validate(clientes,"clientes","cliente_id","cliente_id IS NULL OR cliente IS NULL OR regiao_id IS NULL OR regiao IS NULL OR estado IS NULL","NULL_REQUIRED","campos de cliente"); clientes=dedup(clientes,"clientes","cliente_id",["cliente_id","cliente","regiao_id","regiao","estado"])
else: clientes=previous("clientes")
if "produtos" in present:
    produtos=load("produtos","produto_id STRING, produto STRING, categoria STRING",["produto_id","produto","categoria"],["produto_id","produto","categoria"],True); validate(produtos,"produtos","produto_id","produto_id IS NULL OR produto IS NULL OR categoria IS NULL","NULL_REQUIRED","campos de produto"); produtos=dedup(produtos,"produtos","produto_id",["produto_id","produto","categoria"])
else: produtos=previous("produtos")
if "pagamentos" in present:
    hist=load("pagamentos","evento_pagamento_id STRING, pedido_id STRING, resultado STRING, atualizado_em STRING",["evento_pagamento_id","pedido_id","resultado",F.to_timestamp("atualizado_em").alias("atualizado_em")],["evento_pagamento_id","pedido_id","resultado","atualizado_em"]); validate(hist,"pagamentos","evento_pagamento_id","evento_pagamento_id IS NULL OR pedido_id IS NULL OR atualizado_em IS NULL","INVALID_TYPE","campos de pagamento"); validate(hist,"pagamentos","evento_pagamento_id","resultado NOT IN ('aprovado','estornado','pendente','sem_cobranca')","INVALID_DOMAIN","resultado de pagamento"); hist=dedup(hist,"pagamentos","evento_pagamento_id",["evento_pagamento_id","pedido_id","resultado","atualizado_em"]); w=Window.partitionBy("pedido_id").orderBy(F.col("atualizado_em").desc(),F.col("evento_pagamento_id").desc()); pagamentos=hist.withColumn("rn",F.row_number().over(w)).filter("rn=1").drop("rn")
else: hist,pagamentos=previous("historico_pagamentos"),previous("pagamentos")
if "eventos" in present:
    eventos=load("eventos","evento_id STRING, sessao STRING, evento STRING, pedido_id STRING, dia STRING, canal_id STRING",["evento_id","sessao","evento","pedido_id",F.to_date("dia").alias("data"),"canal_id"],["evento_id","sessao","evento","pedido_id","data","canal_id"]); validate(eventos,"eventos","evento_id","evento_id IS NULL OR sessao IS NULL OR evento IS NULL OR data IS NULL OR canal_id IS NULL","NULL_REQUIRED","campos de evento"); eventos=dedup(eventos,"eventos","evento_id",["evento_id","sessao","evento","pedido_id","data","canal_id"])
else: eventos=previous("eventos")

if bronze_manifest["operation"] == "incremental":
    for name in ("pedidos","itens","clientes","produtos","eventos"):
        if name in present:
            incoming,old,key=locals()[name],previous(name),KEYS[name]; change_metrics(name,incoming,old,key,[x for x in re.findall(r"([a-z_]+)\s+(?:STRING|DATE|TIMESTAMP|INT|DECIMAL\(\d+,\d+\))",SCHEMAS[name]) if x != "origem_arquivo"]); globals()[name]=incoming.unionByName(old.join(incoming.select(key),key,"left_anti"),allowMissingColumns=True)
    if "pagamentos" in present:
        old=previous("historico_pagamentos"); change_metrics("pagamentos",hist,old,"evento_pagamento_id",["evento_pagamento_id","pedido_id","resultado","atualizado_em"]); hist=hist.unionByName(old.join(hist.select("evento_pagamento_id"),"evento_pagamento_id","left_anti"),allowMissingColumns=True); w=Window.partitionBy("pedido_id").orderBy(F.col("atualizado_em").desc(),F.col("evento_pagamento_id").desc()); pagamentos=hist.withColumn("rn",F.row_number().over(w)).filter("rn=1").drop("rn")
for missing,dataset,key,label in [(itens.join(pedidos.select("pedido_id"),"pedido_id","left_anti"),"itens","item_id","item sem pedido"),(itens.join(produtos.select("produto_id"),"produto_id","left_anti"),"itens","item_id","item sem produto"),(pedidos.join(clientes.select("cliente_id"),"cliente_id","left_anti"),"pedidos","pedido_id","pedido sem cliente")]:
    if missing.limit(1).count():
        if "occurrence_id" in missing.columns: record_anomaly(missing,dataset,"MISSING_REFERENCE",key,label)
        fail(label)

payload={"pedidos":pedidos,"itens":itens,"clientes":clientes,"produtos":produtos,"historico_pagamentos":hist,"pagamentos":pagamentos,"eventos":eventos}
for name,frame in payload.items():
    trigger="pagamentos" if name=="historico_pagamentos" else name
    output_rows=frame.count()
    if trigger in present:
        columns=re.findall(r"([a-z_]+)\s+(?:STRING|DATE|TIMESTAMP|INT|DECIMAL\(\d+,\d+\))",SCHEMAS[name]); fq=f"{cat}.{name}"; spark.sql(f"CREATE TABLE IF NOT EXISTS {fq} ({SCHEMAS[name]}) USING iceberg LOCATION 's3://{args['SILVER']}/{name}/' TBLPROPERTIES ('format-version'='2')"); frame.select(*columns).writeTo(fq).overwritePartitions(); m=metrics[trigger]; accepted=m["input_rows"]-m["duplicate_rows"]-m["invalid_rows"]-m["conflicting_rows"]; m.update({"accepted_rows":accepted,"output_rows":output_rows,"inserted_rows":m.get("inserted_rows",accepted if bronze_manifest["operation"] == "initial" else 0),"updated_rows":m.get("updated_rows",0),"unchanged_rows":m.get("unchanged_rows",0),"validation_status":"PASS"})
inputs=[{"key":x["s3_key"],"version_id":x["version_id"],"sha256":x["sha256"],"dataset_type":x.get("dataset_type") or x["s3_key"].split("/")[0]} for x in files]
if previous_manifest: inputs=list({(x["key"],x["version_id"]):x for x in previous_manifest["bronze_objects"]+inputs}.values())
metadata={}; snapshots={}
for name in payload:
    if name in present or (name=="historico_pagamentos" and "pagamentos" in present):
        table=glue.get_table(DatabaseName=db,Name=name)["Table"]; metadata[name]=table.get("Parameters",{}).get("metadata_location"); snapshots[name]=str(spark.sql(f"SELECT snapshot_id FROM {cat}.{name}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0])
    else: metadata[name],snapshots[name]=previous_manifest["iceberg_metadata_locations"][name],previous_manifest["snapshot_ids"][name]
quality_uri,anomalies_uri=persist_quality("PASS")
manifest={"layer":"silver","run_id":args["RUN_ID"],"bronze_manifest":f"s3://{args['RESULTS']}/{args['BRONZE_MANIFEST']}","bronze_objects":inputs,"iceberg_metadata_locations":metadata,"snapshot_ids":snapshots,"batch_id":bronze_manifest["batch_id"],"attempt_id":attempt_id,"tables_updated":sorted(present|({"historico_pagamentos"} if "pagamentos" in present else set())),"started_at":started_at,"finished_at":datetime.now(timezone.utc).isoformat(),"status":"SUCCEEDED","glue_job_run_id":job_run_id,"quality_schema_version":"1","quality_status":"PASS","quality_summary":metrics,"quality_report_uri":quality_uri,"anomalies_uri":anomalies_uri}
s3.put_object(Bucket=args["RESULTS"],Key=f"manifestos/silver/{args['RUN_ID']}.json",Body=json.dumps(manifest,default=str,indent=2).encode(),ContentType="application/json")
job.commit()
