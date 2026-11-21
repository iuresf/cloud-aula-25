"""Manutenção de snapshots Iceberg; o agendamento começa desabilitado."""
import sys
from datetime import datetime, timedelta, timezone
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["DATABASE"])
spark = GlueContext(SparkContext.getOrCreate()).spark_session
db = args["DATABASE"]
cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
for table in ("fato_itens_venda", "fato_sessoes"):
    target = f"glue_catalog.{db}.{table}"
    spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{target}')")
    spark.sql(f"CALL glue_catalog.system.expire_snapshots(table => '{target}', older_than => TIMESTAMP '{cutoff}')")
    orphan_cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"CALL glue_catalog.system.remove_orphan_files(table => '{target}', older_than => TIMESTAMP '{orphan_cutoff}')")
