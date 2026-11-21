"""Manutenção de snapshots Iceberg; o agendamento começa desabilitado."""
import sys
from datetime import datetime, timedelta, timezone
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["DATABASE"])
spark = GlueContext(SparkContext.getOrCreate()).spark_session
db = args["DATABASE"]
# Snapshots registrados em manifestos operacionais podem sustentar recuperações.
# A expiração automática fica suspensa até existir uma política que respeite
# os snapshots Silver e Gold publicados ou ainda recuperáveis.
for table in ("fato_itens_venda", "fato_sessoes"):
    target = f"glue_catalog.{db}.{table}"
    spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{target}')")
