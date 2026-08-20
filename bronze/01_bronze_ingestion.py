# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — Nortech Distribuidora
# MAGIC
# MAGIC Notebook genérico, orientado a metadados: cada arquivo bruto vira uma tabela Delta na
# MAGIC camada Bronze **sem nenhuma limpeza** — apenas landing (pouso) + colunas técnicas de
# MAGIC rastreabilidade. Tudo é lido como `string`, exatamente como está na fonte.
# MAGIC
# MAGIC Regra do Bronze: se está sujo na origem, chega sujo aqui. Tratamento de tipo, formato de
# MAGIC data, moeda, etc. é responsabilidade da camada Silver.
# MAGIC
# MAGIC Fonte das características de cada arquivo (encoding, delimitador): `01_DICIONARIO_DE_DADOS.md`.

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
dbutils.widgets.text("raw_volume_path", "/Volumes/workspace/bronze/raw_files", "Caminho dos arquivos brutos no Volume")

CATALOG = dbutils.widgets.get("catalog")
RAW_PATH = dbutils.widgets.get("raw_volume_path")
BRONZE_SCHEMA = "bronze"

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone
import pandas as pd

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados de ingestão
# MAGIC
# MAGIC Um registro por arquivo fonte. Evita "adivinhar" separador/encoding espalhado pelo código —
# MAGIC toda a heterogeneidade das 3 safras de ERP fica documentada em um único lugar.

# COMMAND ----------

INGESTION_METADATA = [
    {"source_file": "cambio_usd.csv",         "target_table": "cambio_usd",         "encoding": "UTF-8",      "delimiter": ",", "bom": False},
    {"source_file": "carteira_historica.csv", "target_table": "carteira_historica", "encoding": "UTF-8",      "delimiter": ";", "bom": False},
    {"source_file": "clientes.csv",           "target_table": "clientes",           "encoding": "UTF-8",      "delimiter": ";", "bom": False},
    {"source_file": "devolucoes.csv",         "target_table": "devolucoes",         "encoding": "UTF-8",      "delimiter": ";", "bom": False},
    {"source_file": "feriados.csv",           "target_table": "feriados",           "encoding": "UTF-8",      "delimiter": ";", "bom": False},
    {"source_file": "produtos.csv",           "target_table": "produtos",           "encoding": "UTF-8",      "delimiter": ";", "bom": False},
    {"source_file": "seguranca_acessos.csv",  "target_table": "seguranca_acessos",  "encoding": "UTF-8",      "delimiter": ";", "bom": False},
    # vendas_2024 vem do ERP legado: encoding ISO-8859-1 (latin-1), conforme dicionário.
    {"source_file": "vendas_2024.csv",        "target_table": "vendas_2024",        "encoding": "ISO-8859-1", "delimiter": ";", "bom": False},
    {"source_file": "vendas_2025.csv",        "target_table": "vendas_2025",        "encoding": "UTF-8",      "delimiter": ",", "bom": False},
    # vendas_2026 vem com BOM (UTF-8 com BOM) e delimitador diferente das outras safras.
    {"source_file": "vendas_2026.csv",        "target_table": "vendas_2026",        "encoding": "UTF-8",      "delimiter": "|", "bom": True},
    {"source_file": "vendedores.csv",         "target_table": "vendedores",         "encoding": "UTF-8",      "delimiter": ";", "bom": False},
]

# COMMAND ----------

# MAGIC %md ## Ingestão genérica dos arquivos CSV

# COMMAND ----------

def ingest_csv(meta: dict) -> None:
    source_path = f"{RAW_PATH}/{meta['source_file']}"
    target = f"{CATALOG}.{BRONZE_SCHEMA}.{meta['target_table']}"

    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", meta["delimiter"])
        .option("encoding", meta["encoding"])
        .option("inferSchema", "false")   # tudo como string — sem conversão de tipo no Bronze
        .option("multiLine", "true")
        .csv(source_path)
    )

    # Remove BOM residual que às vezes gruda no nome da primeira coluna (ex: vendas_2026.csv)
    if meta["bom"] and df.columns:
        first_col = df.columns[0]
        clean_col = first_col.lstrip("\ufeff")
        if clean_col != first_col:
            df = df.withColumnRenamed(first_col, clean_col)

    df = (
        df
        .withColumn("_source_file", F.lit(meta["source_file"]))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    print(f"OK  {meta['source_file']:28s} -> {target}  ({df.count()} linhas)")


for meta in INGESTION_METADATA:
    ingest_csv(meta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão do Excel — `metas_comerciais.xlsx`
# MAGIC
# MAGIC Particularidades (ver dicionário):
# MAGIC - 3 linhas de título/observação antes do cabeçalho real → **cabeçalho está na linha 4**
# MAGIC   (índice 3 no pandas, já que a contagem começa em 0)
# MAGIC - uma aba por ano (`Metas 2024`, `Metas 2025`, `Metas 2026`)
# MAGIC - existe uma linha de totalização ao final da grade — no Bronze ela **não é removida**,
# MAGIC   só fica junto (a decisão de excluir ou não é da Silver, e precisa ser documentada lá)
# MAGIC - a aba `Premissas` é só texto corrido e vira uma tabela de documentação à parte,
# MAGIC   não um fato — mas registra regras de negócio importantes (conversão cambial, ano fiscal)
# MAGIC   que valem a pena estar consultáveis via SQL

# COMMAND ----------

xlsx_local_path = f"{RAW_PATH}/metas_comerciais.xlsx"
year_sheets = ["Metas 2024", "Metas 2025", "Metas 2026"]

for sheet in year_sheets:
    pdf = pd.read_excel(xlsx_local_path, sheet_name=sheet, header=3)
    pdf = pdf.dropna(how="all")  # remove linhas 100% vazias, mas mantém a linha de totalização
    pdf = pdf.astype(str)
    pdf["_source_file"] = "metas_comerciais.xlsx"
    pdf["_source_sheet"] = sheet
    pdf["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    sdf = spark.createDataFrame(pdf)
    year = sheet.split()[-1]  # "2024", "2025" ou "2026"
    target = f"{CATALOG}.{BRONZE_SCHEMA}.metas_comerciais_{year}"

    sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"OK  metas_comerciais.xlsx [{sheet}] -> {target}  ({sdf.count()} linhas)")

# COMMAND ----------

pdf_premissas = pd.read_excel(xlsx_local_path, sheet_name="Premissas", header=None)
pdf_premissas.columns = ["texto_premissa"]
pdf_premissas = pdf_premissas.dropna(how="all").astype(str)
pdf_premissas["_source_file"] = "metas_comerciais.xlsx"
pdf_premissas["_ingested_at"] = datetime.now(timezone.utc).isoformat()

sdf_premissas = spark.createDataFrame(pdf_premissas)
target_premissas = f"{CATALOG}.{BRONZE_SCHEMA}.metas_premissas"
sdf_premissas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_premissas)
print(f"OK  metas_comerciais.xlsx [Premissas] -> {target_premissas}  ({sdf_premissas.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## Validação rápida

# COMMAND ----------

bronze_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").collect()
print(f"{len(bronze_tables)} tabelas criadas na camada Bronze:")
for t in bronze_tables:
    count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{t['tableName']}").count()
    print(f"  - {t['tableName']:28s} {count:>7} linhas")
