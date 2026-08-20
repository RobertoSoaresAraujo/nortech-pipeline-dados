# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Fato de devoluções
# MAGIC
# MAGIC Aplica **R5**: devoluções são valorizadas pelo preço e desconto do **item de origem**
# MAGIC (não têm preço próprio no arquivo — só quantidade devolvida), reduzem a Receita Líquida
# MAGIC **na data da devolução** (não na data da venda), e devoluções cujo pedido de origem não
# MAGIC existe na base (vendas anteriores a 2024) são **isoladas e reportadas**, nunca descartadas.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
import re
from datetime import date, timedelta


def bronze(table: str):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}")


def silver(table: str):
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")

# COMMAND ----------

# MAGIC %md ## Parsing de data (mesma função dos notebooks anteriores)

# COMMAND ----------

MESES_PT = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
            "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}


@F.udf(returnType=DateType())
def parse_flexible_date(value):
    if value is None:
        return None
    v = str(value).strip()
    if v == "" or v.lower() == "nan":
        return None
    if re.fullmatch(r"\d{4,6}", v):
        try:
            return date(1899, 12, 30) + timedelta(days=int(v))
        except Exception:
            return None
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", v)
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", v)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", v)
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{1,2})/([a-zA-Z]{3})/(\d{2})", v)
    if m:
        d, mes_abbr, yy = m.groups()
        mo = MESES_PT.get(mes_abbr.lower())
        if mo:
            y = 2000 + int(yy)
            try:
                return date(y, mo, int(d))
            except ValueError:
                return None
    return None

# COMMAND ----------

# MAGIC %md ## Padronização de devolucoes

# COMMAND ----------

df_devolucoes_raw = (
    bronze("devolucoes")
    .select(
        F.trim("id_devolucao").alias("id_devolucao"),
        F.trim("id_pedido_origem").alias("id_pedido_origem"),
        F.trim("item").cast("int").alias("item"),
        parse_flexible_date("data_devolucao").alias("data_devolucao"),
        F.trim("quantidade_devolvida").cast("int").alias("quantidade_devolvida"),
        F.trim("motivo").alias("motivo"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## R5 — valorizar pelo item de origem + isolar órfãs
# MAGIC
# MAGIC `silver.vendas` já está com grão único por `id_pedido+item` dentro de cada safra, e
# MAGIC confirmamos nos dados que `id_pedido+item` não colide entre safras diferentes — então o
# MAGIC join abaixo não corre risco de multiplicar linha por engano.

# COMMAND ----------

df_origem = silver("vendas").select(
    F.col("id_pedido").alias("_id_pedido_origem"),
    F.col("item").alias("_item_origem"),
    F.col("safra").alias("safra_origem"),
    F.col("valor_unitario").alias("valor_unitario_origem"),
    F.col("desconto_fracao").alias("desconto_fracao_origem"),
    F.col("moeda").alias("moeda_origem"),
    F.col("taxa_cambio_aplicada").alias("taxa_cambio_origem"),
    F.col("status_pedido").alias("status_pedido_origem"),
    F.col("quantidade_absoluta").alias("quantidade_faturada_origem"),
)

df_devolucoes = (
    df_devolucoes_raw
    .join(
        df_origem,
        (df_devolucoes_raw.id_pedido_origem == df_origem._id_pedido_origem)
        & (df_devolucoes_raw.item == df_origem._item_origem),
        "left",
    )
    .drop("_id_pedido_origem", "_item_origem")
    .withColumn("pedido_origem_encontrado", F.col("safra_origem").isNotNull())
    .withColumn(
        "quantidade_excede_faturado",
        F.col("pedido_origem_encontrado")
        & (F.col("quantidade_devolvida") > F.col("quantidade_faturada_origem")),
    )
    .withColumn(
        "receita_devolucao_bruta",
        F.when(
            F.col("pedido_origem_encontrado"),
            F.col("quantidade_devolvida") * F.col("valor_unitario_origem") * F.col("taxa_cambio_origem"),
        ),  # NULL quando órfã — não dá pra valorizar sem o item de origem
    )
    .withColumn(
        "receita_devolucao_liquida",
        F.col("receita_devolucao_bruta") * (F.lit(1.0) - F.col("desconto_fracao_origem")),
    )
)

target_devolucoes = f"{CATALOG}.{SILVER_SCHEMA}.devolucoes"
df_devolucoes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_devolucoes)
print(f"OK  {target_devolucoes}  ({df_devolucoes.count()} linhas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Devoluções órfãs — tabela dedicada
# MAGIC
# MAGIC O case pede explicitamente, na Página 3 do dashboard: "devoluções sem pedido de origem
# MAGIC (R5)". Em vez de deixar essa informação espalhada, materializamos uma tabela só com elas.

# COMMAND ----------

df_devolucoes_orfas = df_devolucoes.filter(~F.col("pedido_origem_encontrado")).select(
    "id_devolucao", "id_pedido_origem", "item", "data_devolucao", "quantidade_devolvida", "motivo"
)

target_orfas = f"{CATALOG}.{SILVER_SCHEMA}.devolucoes_orfas"
df_devolucoes_orfas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_orfas)
print(f"OK  {target_orfas}  ({df_devolucoes_orfas.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

print("Devoluções com pedido de origem encontrado vs órfãs:")
df_devolucoes.groupBy("pedido_origem_encontrado").count().show()

print("Reconciliação: bronze == total (nenhuma devolução deve ter sumido):")
n_bronze = bronze("devolucoes").count()
n_silver = df_devolucoes.count()
print(f"  bronze={n_bronze}  silver={n_silver}  diferença={n_bronze - n_silver}")

print("Devoluções cuja quantidade devolvida excede a quantidade faturada na origem (deveria ser raro/zero):")
df_devolucoes.filter(F.col("quantidade_excede_faturado")).select(
    "id_devolucao", "id_pedido_origem", "item", "quantidade_devolvida", "quantidade_faturada_origem"
).show(truncate=False)

print("Devoluções referenciando pedido de origem CANCELADO (situação estranha — vale investigar se aparecer):")
df_devolucoes.filter(F.col("status_pedido_origem") == "Cancelado").select(
    "id_devolucao", "id_pedido_origem", "item", "status_pedido_origem"
).show(truncate=False)

print("Amostra de devoluções órfãs (para conferência):")
df_devolucoes_orfas.orderBy("data_devolucao").show(10, truncate=False)

print("Receita de devolução líquida total (valor a subtrair da receita líquida na Gold, por data_devolucao):")
df_devolucoes.filter(F.col("pedido_origem_encontrado")).agg(
    F.round(F.sum("receita_devolucao_liquida"), 2).alias("total_devolvido")
).show()
