# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver — Fato de vendas consolidado
# MAGIC
# MAGIC Unifica `vendas_2024`, `vendas_2025` e `vendas_2026` (3 layouts de ERP diferentes) em um
# MAGIC único fato, aplicando:
# MAGIC - **R1** — Receita Bruta = quantidade × valor_unitário, convertida para BRL quando USD
# MAGIC - **R2** — Receita Líquida = Receita Bruta × (1 − desconto), com semântica de desconto
# MAGIC   diferente em cada safra
# MAGIC - **R3** — Conversão cambial pela PTAX de venda da data de emissão, com fallback pra
# MAGIC   última cotação disponível anterior quando não há pregão na data
# MAGIC - **R4** — Cancelamentos não são receita (quantidade negativa em 2024, `status_pedido`
# MAGIC   nas outras safras)
# MAGIC - **R12** — Registros sem quantidade, valor ou data válida não entram no fato, mas vão
# MAGIC   para uma tabela de rejeitados com o motivo (nunca somem em silêncio)
# MAGIC
# MAGIC Grão do fato: **safra + id_pedido + item** (o `safra` evita colisão entre pedidos de anos
# MAGIC diferentes que por acaso tenham o mesmo número).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DoubleType
from pyspark.sql.window import Window
import re
from datetime import date, timedelta


def bronze(table: str):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}")


def silver(table: str):
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")

# COMMAND ----------

# MAGIC %md ## Funções de parsing (mesmas do notebook 1, redefinidas aqui — cada notebook é autocontido)

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


# valor_unitario de 2024 mistura "135,33" puro com "R$ 5.478,44" (com milhar+símbolo) na mesma
# coluna — esse parser cobre os dois formatos, e também serve para 2025/2026 (decimal com ponto).
@F.udf(returnType=DoubleType())
def parse_valor_brl(value):
    if value is None:
        return None
    v = str(value).strip()
    if v == "" or v.lower() == "nan":
        return None
    v = v.replace("R$", "").strip()
    if "," in v:
        v = v.replace(".", "").replace(",", ".")  # remove milhar, troca decimal
    try:
        return float(v)
    except ValueError:
        return None

# COMMAND ----------

# MAGIC %md ## Padronização por safra

# COMMAND ----------

df_2024 = (
    bronze("vendas_2024")
    .select(
        F.trim("ID_PEDIDO").alias("id_pedido"),
        F.trim("ITEM").cast("int").alias("item"),
        F.lit("2024").alias("safra"),
        parse_flexible_date("DT_EMISSAO").alias("data_emissao"),
        F.trim("COD_CLIENTE").alias("id_cliente"),
        F.trim("COD_PRODUTO").alias("id_produto"),
        F.nullif(F.trim("COD_VENDEDOR"), F.lit("")).alias("id_vendedor"),
        F.trim("QTDE").cast("int").alias("quantidade"),
        parse_valor_brl("VLR_UNITARIO").alias("valor_unitario"),
        # DESCONTO em pontos percentuais (12,50 = 12.5%) -> fração
        (parse_valor_brl("DESCONTO") / F.lit(100.0)).alias("desconto_fracao"),
        F.trim("MOEDA").alias("moeda"),
        F.trim("CANAL").alias("canal"),
        F.lit(None).cast("string").alias("status_pedido_origem"),  # 2024 não tem essa coluna
        F.lit(None).cast("string").alias("id_filial"),             # não existe em 2024
        F.lit(None).cast("double").alias("frete_rateado"),         # não existe em 2024
    )
)

df_2025 = (
    bronze("vendas_2025")
    .select(
        F.trim("id_pedido").alias("id_pedido"),
        F.trim("item").cast("int").alias("item"),
        F.lit("2025").alias("safra"),
        parse_flexible_date("data_emissao").alias("data_emissao"),
        F.trim("id_cliente").alias("id_cliente"),
        F.trim("id_produto").alias("id_produto"),
        F.nullif(F.trim("id_vendedor"), F.lit("")).alias("id_vendedor"),
        F.trim("quantidade").cast("int").alias("quantidade"),
        parse_valor_brl("valor_unitario").alias("valor_unitario"),
        # desconto_pct já é fração decimal (0.1250 = 12.5%) -> usa direto
        parse_valor_brl("desconto_pct").alias("desconto_fracao"),
        F.trim("moeda").alias("moeda"),
        F.trim("canal").alias("canal"),
        F.trim("status_pedido").alias("status_pedido_origem"),
        F.lit(None).cast("string").alias("id_filial"),
        F.lit(None).cast("double").alias("frete_rateado"),
    )
)

df_2026 = (
    bronze("vendas_2026")
    .select(
        F.trim("id_pedido").alias("id_pedido"),
        F.trim("item").cast("int").alias("item"),
        F.lit("2026").alias("safra"),
        parse_flexible_date("data_emissao").alias("data_emissao"),  # sem padronização na origem
        F.trim("id_cliente").alias("id_cliente"),
        F.upper(F.trim("id_produto")).alias("id_produto"),  # módulo de filiais não preserva a caixa
        F.nullif(F.trim("id_vendedor"), F.lit("")).alias("id_vendedor"),
        F.trim("quantidade").cast("int").alias("quantidade"),
        parse_valor_brl("valor_unitario").alias("valor_unitario"),
        # desconto_pct aqui é pontos percentuais de novo (5.0 = 5%), diferente de 2025 -> fração
        (parse_valor_brl("desconto_pct") / F.lit(100.0)).alias("desconto_fracao"),
        F.trim("moeda").alias("moeda"),
        F.trim("canal").alias("canal"),
        F.trim("status_pedido").alias("status_pedido_origem"),
        F.trim("id_filial").alias("id_filial"),
        parse_valor_brl("frete_rateado").alias("frete_rateado"),
    )
)

df_vendas_raw = df_2024.unionByName(df_2025).unionByName(df_2026)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicidade exata (principalmente 2025)
# MAGIC
# MAGIC O dicionário avisa: a extração de `vendas_2025.csv` é feita por reprocessamento
# MAGIC incremental do ERP, **sem controle de idempotência na origem** — ou seja, o mesmo
# MAGIC pedido/item pode ter sido extraído mais de uma vez por engano. Remover isso ANTES da
# MAGIC conversão cambial e do cálculo de receita é essencial: linha duplicada em BRL dobraria a
# MAGIC receita silenciosamente, e em USD interagiria de forma imprevisível com o join de câmbio.
# MAGIC `dropDuplicates()` sem argumento remove só linhas 100% idênticas em todas as colunas —
# MAGIC não mexe em pedidos legítimos que apenas compartilham cliente/produto/data.

# COMMAND ----------

linhas_antes = df_vendas_raw.count()
df_vendas_raw = df_vendas_raw.dropDuplicates()
linhas_depois = df_vendas_raw.count()
print(f"Linhas antes: {linhas_antes}  |  depois de remover duplicatas exatas: {linhas_depois}  |  removidas: {linhas_antes - linhas_depois}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R12 — separar registros inválidos (sem quantidade, sem valor ou sem data válida)
# MAGIC
# MAGIC Não entram no fato, mas vão para `silver.vendas_rejeitadas` com o motivo — nada some
# MAGIC em silêncio.

# COMMAND ----------

df_vendas_raw = df_vendas_raw.withColumn(
    "motivo_rejeicao",
    F.concat_ws(
        "; ",
        F.when(F.col("quantidade").isNull(), F.lit("quantidade_ausente")),
        F.when(F.col("valor_unitario").isNull(), F.lit("valor_ausente")),
        F.when(F.col("data_emissao").isNull(), F.lit("data_invalida")),
    )
)

df_rejeitadas = df_vendas_raw.filter(F.col("motivo_rejeicao") != "")
df_validas = df_vendas_raw.filter(F.col("motivo_rejeicao") == "").drop("motivo_rejeicao")

target_rejeitadas = f"{CATALOG}.{SILVER_SCHEMA}.vendas_rejeitadas"
df_rejeitadas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_rejeitadas)
print(f"OK  {target_rejeitadas}  ({df_rejeitadas.count()} linhas rejeitadas)")

print("Motivos de rejeição por safra:")
df_rejeitadas.groupBy("safra", "motivo_rejeicao").count().orderBy("safra", "motivo_rejeicao").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## R3 — conversão cambial (PTAX de venda da data de emissão, com fallback)
# MAGIC
# MAGIC Como `cambio_usd` só tem cotação em dias de pregão, um join direto por data perderia
# MAGIC fins de semana e feriados. Em vez disso, para cada venda em USD, buscamos a **cotação
# MAGIC mais recente com `data_cotacao <= data_emissao`** (join por intervalo + `row_number`
# MAGIC pegando a mais próxima).

# COMMAND ----------

df_cambio = silver("cambio_usd").select("data_cotacao", "taxa_ptax_venda")

df_usd = df_validas.filter(F.col("moeda") == "USD")
df_brl = df_validas.filter(F.col("moeda") != "USD")

w_taxa = Window.partitionBy("safra", "id_pedido", "item").orderBy(F.col("data_cotacao").desc())

df_usd_convertido = (
    df_usd.join(F.broadcast(df_cambio), df_cambio.data_cotacao <= df_usd.data_emissao, "left")
    .withColumn("rn", F.row_number().over(w_taxa))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumnRenamed("taxa_ptax_venda", "taxa_cambio_aplicada")
    .drop("data_cotacao")
)

df_brl_com_taxa = df_brl.withColumn("taxa_cambio_aplicada", F.lit(1.0))

df_validas = df_usd_convertido.unionByName(df_brl_com_taxa)

print("Vendas USD sem cotação encontrada (deveria ser 0 — indicaria data anterior ao início do cambio_usd):")
df_validas.filter((F.col("moeda") == "USD") & F.col("taxa_cambio_aplicada").isNull()).select(
    "safra", "id_pedido", "item", "data_emissao"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## R4 — cancelamentos não são receita
# MAGIC
# MAGIC 2024 não tem coluna de status: cancelamento foi registrado com quantidade negativa.
# MAGIC 2025/2026 têm `status_pedido`. Unificamos num único `status_pedido` e usamos o valor
# MAGIC absoluto da quantidade para exibição, mas zeramos a receita de pedidos cancelados.

# COMMAND ----------

df_validas = (
    df_validas
    .withColumn(
        "status_pedido",
        F.when(F.col("safra") == "2024",
               F.when(F.col("quantidade") < 0, F.lit("Cancelado")).otherwise(F.lit("Faturado")))
        .otherwise(F.col("status_pedido_origem"))
    )
    .withColumn("quantidade_absoluta", F.abs(F.col("quantidade")))
    .drop("status_pedido_origem")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## R1 + R2 — Receita Bruta e Receita Líquida

# COMMAND ----------

df_validas = (
    df_validas
    .withColumn(
        "receita_bruta",
        F.when(F.col("status_pedido") == "Cancelado", F.lit(0.0))
        .otherwise(F.col("quantidade_absoluta") * F.col("valor_unitario") * F.col("taxa_cambio_aplicada")),
    )
    .withColumn("receita_liquida", F.col("receita_bruta") * (F.lit(1.0) - F.col("desconto_fracao")))
)

# COMMAND ----------

# MAGIC %md ## Chave robusta de produto + sinalização de vendedor ambíguo

# COMMAND ----------

df_validas = df_validas.withColumn("id_produto_chave", F.upper(F.trim(F.col("id_produto"))))

ids_vendedor_duplicados = [
    r["id_vendedor"]
    for r in silver("vendedores").filter(F.col("id_vendedor_duplicado")).select("id_vendedor").distinct().collect()
]
df_validas = df_validas.withColumn(
    "id_vendedor_ambiguo",
    F.col("id_vendedor").isin(ids_vendedor_duplicados) if ids_vendedor_duplicados else F.lit(False),
)

# COMMAND ----------

target_vendas = f"{CATALOG}.{SILVER_SCHEMA}.vendas"
df_validas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_vendas)
print(f"OK  {target_vendas}  ({df_validas.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

print("Linhas por safra (fato válido):")
df_validas.groupBy("safra").count().orderBy("safra").show()

print("Linhas rejeitadas por safra:")
df_rejeitadas.groupBy("safra").count().orderBy("safra").show()

print("Reconciliação: bronze == válidas + rejeitadas + duplicatas removidas (deve fechar por safra):")
for ano, tabela_bronze in [("2024", "vendas_2024"), ("2025", "vendas_2025"), ("2026", "vendas_2026")]:
    n_bronze = bronze(tabela_bronze).count()
    n_validas = df_validas.filter(F.col("safra") == ano).count()
    n_rejeitadas = df_rejeitadas.filter(F.col("safra") == ano).count()
    print(f"  {ano}: bronze={n_bronze}  válidas={n_validas}  rejeitadas={n_rejeitadas}  "
          f"soma={n_validas + n_rejeitadas}  diferença(duplicatas removidas)={n_bronze - (n_validas + n_rejeitadas)}")

print("Pedidos cancelados por safra (receita deve ser 0 em todos):")
df_validas.filter(F.col("status_pedido") == "Cancelado").groupBy("safra").agg(
    F.count("*").alias("qtd"), F.sum("receita_bruta").alias("receita_bruta_soma")
).orderBy("safra").show()

print("Receita líquida total por safra (conferência de sanidade):")
df_validas.groupBy("safra").agg(F.round(F.sum("receita_liquida"), 2).alias("receita_liquida_total")).orderBy("safra").show()

print("Vendas com id_vendedor ambíguo (V001):")
print(df_validas.filter(F.col("id_vendedor_ambiguo")).count())