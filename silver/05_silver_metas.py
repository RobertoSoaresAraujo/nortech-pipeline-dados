# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Fato de metas comerciais
# MAGIC
# MAGIC Aplica **R7**: meta é definida por **região × segmento × mês**, em BRL, sem desdobramento
# MAGIC por cliente, produto ou vendedor. Desnormaliza o layout matricial da planilha da Diretoria
# MAGIC (uma coluna por mês) para o formato tabular, e respeita a regra da aba `Premissas`: a
# MAGIC **Meta Ano prevalece sobre a soma dos meses** — por isso guardamos as duas em tabelas
# MAGIC separadas, sem forçar uma a bater com a outra.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
import re
import unicodedata


def bronze(table: str):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}")

# COMMAND ----------

# MAGIC %md ## Funções reutilizadas do notebook 1 (segmento/região) e do notebook 3 (valores em R$)

# COMMAND ----------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _normalize_key(v) -> str:
    if v is None:
        return ""
    v = _strip_accents(str(v)).strip().lower()
    v = re.sub(r"[.\-]", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v


SEGMENTO_MAP = {
    "atacado": "Atacado", "atac": "Atacado",
    "governo": "Governo", "gov": "Governo", "poder publico": "Governo",
    "industria": "Industria", "ind": "Industria",
    "varejo": "Varejo",
}

REGIAO_MAP = {
    "sul": "Sul",
    "norte": "Norte",
    "nordeste": "Nordeste", "ne": "Nordeste", "nord este": "Nordeste",
    "sudeste": "Sudeste", "se": "Sudeste", "sudest": "Sudeste",
    "centro oeste": "Centro-Oeste", "co": "Centro-Oeste",
}


@F.udf(returnType=StringType())
def normalize_segmento(v):
    return SEGMENTO_MAP.get(_normalize_key(v))


@F.udf(returnType=StringType())
def normalize_regiao(v):
    return REGIAO_MAP.get(_normalize_key(v))


# Valores de meta vêm como texto puro ("2599100.0") ou formatados em R$ ("R$ 1.135.400,00") —
# mesma mistura já vista em vendas_2024/2025/2026.
@F.udf(returnType=DoubleType())
def parse_valor_brl(value):
    if value is None:
        return None
    v = str(value).strip()
    if v == "" or v.lower() == "nan":
        return None
    v = v.replace("R$", "").strip()
    if "," in v:
        v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return None


MESES_NUMERO = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
                 "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Padronização por ano + separação da linha de totalização
# MAGIC
# MAGIC A planilha traz, depois das 20 linhas de dado (5 regiões × 4 segmentos), uma linha
# MAGIC `TOTAL GERAL` (soma de todas as regiões/segmentos), uma linha em branco, e uma observação
# MAGIC sobre o segmento Governo. Filtrar por "segmento reconhecido" já exclui as três de uma vez —
# MAGIC mas guardamos a `TOTAL GERAL` à parte só para conferência cruzada na validação.

# COMMAND ----------

def carregar_ano(tabela_bronze: str, ano: int):
    df_bruto = bronze(tabela_bronze)
    df = df_bruto.withColumn("ano", F.lit(ano))
    df = df.withColumn("regiao", normalize_regiao(F.col("Regiao"))).withColumn(
        "segmento", normalize_segmento(F.col("Segmento"))
    )
    dados = df.filter(F.col("segmento").isNotNull())  # descarta TOTAL GERAL, linha em branco, observação

    # Extraída a partir de uma leitura fresca do bronze (não do "df" acima) — isolar essa consulta
    # evitou um comportamento não explicado em que o filtro não encontrava a linha quando
    # encadeado depois das colunas geradas por UDF (regiao/segmento).
    total_geral = df_bruto.withColumn("ano", F.lit(ano)).filter(F.upper(F.trim(F.col("Regiao"))) == "TOTAL GERAL")
    return dados, total_geral


dados_2024, total_2024 = carregar_ano("metas_comerciais_2024", 2024)
dados_2025, total_2025 = carregar_ano("metas_comerciais_2025", 2025)
dados_2026, total_2026 = carregar_ano("metas_comerciais_2026", 2026)

df_dados = dados_2024.unionByName(dados_2025).unionByName(dados_2026)
df_total_geral = total_2024.unionByName(total_2025).unionByName(total_2026)

# COMMAND ----------

# MAGIC %md ## Meta anual (grão: ano × região × segmento) — a que prevalece, conforme a Premissas

# COMMAND ----------

df_metas_anuais = df_dados.select(
    "ano", "regiao", "segmento",
    parse_valor_brl("Meta_Ano").alias("meta_ano"),
)

target_anuais = f"{CATALOG}.{SILVER_SCHEMA}.metas_anuais"
df_metas_anuais.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_anuais)
print(f"OK  {target_anuais}  ({df_metas_anuais.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## Meta mensal (grão: ano × região × segmento × mês) — desnormalização do layout matricial

# COMMAND ----------

df_metas_mensais = df_dados.select(
    "ano", "regiao", "segmento",
    F.expr(
        """stack(12,
        'jan', Jan, 'fev', Fev, 'mar', Mar, 'abr', Abr, 'mai', Mai, 'jun', Jun,
        'jul', Jul, 'ago', Ago, 'set', Set, 'out', Out, 'nov', Nov, 'dez', Dez
        ) as (mes_abrev, meta_mensal_raw)"""
    ),
)

mapping_mes = F.create_map([F.lit(x) for pair in MESES_NUMERO.items() for x in pair])

df_metas_mensais = (
    df_metas_mensais
    .withColumn("mes", mapping_mes[F.col("mes_abrev")])
    .withColumn("meta_mensal", parse_valor_brl("meta_mensal_raw"))
    .withColumn(
        "data_referencia",
        F.to_date(
            F.concat_ws("-", F.col("ano").cast("string"), F.lpad(F.col("mes").cast("string"), 2, "0"), F.lit("01"))
        ),
    )
    .drop("mes_abrev", "meta_mensal_raw")
)

target_mensais = f"{CATALOG}.{SILVER_SCHEMA}.metas_mensais"
df_metas_mensais.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_mensais)
print(f"OK  {target_mensais}  ({df_metas_mensais.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

print("Linhas por ano em metas_anuais (esperado: 20 por ano = 5 regiões x 4 segmentos):")
df_metas_anuais.groupBy("ano").count().orderBy("ano").show()

print("Linhas por ano em metas_mensais (esperado: 240 por ano = 20 x 12 meses):")
df_metas_mensais.groupBy("ano").count().orderBy("ano").show()

print("Região/segmento sem mapeamento (deveria ser 0 linhas):")
df_dados.filter(F.col("regiao").isNull() | F.col("segmento").isNull()).select("ano", "Regiao", "Segmento").show()

print("Meta Ano nula após parsing (deveria ser 0 linhas):")
df_metas_anuais.filter(F.col("meta_ano").isNull()).show()

print("Meta mensal nula após parsing — ATENÇÃO: isso é esperado, não é erro.")
print("O dicionário é explícito: célula vazia = meta não definida para a combinação (≠ meta zero).")
print("Confirma se o padrão faz sentido (ex: só aparece em Governo, região menor, meses de fechamento de trimestre):")
df_metas_mensais.filter(F.col("meta_mensal").isNull()).groupBy("regiao", "segmento", "mes").count().orderBy("regiao", "mes").show(40)

print("Conferência visual de data_referencia (mês precisa estar com 2 dígitos, ex: 2024-01-01):")
df_metas_mensais.select("ano", "mes", "data_referencia").distinct().orderBy("ano", "mes").show(36)

print("Conferência: soma(meta_mensal) por ano+mês, comparado com a linha TOTAL GERAL do arquivo:")

# Diagnóstico — isola em qual etapa a linha está sumindo
print(f"  [debug] df_total_geral.count() = {df_total_geral.count()}  (esperado: 3, uma por ano)")
df_total_geral.select("ano", "Regiao", "Jan").show(truncate=False)

soma_calculada = df_metas_mensais.groupBy("ano", "mes").agg(F.sum("meta_mensal").alias("soma_calculada"))
print(f"  [debug] soma_calculada.count() = {soma_calculada.count()}  (esperado: 36 = 3 anos x 12 meses)")

total_arquivo = (
    df_total_geral.select(
        "ano",
        F.expr(
            """stack(12,
            'jan', Jan, 'fev', Fev, 'mar', Mar, 'abr', Abr, 'mai', Mai, 'jun', Jun,
            'jul', Jul, 'ago', Ago, 'set', Set, 'out', Out, 'nov', Nov, 'dez', Dez
            ) as (mes_abrev, total_raw)"""
        ),
    )
    .withColumn("mes", mapping_mes[F.col("mes_abrev")])
    .withColumn("total_arquivo", parse_valor_brl("total_raw"))
    .select("ano", "mes", "total_arquivo")
)
print(f"  [debug] total_arquivo.count() = {total_arquivo.count()}  (esperado: 36 = 3 anos x 12 meses)")
print(f"  [debug] soma_calculada.dtypes = {soma_calculada.dtypes}")
print(f"  [debug] total_arquivo.dtypes = {total_arquivo.dtypes}")

soma_calculada.join(total_arquivo, ["ano", "mes"]).withColumn(
    "diferenca", F.round(F.col("soma_calculada") - F.col("total_arquivo"), 2)
).orderBy("ano", "mes").show(40)

print("Diferença entre Meta Ano (planilha) e soma dos 12 meses (esperado: pode divergir — Meta Ano prevalece, conforme Premissas):")
df_metas_mensais.groupBy("ano", "regiao", "segmento").agg(F.sum("meta_mensal").alias("soma_meses")).join(
    df_metas_anuais, ["ano", "regiao", "segmento"]
).withColumn("diferenca", F.round(F.col("meta_ano") - F.col("soma_meses"), 2)).filter(
    F.col("diferenca") != 0
).orderBy("ano", "regiao", "segmento").show(40, truncate=False)
