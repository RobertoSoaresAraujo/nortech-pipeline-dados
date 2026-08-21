# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fMetas
# MAGIC
# MAGIC Grão: **região × segmento × mês** (R7). Sem chave órfã pra resolver aqui — região e
# MAGIC segmento sempre vêm válidos na planilha da Diretoria (só a dimensão `dRegiaoSegmento` já
# MAGIC cobre todas as combinações, verificado no notebook 1).
# MAGIC
# MAGIC **Atenção pra quem for escrever DAX em cima disso**: a coluna `meta_ano_referencia` é o
# MAGIC valor da Meta Ano **repetido em cada uma das 12 linhas mensais** da mesma
# MAGIC região×segmento×ano — ela existe pra facilitar consulta, mas **não pode ser somada
# MAGIC diretamente** (`SUM` ingênuo multiplicaria o valor por 12). Pra usar o valor anual de
# MAGIC verdade, sempre agregue por linha distinta antes de somar
# MAGIC (`SUMX(DISTINCT(região, segmento, ano), meta_ano_referencia)` ou equivalente).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


def silver(table: str):
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")


def gold(table: str):
    return spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table}")


def save_gold(df, table: str):
    target = f"{CATALOG}.{GOLD_SCHEMA}.{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"OK  {target:20s} ({df.count()} linhas)")

# COMMAND ----------

from pyspark.sql import functions as F

df_metas_mensais = silver("metas_mensais")
df_metas_anuais = silver("metas_anuais")

# COMMAND ----------

# MAGIC %md ## Chave substituta de região×segmento + Meta Ano denormalizada

# COMMAND ----------

dregiaosegmento_lookup = gold("dRegiaoSegmento").select(
    F.col("regiao").alias("_regiao_lookup"),
    F.col("segmento").alias("_segmento_lookup"),
    F.col("sk_regiao_segmento"),
)

df_fmetas = (
    df_metas_mensais
    .join(
        dregiaosegmento_lookup,
        (df_metas_mensais.regiao == F.col("_regiao_lookup")) & (df_metas_mensais.segmento == F.col("_segmento_lookup")),
        "left",
    )
    .drop("_regiao_lookup", "_segmento_lookup")
    .join(
        df_metas_anuais.select(
            "ano", "regiao", "segmento", F.col("meta_ano").alias("meta_ano_referencia")
        ),
        ["ano", "regiao", "segmento"],
        "left",
    )
)

# COMMAND ----------

# MAGIC %md ## Seleção final e gravação

# COMMAND ----------

df_fmetas_final = df_fmetas.select(
    "sk_regiao_segmento",
    "regiao", "segmento",  # denormalizados também aqui — conveniente pra debug/exploração ad-hoc
    "ano", "mes", "data_referencia",
    "meta_mensal",
    "meta_ano_referencia",
)

save_gold(df_fmetas_final, "fMetas")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

n_silver = silver("metas_mensais").count()
n_gold = df_fmetas_final.count()
print(f"Reconciliação: silver.metas_mensais={n_silver}  gold.fMetas={n_gold}  diferença={n_silver - n_gold}")

print("sk_regiao_segmento nulo após o join (deveria ser 0 linhas):")
df_fmetas_final.filter(F.col("sk_regiao_segmento").isNull()).show()

print("meta_ano_referencia nula após o join (deveria ser 0 linhas):")
df_fmetas_final.filter(F.col("meta_ano_referencia").isNull()).show()

print("Linhas por ano (esperado: 240 = 20 combinações x 12 meses):")
df_fmetas_final.groupBy("ano").count().orderBy("ano").show()

print("meta_mensal nula preservada (esperado: 24 — Governo em Norte/Centro-Oeste, meses de fechamento de trimestre):")
print(df_fmetas_final.filter(F.col("meta_mensal").isNull()).count())

print("Checagem do alerta de soma ingênua: SUM(meta_ano_referencia) vs valor real, pra uma combinação")
exemplo = df_fmetas_final.filter((F.col("ano") == 2024) & (F.col("regiao") == "Sul") & (F.col("segmento") == "Atacado"))
exemplo.agg(
    F.sum("meta_ano_referencia").alias("soma_ingenua_12x"),
    F.first("meta_ano_referencia").alias("valor_real"),
).show()
