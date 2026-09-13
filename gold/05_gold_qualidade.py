# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Qualidade de dados e governança
# MAGIC
# MAGIC Promove as tabelas construídas no notebook 6 da Silver (quarentena consolidada) para a
# MAGIC Gold, prontas pra Página 3 do dashboard ("Volume processado por safra, registros
# MAGIC rejeitados por motivo, chaves órfãs por dimensão, devoluções sem pedido de origem, e
# MAGIC data/hora da última atualização").
# MAGIC
# MAGIC Sem transformação nenhuma além do próprio propósito da camada Gold (deixar pronto pro
# MAGIC consumo direto do Power BI) — os dados já foram calculados e validados na Silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


def silver(table: str):
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")


def save_gold(df, table: str):
    target = f"{CATALOG}.{GOLD_SCHEMA}.{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"OK  {target:35s} ({df.count()} linhas)")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## Tabelas de passthrough direto (já no formato final desde a Silver)

# COMMAND ----------

save_gold(silver("qualidade_volume_processado"), "qualidade_volume_processado")
save_gold(silver("qualidade_rejeicoes"), "qualidade_rejeicoes")
save_gold(silver("qualidade_chaves_orfas"), "qualidade_chaves_orfas")
save_gold(silver("qualidade_chaves_ambiguas"), "qualidade_chaves_ambiguas")
save_gold(silver("devolucoes_orfas"), "devolucoes_orfas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados de atualização — `atualizado_em` é reescrito aqui
# MAGIC
# MAGIC `periodo_inicio`/`periodo_fim` (o que a base cobre) continuam vindo da Silver, sem mudança.
# MAGIC `atualizado_em` é atualizado pra refletir o momento da execução da Gold, não da Silver —
# MAGIC é essa a informação que interessa pra quem olha o dashboard ("dado atualizado até quando").

# COMMAND ----------

df_metadata = silver("qualidade_metadata_atualizacao").drop("atualizado_em").withColumn(
    "atualizado_em", F.current_timestamp()
)
save_gold(df_metadata, "qualidade_metadata_atualizacao")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

for t in [
    "qualidade_volume_processado", "qualidade_rejeicoes", "qualidade_chaves_orfas",
    "qualidade_chaves_ambiguas", "devolucoes_orfas", "qualidade_metadata_atualizacao",
]:
    n_silver = silver(t).count()
    n_gold = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{t}").count()
    print(f"  {t:35s} silver={n_silver:>4}  gold={n_gold:>4}  diferença={n_silver - n_gold}")

print()
print("Metadados de atualização (gold):")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.qualidade_metadata_atualizacao").show(truncate=False)
