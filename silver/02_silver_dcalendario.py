# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — dCalendario
# MAGIC
# MAGIC O case pede explicitamente: "Não há tabela calendário pronta: construa a sua." Esta tabela
# MAGIC cobre um intervalo de datas fixo (folgado o bastante pra abranger o histórico de
# MAGIC `carteira_historica` e o ano fiscal corrente por inteiro, mesmo com a base de vendas
# MAGIC terminando em 12/08/2026 — R11), com ano fiscal abril→março, trimestre fiscal e flag de
# MAGIC dia útil nacional.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
dbutils.widgets.text("data_inicio", "2023-01-01", "Data inicial do calendário")
dbutils.widgets.text("data_fim", "2027-03-31", "Data final do calendário (cobre a FY2027 inteira)")

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = "silver"
DATA_INICIO = dbutils.widgets.get("data_inicio")
DATA_FIM = dbutils.widgets.get("data_fim")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regra do ano fiscal (abril → março)
# MAGIC
# MAGIC `FY2026 = abr/2025 a mar/2026` (R6 do case). Ou seja: meses de abril a dezembro pertencem
# MAGIC ao ano fiscal **seguinte** ao ano civil; meses de janeiro a março pertencem ao **mesmo**
# MAGIC ano civil. Daí: `ano_fiscal = ano + 1` se `mes >= 4`, senão `ano_fiscal = ano`.
# MAGIC
# MAGIC O trimestre fiscal segue o mesmo deslocamento: Q1 fiscal = abr-mai-jun, Q4 fiscal = jan-fev-mar.

# COMMAND ----------

df_calendario = (
    spark.sql(f"SELECT explode(sequence(to_date('{DATA_INICIO}'), to_date('{DATA_FIM}'), interval 1 day)) AS data")
    .withColumn("data_sk", F.date_format("data", "yyyyMMdd").cast("int"))  # chave substituta p/ modelagem estrela
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("nome_mes", F.date_format("data", "MMMM"))
    .withColumn("nome_mes_abrev", F.date_format("data", "MMM"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("nome_dia_semana", F.date_format("data", "EEEE"))
    .withColumn("numero_dia_semana", F.dayofweek("data"))  # 1 = domingo ... 7 = sábado
    .withColumn("flag_fim_de_semana", F.col("numero_dia_semana").isin([1, 7]))
    # --- Ano fiscal (R6: abril a março) ---
    .withColumn("ano_fiscal", F.when(F.col("mes") >= 4, F.col("ano") + 1).otherwise(F.col("ano")))
    .withColumn("mes_fiscal", F.when(F.col("mes") >= 4, F.col("mes") - 3).otherwise(F.col("mes") + 9))
    .withColumn("trimestre_fiscal", F.ceil(F.col("mes_fiscal") / 3).cast("int"))
    .withColumn(
        "label_ano_fiscal", F.concat(F.lit("FY"), F.col("ano_fiscal").cast("string"))
    )
)

# COMMAND ----------

# MAGIC %md ## Flag de dia útil (usando feriados.csv, nível nacional)
# MAGIC
# MAGIC O dicionário lista feriados nacionais e estaduais (`abrangencia`). Como a distribuidora
# MAGIC atua em 5 regionais/vários estados, um único flag "dia útil" por data só é 100% correto
# MAGIC em nível **nacional** — feriado estadual exigiria um calendário por UF (cruzando com a UF
# MAGIC do cliente), o que é tratado aqui como decisão consciente de escopo, não esquecimento:
# MAGIC mantemos `flag_dia_util` calculado com feriados nacionais + fins de semana, e deixamos a
# MAGIC lista de feriados estaduais disponível à parte (`silver.feriados`) para quem quiser
# MAGIC estender depois.

# COMMAND ----------

df_feriados_nacionais = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.feriados")
    .filter(F.col("abrangencia") == "Nacional")
    .select(F.col("data").alias("data_feriado"), F.col("descricao").alias("nome_feriado"))
)

df_calendario = (
    df_calendario
    .join(df_feriados_nacionais, df_calendario.data == df_feriados_nacionais.data_feriado, "left")
    .withColumn("flag_feriado_nacional", F.col("data_feriado").isNotNull())
    .withColumn("flag_dia_util", ~F.col("flag_fim_de_semana") & ~F.col("flag_feriado_nacional"))
    .drop("data_feriado")
)

# COMMAND ----------

target = f"{CATALOG}.{SILVER_SCHEMA}.dcalendario"
(
    df_calendario.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target)
)
print(f"OK  {target}  ({df_calendario.count()} linhas, {DATA_INICIO} a {DATA_FIM})")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

print("Amostra ao redor da virada do ano fiscal (mar->abr/2026):")
df_calendario.filter((F.col("data") >= "2026-03-28") & (F.col("data") <= "2026-04-03")).select(
    "data", "ano", "mes", "ano_fiscal", "label_ano_fiscal", "mes_fiscal", "trimestre_fiscal", "nome_dia_semana"
).orderBy("data").show(truncate=False)

print("Feriados nacionais marcados:")
df_calendario.filter(F.col("flag_feriado_nacional")).select("data", "nome_mes", "ano").orderBy("data").show(40, truncate=False)

print("Contagem de dias úteis por ano fiscal:")
df_calendario.groupBy("label_ano_fiscal").agg(
    F.count("*").alias("dias_total"),
    F.sum(F.col("flag_dia_util").cast("int")).alias("dias_uteis"),
).orderBy("label_ano_fiscal").show()
