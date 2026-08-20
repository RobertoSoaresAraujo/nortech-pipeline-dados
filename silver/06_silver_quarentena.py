# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Quarentena consolidada e qualidade de dados
# MAGIC
# MAGIC Junta tudo que já foi sinalizado ao longo do pipeline (R12, R5, chaves ambíguas) com uma
# MAGIC verificação sistemática de **integridade referencial** (chaves órfãs entre fato e dimensão)
# MAGIC que ainda não tinha sido feita de forma centralizada. Alimenta diretamente a Página 3 do
# MAGIC dashboard: "Volume processado por safra, registros rejeitados por motivo (R12), chaves
# MAGIC órfãs por dimensão, devoluções sem pedido de origem (R5)".
# MAGIC
# MAGIC Importante: aqui só **detectamos e quantificamos**. A decisão de como tratar cada chave
# MAGIC órfã no modelo final (ex: membro "Não Identificado", conforme pede a seção de modelagem
# MAGIC do case) é responsabilidade da camada Gold — misturar detecção com correção no mesmo
# MAGIC lugar dificultaria auditar os dois separadamente.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

from pyspark.sql import functions as F


def bronze(table: str):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}")


def silver(table: str):
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")

# COMMAND ----------

# MAGIC %md ## 1. Volume processado por safra

# COMMAND ----------

linhas_volume = []
for safra, tabela_bronze in [("2024", "vendas_2024"), ("2025", "vendas_2025"), ("2026", "vendas_2026")]:
    n_bronze = bronze(tabela_bronze).count()
    n_validas = silver("vendas").filter(F.col("safra") == safra).count()
    n_rejeitadas = silver("vendas_rejeitadas").filter(F.col("safra") == safra).count()
    linhas_volume.append(("vendas", safra, n_bronze, n_validas, n_rejeitadas, n_bronze - n_validas - n_rejeitadas))

n_dev_bronze = bronze("devolucoes").count()
n_dev_com_origem = silver("devolucoes").filter(F.col("pedido_origem_encontrado")).count()
n_dev_orfas = silver("devolucoes_orfas").count()
linhas_volume.append(("devolucoes", "todas", n_dev_bronze, n_dev_com_origem, n_dev_orfas, 0))

df_volume = spark.createDataFrame(
    linhas_volume,
    ["fonte", "safra", "linhas_bronze", "linhas_validas", "linhas_rejeitadas_ou_orfas", "duplicatas_removidas"],
)

target_volume = f"{CATALOG}.{SILVER_SCHEMA}.qualidade_volume_processado"
df_volume.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_volume)
df_volume.show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 2. Registros rejeitados por motivo (R12)

# COMMAND ----------

df_rejeicoes_motivo = (
    silver("vendas_rejeitadas")
    .groupBy("safra", "motivo_rejeicao")
    .agg(F.count("*").alias("quantidade"))
    .withColumn("fonte", F.lit("vendas"))
    .select("fonte", "safra", F.col("motivo_rejeicao").alias("motivo"), "quantidade")
)

target_rejeicoes = f"{CATALOG}.{SILVER_SCHEMA}.qualidade_rejeicoes"
df_rejeicoes_motivo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_rejeicoes)
df_rejeicoes_motivo.orderBy("safra", "motivo").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Chaves órfãs por dimensão (integridade referencial)
# MAGIC
# MAGIC Checagem sistemática: toda chave estrangeira do fato de vendas e da carteira histórica
# MAGIC realmente existe na dimensão correspondente? `id_vendedor` nulo é válido (canal
# MAGIC E-commerce, conforme dicionário) e não conta como órfão.

# COMMAND ----------

def contar_orfaos(df_fato, coluna_fk, df_dimensao, coluna_pk, ignorar_nulos=True):
    fk = df_fato.select(F.col(coluna_fk).alias("_fk")).distinct()
    if ignorar_nulos:
        fk = fk.filter(F.col("_fk").isNotNull())
    pk = df_dimensao.select(F.col(coluna_pk).alias("_pk")).distinct()
    orfaos = fk.join(pk, fk._fk == pk._pk, "left_anti")
    return orfaos.count()


df_vendas = silver("vendas")
df_carteira = silver("carteira_historica")

chaves_orfas = [
    ("fVendas", "id_cliente", "dClientes", contar_orfaos(df_vendas, "id_cliente", silver("clientes"), "id_cliente")),
    ("fVendas", "id_produto_chave", "dProdutos", contar_orfaos(df_vendas, "id_produto_chave", silver("produtos"), "id_produto_chave")),
    ("fVendas", "id_vendedor", "dVendedores", contar_orfaos(df_vendas, "id_vendedor", silver("vendedores"), "id_vendedor")),
    ("carteira_historica", "id_cliente", "dClientes", contar_orfaos(df_carteira, "id_cliente", silver("clientes"), "id_cliente")),
    ("carteira_historica", "id_vendedor", "dVendedores", contar_orfaos(df_carteira, "id_vendedor", silver("vendedores"), "id_vendedor")),
]

df_chaves_orfas = spark.createDataFrame(chaves_orfas, ["fato", "coluna_fk", "dimensao_referenciada", "chaves_distintas_orfas"])

target_orfas = f"{CATALOG}.{SILVER_SCHEMA}.qualidade_chaves_orfas"
df_chaves_orfas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_orfas)
df_chaves_orfas.show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 4. Chaves ambíguas (não órfãs — existem, mas apontam para mais de um registro)

# COMMAND ----------

linhas_ambiguas = [
    ("clientes", "cnpj_limpo", silver("clientes").filter(F.col("cnpj_duplicado")).count(),
     "Mesmo CNPJ com mais de um id_cliente (recadastramento pós-migração — ver notebook 1)"),
    ("vendedores", "id_vendedor", silver("vendedores").filter(F.col("id_vendedor_duplicado")).count(),
     "Mesmo id_vendedor atribuído a duas pessoas diferentes (V001 — ver notebook 1)"),
    ("vendas", "id_vendedor", silver("vendas").filter(F.col("id_vendedor_ambiguo")).count(),
     "Vendas que herdam a ambiguidade do id_vendedor duplicado"),
    ("carteira_historica", "id_vendedor", silver("carteira_historica").filter(F.col("id_vendedor_ambiguo")).count(),
     "Vínculos cliente-vendedor que herdam a mesma ambiguidade"),
]

df_ambiguas = spark.createDataFrame(linhas_ambiguas, ["tabela", "coluna", "quantidade", "descricao"])

target_ambiguas = f"{CATALOG}.{SILVER_SCHEMA}.qualidade_chaves_ambiguas"
df_ambiguas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_ambiguas)
df_ambiguas.show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 5. Devoluções sem pedido de origem (R5) — já existe, só referenciamos aqui

# COMMAND ----------

print(f"silver.devolucoes_orfas já contém {silver('devolucoes_orfas').count()} linhas (ver notebook 4 para o detalhe).")
silver("devolucoes_orfas").groupBy("motivo").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# MAGIC %md ## 6. Metadados de atualização e período coberto

# COMMAND ----------

periodo = silver("vendas").agg(
    F.min("data_emissao").alias("data_minima"), F.max("data_emissao").alias("data_maxima")
).collect()[0]

df_metadata = spark.createDataFrame(
    [("vendas", str(periodo["data_minima"]), str(periodo["data_maxima"]))],
    ["fonte", "periodo_inicio", "periodo_fim"],
).withColumn("atualizado_em", F.current_timestamp())

target_metadata = f"{CATALOG}.{SILVER_SCHEMA}.qualidade_metadata_atualizacao"
df_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_metadata)
df_metadata.show(truncate=False)

# COMMAND ----------

# MAGIC %md ## Resumo final — visão consolidada pra Página 3

# COMMAND ----------

print("=== Volume processado por safra ===")
spark.table(f"{CATALOG}.{SILVER_SCHEMA}.qualidade_volume_processado").show(truncate=False)

print("=== Rejeições por motivo (R12) ===")
spark.table(f"{CATALOG}.{SILVER_SCHEMA}.qualidade_rejeicoes").orderBy("safra", "motivo").show(truncate=False)

print("=== Chaves órfãs por dimensão ===")
spark.table(f"{CATALOG}.{SILVER_SCHEMA}.qualidade_chaves_orfas").show(truncate=False)

print("=== Chaves ambíguas ===")
spark.table(f"{CATALOG}.{SILVER_SCHEMA}.qualidade_chaves_ambiguas").show(truncate=False)

print("=== Devoluções órfãs (R5) ===")
print(silver("devolucoes_orfas").count())

print("=== Metadados de atualização ===")
spark.table(f"{CATALOG}.{SILVER_SCHEMA}.qualidade_metadata_atualizacao").show(truncate=False)
