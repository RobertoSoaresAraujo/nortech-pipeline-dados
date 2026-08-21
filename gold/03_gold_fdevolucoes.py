# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fDevolucoes
# MAGIC
# MAGIC Decisão de arquitetura já registrada na conversa: `fDevolucoes` **não** se relaciona
# MAGIC fisicamente com `fVendas` no modelo (cada devolução tem sua própria data — a da devolução,
# MAGIC não a da venda, conforme R5 — e uma relação viva entre os dois fatos confundiria o filtro
# MAGIC de `dCalendario`). Em vez disso, `id_cliente`/`id_produto` são **denormalizados** aqui, uma
# MAGIC única vez, puxando as chaves substitutas já resolvidas em `fVendas` — assim `fDevolucoes`
# MAGIC vira um fato independente, com sua própria relação direta com `dCalendario`, `dClientes` e
# MAGIC `dProdutos`.

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

df_devolucoes = silver("devolucoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Denormalizar cliente/produto a partir de fVendas
# MAGIC
# MAGIC Join seguro por construção: `fVendas` já tem grão único (`safra+id_pedido+item`,
# MAGIC verificado no notebook anterior), então não há risco de fan-out aqui. Devoluções órfãs
# MAGIC (pedido de origem não encontrado) simplesmente não casam no join e caem no sentinela
# MAGIC "Não Identificado" — igual às outras chaves órfãs do modelo.

# COMMAND ----------

fvendas_lookup = gold("fVendas").select(
    F.col("safra").alias("_safra_v"),
    F.col("id_pedido").alias("_id_pedido_v"),
    F.col("item").alias("_item_v"),
    F.col("sk_cliente").alias("_sk_cliente_v"),
    F.col("sk_produto").alias("_sk_produto_v"),
)

df_devolucoes_enriquecido = (
    df_devolucoes
    .join(
        fvendas_lookup,
        (df_devolucoes.safra_origem == F.col("_safra_v"))
        & (df_devolucoes.id_pedido_origem == F.col("_id_pedido_v"))
        & (df_devolucoes.item == F.col("_item_v")),
        "left",
    )
    .withColumn("sk_cliente", F.coalesce(F.col("_sk_cliente_v"), F.lit(-1)))
    .withColumn("sk_produto", F.coalesce(F.col("_sk_produto_v"), F.lit(-1)))
    .drop("_safra_v", "_id_pedido_v", "_item_v", "_sk_cliente_v", "_sk_produto_v")
)

# COMMAND ----------

# MAGIC %md ## Seleção final e gravação

# COMMAND ----------

df_fdevolucoes = df_devolucoes_enriquecido.select(
    "id_devolucao",
    "id_pedido_origem", "item", "safra_origem",
    "data_devolucao",
    "sk_cliente", "sk_produto",
    "quantidade_devolvida",
    "valor_unitario_origem", "desconto_fracao_origem", "moeda_origem", "taxa_cambio_origem",
    "receita_devolucao_bruta", "receita_devolucao_liquida",
    "motivo",
    "pedido_origem_encontrado", "devolucao_de_pedido_cancelado", "quantidade_excede_faturado",
    "status_pedido_origem",
)

save_gold(df_fdevolucoes, "fDevolucoes")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

n_silver = silver("devolucoes").count()
n_gold = df_fdevolucoes.count()
print(f"Reconciliação: silver.devolucoes={n_silver}  gold.fDevolucoes={n_gold}  diferença={n_silver - n_gold}")

print("Checagem de fan-out: id_devolucao não pode se repetir")
duplicadas = df_fdevolucoes.groupBy("id_devolucao").count().filter(F.col("count") > 1)
print(f"  linhas duplicadas encontradas: {duplicadas.count()}  (esperado: 0)")

print("Devoluções órfãs corretamente mapeadas pro sentinela -1:")
df_fdevolucoes.filter(~F.col("pedido_origem_encontrado")).groupBy("sk_cliente", "sk_produto").count().show()

print("Receita de devolução líquida total (deve bater com o valor já validado na Silver):")
df_fdevolucoes.filter(F.col("pedido_origem_encontrado")).agg(
    F.round(F.sum("receita_devolucao_liquida"), 2).alias("total")
).show()

print("Amostra de devoluções COM cliente/produto identificado:")
df_fdevolucoes.filter(F.col("sk_cliente") > 0).select(
    "id_devolucao", "sk_cliente", "sk_produto", "quantidade_devolvida", "receita_devolucao_liquida"
).show(5, truncate=False)
