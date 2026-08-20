# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — fVendas
# MAGIC
# MAGIC Constrói o fato de vendas pronto para o Power BI: troca toda chave de negócio por chave
# MAGIC substituta, resolve o **vendedor responsável** via `carteira_historica` (as-of join por
# MAGIC cliente + data de emissão — decisão de arquitetura: resolvida no ETL, não em DAX) e
# MAGIC redireciona toda chave órfã/ambígua para os sentinelas criados no notebook 1.
# MAGIC
# MAGIC **Cuidado central deste notebook**: `dVendedores` tem duas linhas reais para o código
# MAGIC `V001` (ambíguo). Um join direto por `id_vendedor` nesse código causaria fan-out (a venda
# MAGIC apareceria duplicada, uma vez pra cada pessoa). Por isso, toda resolução de vendedor separa
# MAGIC primeiro os casos nulos/ambíguos (resolvidos para sentinela sem nenhum join) dos casos
# MAGIC normais (só esses passam pelo join com `dVendedores`).

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
from pyspark.sql.window import Window

df_vendas = silver("vendas")
df_carteira = silver("carteira_historica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 1 — vendedor via carteira_historica (as-of join por cliente + data)
# MAGIC
# MAGIC Mesma técnica do câmbio na Silver: junta pelo cliente e pega a vigência que cobre a data
# MAGIC de emissão, escolhendo a mais recente em caso de sobreposição (defensivo — não deveria
# MAGIC ocorrer, mas não custa proteger).

# COMMAND ----------

v_base = df_vendas.select(
    "safra", "id_pedido", "item",
    F.col("id_cliente").alias("_v_id_cliente"),
    F.col("data_emissao").alias("_v_data_emissao"),
)
c_base = df_carteira.select(
    F.col("id_cliente").alias("_c_id_cliente"),
    F.col("id_vendedor").alias("_c_id_vendedor"),
    "vigencia_inicio", "vigencia_fim", "vigencia_aberta",
    F.col("id_vendedor_ambiguo").alias("_c_id_vendedor_ambiguo"),
)

candidatos = v_base.join(
    c_base,
    (v_base._v_id_cliente == c_base._c_id_cliente)
    & (c_base.vigencia_inicio <= v_base._v_data_emissao)
    & ((c_base.vigencia_fim >= v_base._v_data_emissao) | c_base.vigencia_aberta),
    "left",
)

w_vig = Window.partitionBy("safra", "id_pedido", "item").orderBy(F.col("vigencia_inicio").desc())
carteira_resolvida = (
    candidatos
    .withColumn("_rn", F.row_number().over(w_vig))
    .filter(F.col("_rn") == 1)
    .select(
        "safra", "id_pedido", "item",
        F.col("_c_id_vendedor").alias("vendedor_via_carteira"),
        F.col("_c_id_vendedor_ambiguo").alias("vendedor_via_carteira_ambiguo"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 2 — combinar com o vendedor gravado no pedido original
# MAGIC
# MAGIC Regra de negócio (dicionário de `carteira_historica.csv`): a venda deve ser atribuída ao
# MAGIC executivo **vigente na data de emissão**, que não é necessariamente o código gravado no
# MAGIC pedido. Prioridade: carteira > pedido original > sem vendedor. Quando não há registro de
# MAGIC carteira pra aquele cliente/data, caímos pro vendedor gravado no pedido como alternativa
# MAGIC razoável — melhor que perder a atribuição por completo.

# COMMAND ----------

df_vendas_enriquecido = (
    df_vendas
    .join(carteira_resolvida, ["safra", "id_pedido", "item"], "left")
    .withColumn(
        "id_vendedor_atribuicao",
        F.coalesce(F.col("vendedor_via_carteira"), F.col("id_vendedor")),
    )
    .withColumn(
        "vendedor_ambiguo_atribuicao",
        F.when(F.col("vendedor_via_carteira").isNotNull(), F.col("vendedor_via_carteira_ambiguo"))
        .otherwise(F.col("id_vendedor_ambiguo")),
    )
    .withColumn(
        "vendedor_fonte",
        F.when(F.col("vendedor_via_carteira").isNotNull(), F.lit("carteira_historica"))
        .when(F.col("id_vendedor").isNotNull(), F.lit("pedido_original_sem_carteira"))
        .otherwise(F.lit("sem_vendedor")),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passo 3 — resolução segura de chave substituta de vendedor (sem fan-out)
# MAGIC
# MAGIC Nulos e ambíguos são resolvidos para sentinela **sem tocar em `dVendedores`**. Só os casos
# MAGIC normais (garantidamente 1 correspondência) passam pelo join.

# COMMAND ----------

dvendedores_lookup = gold("dVendedores").select(
    F.col("id_vendedor").alias("_id_vendedor_lookup"), F.col("sk_vendedor").alias("_sk_vendedor_lookup")
).filter(F.col("sk_vendedor") > 0)  # só vendedores reais — nunca os sentinelas


def resolver_sk_vendedor(df, col_id, col_ambiguo, sk_col_name):
    com_sentinela_provisoria = df.withColumn(
        sk_col_name,
        F.when(F.col(col_id).isNull(), F.lit(0))       # sem vendedor
        .when(F.col(col_ambiguo), F.lit(-2))            # ambíguo — NUNCA passa pelo join abaixo
        .otherwise(F.lit(None).cast("long")),
    )

    ja_resolvido = com_sentinela_provisoria.filter(F.col(sk_col_name).isNotNull())
    precisa_resolver = com_sentinela_provisoria.filter(F.col(sk_col_name).isNull())

    resolvido = (
        precisa_resolver
        .join(dvendedores_lookup, F.col(col_id) == F.col("_id_vendedor_lookup"), "left")
        .withColumn(sk_col_name, F.coalesce(F.col("_sk_vendedor_lookup"), F.lit(-1)))  # -1 = defensivo, não deveria ocorrer
        .drop("_id_vendedor_lookup", "_sk_vendedor_lookup")
    )
    return ja_resolvido.unionByName(resolvido)


df_vendas_enriquecido = resolver_sk_vendedor(
    df_vendas_enriquecido, "id_vendedor", "id_vendedor_ambiguo", "sk_vendedor_pedido"
)
df_vendas_enriquecido = resolver_sk_vendedor(
    df_vendas_enriquecido, "id_vendedor_atribuicao", "vendedor_ambiguo_atribuicao", "sk_vendedor_responsavel"
)

# COMMAND ----------

# MAGIC %md ## Passo 4 — chaves substitutas de cliente e produto (join simples — sem risco de fan-out)

# COMMAND ----------

dclientes_lookup = gold("dClientes").select(
    F.col("id_cliente").alias("_id_cliente_lookup"), F.col("sk_cliente").alias("_sk_cliente_lookup")
).filter(F.col("sk_cliente") > 0)

dprodutos_lookup = gold("dProdutos").select(
    F.col("id_produto_chave").alias("_id_produto_lookup"), F.col("sk_produto").alias("_sk_produto_lookup")
).filter(F.col("sk_produto") > 0)

df_vendas_enriquecido = (
    df_vendas_enriquecido
    .join(dclientes_lookup, F.col("id_cliente") == F.col("_id_cliente_lookup"), "left")
    .withColumn("sk_cliente", F.coalesce(F.col("_sk_cliente_lookup"), F.lit(-1)))
    .drop("_id_cliente_lookup", "_sk_cliente_lookup")
    .join(dprodutos_lookup, F.col("id_produto_chave") == F.col("_id_produto_lookup"), "left")
    .withColumn("sk_produto", F.coalesce(F.col("_sk_produto_lookup"), F.lit(-1)))
    .drop("_id_produto_lookup", "_sk_produto_lookup")
)

# COMMAND ----------

# MAGIC %md ## Seleção final e gravação

# COMMAND ----------

df_fvendas = df_vendas_enriquecido.select(
    "safra", "id_pedido", "item",
    "data_emissao",
    "sk_cliente", "sk_produto", "sk_vendedor_pedido", "sk_vendedor_responsavel", "vendedor_fonte",
    "id_cliente", "id_produto", "id_produto_chave", "id_vendedor", "id_vendedor_atribuicao",
    "canal", "moeda", "id_filial",
    "quantidade_absoluta", "valor_unitario", "desconto_fracao", "taxa_cambio_aplicada",
    "receita_bruta", "receita_liquida",
    "status_pedido", "frete_rateado",
)

save_gold(df_fvendas, "fVendas")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

n_silver = silver("vendas").count()
n_gold = df_fvendas.count()
print(f"Reconciliação de linhas: silver.vendas={n_silver}  gold.fVendas={n_gold}  diferença={n_silver - n_gold}")

print("Distribuição de sk_cliente/sk_produto/sk_vendedor_pedido/sk_vendedor_responsavel sentinela (<=0):")
df_fvendas.select(
    F.sum((F.col("sk_cliente") < 0).cast("int")).alias("clientes_nao_identificados"),
    F.sum((F.col("sk_produto") < 0).cast("int")).alias("produtos_nao_identificados"),
    F.sum((F.col("sk_vendedor_pedido") == 0).cast("int")).alias("pedido_sem_vendedor"),
    F.sum((F.col("sk_vendedor_pedido") == -2).cast("int")).alias("pedido_vendedor_ambiguo"),
    F.sum((F.col("sk_vendedor_responsavel") == 0).cast("int")).alias("responsavel_sem_vendedor"),
    F.sum((F.col("sk_vendedor_responsavel") == -2).cast("int")).alias("responsavel_ambiguo"),
).show(truncate=False)

print("Distribuição de vendedor_fonte:")
df_fvendas.groupBy("vendedor_fonte").count().show(truncate=False)

print("Receita líquida total (deve bater com o total já validado na Silver):")
df_fvendas.agg(F.round(F.sum("receita_liquida"), 2).alias("total")).show()

print("Checagem de fan-out: nenhuma combinação (safra,id_pedido,item) pode aparecer mais de uma vez")
duplicadas = df_fvendas.groupBy("safra", "id_pedido", "item").count().filter(F.col("count") > 1)
print(f"  linhas duplicadas encontradas: {duplicadas.count()}  (esperado: 0)")
