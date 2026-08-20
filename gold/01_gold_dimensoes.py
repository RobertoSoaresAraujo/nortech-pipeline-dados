# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Dimensões
# MAGIC
# MAGIC Cria as dimensões do star schema com **chave substituta (surrogate key)** — inteiro
# MAGIC sequencial pela chave de negócio, gerado por `row_number()`. Como o pipeline sempre faz
# MAGIC *overwrite* completo (não é upsert incremental), a chave é estável entre execuções: mesma
# MAGIC ordenação da chave de negócio ⇒ mesmo inteiro.
# MAGIC
# MAGIC Cada dimensão ganha um ou mais **membros sentinela** com chave negativa (nunca colide com
# MAGIC a sequência real, que começa em 1) — é assim que as chaves órfãs/ambíguas do `fVendas`
# MAGIC (mapeadas no notebook 6 da Silver) viram "Não Identificado" no modelo, sem perder linha nem
# MAGIC receita no total (conforme pede a seção 5.2 do case).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def silver(table: str):
    return spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}")


def save_gold(df, table: str):
    target = f"{CATALOG}.{GOLD_SCHEMA}.{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"OK  {target:30s} ({df.count()} linhas)")


def com_sentinelas(df_real, sk_col, sentinelas: list[dict]):
    """Adiciona linhas sentinela (chave negativa) antes da real (chave >= 1).
    Cada sentinela é um dict só com os campos que fazem sentido preencher — os demais viram
    NULL. IMPORTANTE: construímos tuplas na ordem exata de df_real.columns, não usamos
    Row(**dict) — esse construtor reordena os campos alfabeticamente por baixo dos panos,
    o que desalinharia silenciosamente os valores com o schema real."""
    colunas = df_real.columns
    linhas = [tuple(sent.get(c) for c in colunas) for sent in sentinelas]
    df_sentinelas = spark.createDataFrame(linhas, schema=df_real.schema)
    return df_sentinelas.unionByName(df_real)

# COMMAND ----------

# MAGIC %md ## dCalendario — praticamente um passthrough da Silver

# COMMAND ----------

df_dcalendario = silver("dcalendario")
save_gold(df_dcalendario, "dCalendario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dClientes
# MAGIC
# MAGIC Além da chave substituta, resolve a hierarquia matriz/filial **achatada** (decisão
# MAGIC registrada na conversa de arquitetura: 2 níveis não justificam parent-child em DAX).

# COMMAND ----------

df_clientes_base = silver("clientes")

df_matriz_lookup = df_clientes_base.select(
    F.col("id_cliente").alias("_id_matriz"), F.col("razao_social").alias("_nome_matriz")
)

df_clientes_enriquecido = (
    df_clientes_base
    .withColumn("id_grupo_economico", F.coalesce(F.col("id_matriz"), F.col("id_cliente")))
    .join(df_matriz_lookup, F.col("id_grupo_economico") == F.col("_id_matriz"), "left")
    .withColumn("nome_grupo_economico", F.coalesce(F.col("_nome_matriz"), F.col("razao_social")))
    .withColumn("eh_matriz", F.col("id_matriz").isNull())
    .drop("_id_matriz", "_nome_matriz")
)

w_cliente = Window.orderBy("id_cliente")
df_clientes_com_sk = df_clientes_enriquecido.withColumn("sk_cliente", F.row_number().over(w_cliente))

df_dclientes = com_sentinelas(
    df_clientes_com_sk,
    "sk_cliente",
    [{"sk_cliente": -1, "id_cliente": "N/A", "razao_social": "Não Identificado",
      "segmento": "Não Identificado", "regiao": "Não Identificado", "situacao": "Não Identificado"}],
)

save_gold(df_dclientes, "dClientes")

# COMMAND ----------

# MAGIC %md ## dProdutos

# COMMAND ----------

w_produto = Window.orderBy("id_produto_chave")
df_produtos_com_sk = silver("produtos").withColumn("sk_produto", F.row_number().over(w_produto))

df_dprodutos = com_sentinelas(
    df_produtos_com_sk,
    "sk_produto",
    [{"sk_produto": -1, "id_produto": "N/A", "id_produto_chave": "N/A",
      "descricao": "Não Identificado", "categoria": "Não Identificado",
      "subcategoria": "Não Identificado", "linha": "Não Identificado"}],
)

save_gold(df_dprodutos, "dProdutos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dVendedores
# MAGIC
# MAGIC Três sentinelas, porque são três situações semanticamente diferentes:
# MAGIC - **0** = sem vendedor associado (canal E-commerce — situação válida, não é erro)
# MAGIC - **-1** = vendedor não identificado (chave órfã de verdade — não ocorreu nos dados, mas
# MAGIC   o modelo precisa saber lidar com isso se aparecer)
# MAGIC - **-2** = vendedor ambíguo (o caso do `V001` duplicado)

# COMMAND ----------

w_vendedor = Window.orderBy("id_vendedor")
df_vendedores_com_sk = silver("vendedores").withColumn("sk_vendedor", F.row_number().over(w_vendedor))

df_dvendedores = com_sentinelas(
    df_vendedores_com_sk,
    "sk_vendedor",
    [
        {"sk_vendedor": 0, "id_vendedor": "N/A", "nome": "Sem vendedor associado (E-commerce)", "cargo": "N/A"},
        {"sk_vendedor": -1, "id_vendedor": "DESCONHECIDO", "nome": "Vendedor não identificado", "cargo": "N/A"},
        {"sk_vendedor": -2, "id_vendedor": "V001_AMBIGUO", "nome": "Vendedor ambíguo (código duplicado)", "cargo": "N/A"},
    ],
)

save_gold(df_dvendedores, "dVendedores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dRegiaoSegmento
# MAGIC
# MAGIC Dimensão pequena e independente (5 regiões × 4 segmentos) que serve o `fMetas` sem
# MAGIC precisar tocar em `dClientes` — é o que permite comparar Meta × Realizado sem relação
# MAGIC direta entre os dois fatos (cada um se conecta a essa dimensão, ou a `dClientes`, de forma
# MAGIC independente).

# COMMAND ----------

df_regseg_clientes = silver("clientes").select("regiao", "segmento")
df_regseg_metas = silver("metas_anuais").select("regiao", "segmento")

w_regseg = Window.orderBy("regiao", "segmento")
df_dregiaosegmento = (
    df_regseg_clientes.unionByName(df_regseg_metas)
    .distinct()
    .withColumn("sk_regiao_segmento", F.row_number().over(w_regseg))
)

save_gold(df_dregiaosegmento, "dRegiaoSegmento")

# COMMAND ----------

# MAGIC %md ## dSegurancaAcessos — passthrough, usado só pela RLS (não se relaciona com os fatos)

# COMMAND ----------

df_dseguranca = silver("seguranca_acessos")
save_gold(df_dseguranca, "dSegurancaAcessos")

# COMMAND ----------

# MAGIC %md ## Validação

# COMMAND ----------

for t in ["dCalendario", "dClientes", "dProdutos", "dVendedores", "dRegiaoSegmento", "dSegurancaAcessos"]:
    n = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{t}").count()
    print(f"  gold.{t:20s} {n:>6} linhas")

print()
print("dClientes — sentinela presente:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dClientes").filter(F.col("sk_cliente") < 0).show(truncate=False)

print("dVendedores — os 3 sentinelas presentes:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dVendedores").filter(F.col("sk_vendedor") <= 0).select(
    "sk_vendedor", "id_vendedor", "nome"
).show(truncate=False)

print("dRegiaoSegmento — esperado 20 combinações:")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dRegiaoSegmento").orderBy("regiao", "segmento").show(20, truncate=False)

print("dClientes — amostra do grupo econômico (matriz/filial achatado):")
spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dClientes").filter(F.col("id_matriz").isNotNull()).select(
    "id_cliente", "razao_social", "id_matriz", "id_grupo_economico", "nome_grupo_economico"
).show(10, truncate=False)
