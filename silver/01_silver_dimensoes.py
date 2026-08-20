# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Dimensões simples
# MAGIC
# MAGIC Padroniza `clientes`, `produtos`, `vendedores`, `carteira_historica`, `seguranca_acessos`,
# MAGIC `feriados` e `cambio_usd`. Aqui entram tipos corretos, texto padronizado e datas convertidas.
# MAGIC
# MAGIC Os valores reais de cada campo (categorias, regiões, formatos de data) foram inspecionados
# MAGIC nos arquivos originais antes de escrever as regras abaixo — não são regras "no chute".
# MAGIC
# MAGIC **Convenção de qualidade usada em todo o notebook:** quando um valor não bate com nenhuma
# MAGIC regra de normalização conhecida, ele vira `NULL` na coluna tratada (nunca é inventado) e a
# MAGIC linha inteira continua na tabela — a consolidação desses casos numa quarentena central
# MAGIC acontece no notebook 6, quando o quadro completo (vendas + devoluções + dimensões) estiver
# MAGIC fechado.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DoubleType
from pyspark.sql.window import Window
import re
import unicodedata
from datetime import date, timedelta


def bronze(table: str):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table}")


def save_silver(df, table: str):
    target = f"{CATALOG}.{SILVER_SCHEMA}.{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    print(f"OK  {target:35s} ({df.count()} linhas)")

# COMMAND ----------

# MAGIC %md ## Funções de normalização reutilizáveis

# COMMAND ----------

# ---- Texto: remove acento/pontuação/espaço duplicado para comparar com o dicionário de mapeamento ----

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _normalize_key(v) -> str:
    if v is None:
        return ""
    v = _strip_accents(str(v)).strip().lower()
    v = re.sub(r"[.\-]", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v


# Variantes encontradas de fato em clientes.csv, mapeadas para os 4 valores canônicos do dicionário
SEGMENTO_MAP = {
    "atacado": "Atacado", "atac": "Atacado",
    "governo": "Governo", "gov": "Governo", "poder publico": "Governo",
    "industria": "Industria", "ind": "Industria",
    "varejo": "Varejo",
}

# Variantes encontradas de fato em clientes.csv (incluindo os typos "nord este" e "sudest")
REGIAO_MAP = {
    "sul": "Sul",
    "norte": "Norte",
    "nordeste": "Nordeste", "ne": "Nordeste", "nord este": "Nordeste",
    "sudeste": "Sudeste", "se": "Sudeste", "sudest": "Sudeste",
    "centro oeste": "Centro-Oeste", "co": "Centro-Oeste",
}

SITUACAO_MAP = {
    "a": "Ativo", "ativo": "Ativo", "inativo": "Inativo",
}

# uf é um campo controlado e confiável (conforme dicionário de dados) — usado como fallback
# para inferir a região quando ela vem vazia na origem (34 linhas de clientes.csv, confirmado
# nos dados brutos: não é problema de grafia, o campo está mesmo em branco).
UF_TO_REGIAO = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}


@F.udf(returnType=StringType())
def regiao_from_uf(uf):
    if uf is None:
        return None
    return UF_TO_REGIAO.get(str(uf).strip().upper())


# Padroniza capitalização de razão social (ex: "ECLIPSE GROUP LTDA" / "eclipse group ltda"
# -> "Eclipse Group Ltda"), corrigindo siglas jurídicas que não seguem Title Case comum.
RAZAO_SOCIAL_ACRONYM_FIX = {"Eireli": "EIRELI", "Me": "ME", "Epp": "EPP"}


@F.udf(returnType=StringType())
def standardize_razao_social(v):
    if v is None:
        return None
    v = re.sub(r"\s+", " ", str(v).strip())
    palavras = []
    for w in v.split(" "):
        base = w.title()
        palavras.append(RAZAO_SOCIAL_ACRONYM_FIX.get(base, base))
    return " ".join(palavras)


@F.udf(returnType=StringType())
def normalize_segmento(v):
    return SEGMENTO_MAP.get(_normalize_key(v))


@F.udf(returnType=StringType())
def normalize_regiao(v):
    return REGIAO_MAP.get(_normalize_key(v))


@F.udf(returnType=StringType())
def normalize_situacao(v):
    return SITUACAO_MAP.get(_normalize_key(v))


# ---- Datas em formatos mistos: dd/mm/aaaa, aaaa-mm-dd, dd-mm-aaaa, serial do Excel, dd/mmm/aa ----

MESES_PT = {"jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
            "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12}


@F.udf(returnType=DateType())
def parse_flexible_date(value):
    if value is None:
        return None
    v = str(value).strip()
    if v == "" or v.lower() == "nan":
        return None

    # Serial do Excel (dias desde 1899-12-30) — aparece quando a célula não foi formatada como data
    if re.fullmatch(r"\d{4,6}", v):
        try:
            return date(1899, 12, 30) + timedelta(days=int(v))
        except Exception:
            return None

    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", v)  # dd/mm/aaaa
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", v)  # aaaa-mm-dd
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", v)  # dd-mm-aaaa
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None

    m = re.fullmatch(r"(\d{1,2})/([a-zA-Z]{3})/(\d{2})", v)  # dd/mmm/aa (ex: 07/jan/20)
    if m:
        d, mes_abbr, yy = m.groups()
        mo = MESES_PT.get(mes_abbr.lower())
        if mo:
            y = 2000 + int(yy)  # dados do case são todos do século 21
            try:
                return date(y, mo, int(d))
            except ValueError:
                return None

    return None  # formato não reconhecido -> NULL, rastreado depois na quarentena


# ---- Números com vírgula decimal (locale pt-BR) ----

@F.udf(returnType=DoubleType())
def parse_ptbr_decimal(value):
    if value is None:
        return None
    v = str(value).strip()
    if v == "" or v.lower() == "nan":
        return None
    try:
        return float(v.replace(",", "."))
    except ValueError:
        return None

# COMMAND ----------

# MAGIC %md ## feriados

# COMMAND ----------

df_feriados = (
    bronze("feriados")
    .select(
        F.to_date("data", "yyyy-MM-dd").alias("data"),
        F.trim("descricao").alias("descricao"),
        F.trim("abrangencia").alias("abrangencia"),
    )
)
save_silver(df_feriados, "feriados")

# COMMAND ----------

# MAGIC %md ## cambio_usd

# COMMAND ----------

df_cambio = (
    bronze("cambio_usd")
    .select(
        parse_flexible_date("data_cotacao").alias("data_cotacao"),
        F.trim("moeda").alias("moeda"),
        parse_ptbr_decimal("taxa_ptax_venda").alias("taxa_ptax_venda"),
    )
)
save_silver(df_cambio, "cambio_usd")

# COMMAND ----------

# MAGIC %md ## produtos

# COMMAND ----------

df_produtos = (
    bronze("produtos")
    .select(
        F.trim("id_produto").alias("id_produto"),
        F.upper(F.trim("id_produto")).alias("id_produto_chave"),  # chave robusta p/ join com vendas_2026 (perde caixa)
        F.trim("descricao").alias("descricao"),
        F.trim("categoria").alias("categoria"),
        F.trim("subcategoria").alias("subcategoria"),
        F.trim("linha").alias("linha"),
        F.trim("unidade").alias("unidade"),
        parse_ptbr_decimal("custo_padrao").alias("custo_padrao"),
        parse_ptbr_decimal("preco_lista").alias("preco_lista"),
        (F.trim("flag_descontinuado") == "1").alias("descontinuado"),
    )
)
save_silver(df_produtos, "produtos")

# COMMAND ----------

# MAGIC %md ## vendedores

# COMMAND ----------

df_vendedores = (
    bronze("vendedores")
    .select(
        F.trim("id_vendedor").alias("id_vendedor"),
        F.trim("nome").alias("nome"),
        F.lower(F.trim("email_corporativo")).alias("email_corporativo"),  # chave de join com seguranca_acessos
        F.trim("cargo").alias("cargo"),
        F.trim("regiao").alias("regiao"),
        F.trim("id_gestor").alias("id_gestor"),
        parse_flexible_date("data_admissao").alias("data_admissao"),
        parse_flexible_date("data_desligamento").alias("data_desligamento"),
    )
    .withColumn("vendedor_ativo", F.col("data_desligamento").isNull())
)
save_silver(df_vendedores, "vendedores")

# COMMAND ----------

# MAGIC %md ## carteira_historica

# COMMAND ----------

df_carteira = (
    bronze("carteira_historica")
    .select(
        F.trim("id_cliente").alias("id_cliente"),
        F.trim("id_vendedor").alias("id_vendedor"),
        F.to_date("vigencia_inicio", "yyyy-MM-dd").alias("vigencia_inicio"),
        F.to_date("vigencia_fim", "yyyy-MM-dd").alias("vigencia_fim"),
    )
    .withColumn("vigencia_aberta", F.col("vigencia_fim") == F.lit("9999-12-31"))
)
save_silver(df_carteira, "carteira_historica")

# COMMAND ----------

# MAGIC %md ## seguranca_acessos

# COMMAND ----------

df_seguranca = (
    bronze("seguranca_acessos")
    .select(
        F.lower(F.trim("email")).alias("email"),
        F.trim("nome").alias("nome"),
        F.trim("perfil").alias("perfil"),
        F.trim("regiao_permitida").alias("regiao_permitida"),
        F.trim("segmento_permitido").alias("segmento_permitido"),
        (F.trim("ativo") == "SIM").alias("ativo"),
    )
)
save_silver(df_seguranca, "seguranca_acessos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## clientes
# MAGIC
# MAGIC A dimensão mais suja do case. Decisões tomadas aqui:
# MAGIC - **CNPJ**: mantido o original (`cnpj`) e criada `cnpj_limpo` (só dígitos) + `cnpj_valido`
# MAGIC   (14 dígitos). Não usamos CNPJ como chave — `id_cliente` continua sendo a chave do cadastro.
# MAGIC - **Recadastramento**: o dicionário avisa que o mesmo CNPJ pode ter mais de um `id_cliente`
# MAGIC   (migração de sistema). Não fundimos os cadastros aqui — isso mudaria granularidade e
# MAGIC   esconderia o problema. Em vez disso, sinalizamos com `cnpj_duplicado` para a decisão de
# MAGIC   negócio (fundir ou não) ficar visível e documentada, não resolvida silenciosamente.
# MAGIC - **segmento / regiao**: mapeados para os valores canônicos via `SEGMENTO_MAP` / `REGIAO_MAP`.

# COMMAND ----------

df_clientes_raw = bronze("clientes")

w_cnpj = Window.partitionBy("cnpj_limpo")

df_clientes = (
    df_clientes_raw
    .select(
        F.trim("id_cliente").alias("id_cliente"),
        F.trim("razao_social").alias("razao_social_original"),
        standardize_razao_social("razao_social").alias("razao_social"),
        F.trim("cnpj").alias("cnpj"),
        F.regexp_replace(F.trim("cnpj"), r"[^0-9]", "").alias("cnpj_limpo"),
        F.col("segmento").alias("segmento_original"),
        normalize_segmento("segmento").alias("segmento"),
        F.col("regiao").alias("regiao_original"),
        normalize_regiao("regiao").alias("regiao_mapeada"),
        F.upper(F.trim("uf")).alias("uf"),
        F.trim("cidade").alias("cidade"),
        F.col("data_cadastro").alias("data_cadastro_original"),
        parse_flexible_date("data_cadastro").alias("data_cadastro"),
        F.nullif(F.trim("id_matriz"), F.lit("")).alias("id_matriz"),
        F.col("situacao").alias("situacao_original"),
        normalize_situacao("situacao").alias("situacao"),
    )
    .withColumn("regiao_inferida_por_uf", F.col("regiao_mapeada").isNull() & F.col("uf").isNotNull())
    .withColumn("regiao", F.coalesce(F.col("regiao_mapeada"), regiao_from_uf(F.col("uf"))))
    .drop("regiao_mapeada")
    .withColumn("cnpj_valido", F.length("cnpj_limpo") == 14)
    .withColumn("cnpj_duplicado", F.count("id_cliente").over(w_cnpj) > 1)
)

save_silver(df_clientes, "clientes")

# COMMAND ----------

# MAGIC %md ## Validação — cobertura das regras de mapeamento

# COMMAND ----------

print("Valores de segmento sem mapeamento (deveria ser 0 linhas):")
df_clientes.filter(F.col("segmento").isNull()).select("id_cliente", "segmento_original").show(truncate=False)

print("Clientes com região inferida a partir da UF (fallback aplicado):")
print(df_clientes.filter(F.col("regiao_inferida_por_uf")).count())

print("Valores de regiao ainda sem mapeamento após o fallback por UF (deveria ser 0 linhas):")
df_clientes.filter(F.col("regiao").isNull()).select("id_cliente", "regiao_original", "uf").show(truncate=False)

print("Valores de situacao sem mapeamento (deveria ser 0 linhas):")
df_clientes.filter(F.col("situacao").isNull()).select("id_cliente", "situacao_original").show(truncate=False)

print("Datas de cadastro não reconhecidas (deveria ser 0 linhas):")
df_clientes.filter(F.col("data_cadastro").isNull()).select("id_cliente", "data_cadastro_original").show(truncate=False)

print("CNPJs duplicados (mesmo CNPJ, mais de um id_cliente):")
df_clientes.filter(F.col("cnpj_duplicado")).select("id_cliente", "cnpj_limpo", "razao_social", "razao_social_original").orderBy("cnpj_limpo").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md ## Resumo final

# COMMAND ----------

for t in ["feriados", "cambio_usd", "produtos", "vendedores", "carteira_historica", "seguranca_acessos", "clientes"]:
    n = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{t}").count()
    print(f"  silver.{t:22s} {n:>6} linhas")
