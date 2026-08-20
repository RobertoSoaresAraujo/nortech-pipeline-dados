# Bronze — Ingestão

## Pré-requisitos

1. Catálogo e schema `bronze` criados no Unity Catalog.
2. Volume `raw_files` criado dentro do schema `bronze`.
3. Os 12 arquivos originais enviados para esse Volume, sem edição:
   `cambio_usd.csv`, `carteira_historica.csv`, `clientes.csv`, `devolucoes.csv`,
   `feriados.csv`, `metas_comerciais.xlsx`, `produtos.csv`, `seguranca_acessos.csv`,
   `vendas_2024.csv`, `vendas_2025.csv`, `vendas_2026.csv`, `vendedores.csv`.

## Como rodar

1. Abra `01_bronze_ingestion.py` no Databricks (via Repos, já sincronizado com o GitHub).
2. Ajuste os widgets no topo do notebook se o nome do seu catálogo ou o caminho do Volume
   forem diferentes de `workspace` / `/Volumes/workspace/bronze/raw_files`.
3. Rode todas as células (`Run all`).
4. A última célula lista as tabelas criadas e a contagem de linhas de cada uma — confira se
   bate com a tabela "Linhas" do `00_CASE.md` (ex: `vendas_2024` = 32.829 linhas de dado,
   já que a primeira linha do CSV é o cabeçalho).

## O que este notebook faz (e o que não faz)

Faz: lê cada arquivo com o encoding/delimitador corretos (segundo o dicionário de dados),
grava como tabela Delta gerenciada, adiciona `_source_file` e `_ingested_at`.

Não faz: nenhuma limpeza, conversão de tipo, padronização de texto, tratamento de data,
resolução de moeda ou aplicação de regra de negócio. Tudo isso é responsabilidade da
camada Silver — é lá que as regras R1–R12 do `00_CASE.md` entram em ação.

## Decisões registradas

- **`vendas_2024.csv` lido como ISO-8859-1**: é o encoding real do ERP legado, segundo o
  dicionário. Ler como UTF-8 corromperia acentos silenciosamente.
- **BOM removido apenas do nome da coluna, não do conteúdo**: o BOM de `vendas_2026.csv`
  aparece colado ao primeiro cabeçalho (`id_pedido`); os dados em si não são afetados.
- **Linha de totalização das metas mantida no Bronze**: será tratada (isolada ou descartada,
  com justificativa) na Silver — no Bronze, por princípio, nada é removido.
- **Aba `Premissas` virou tabela própria**: mesmo sendo só texto, contém as regras de negócio
  que a Diretoria usa (conversão cambial, ano fiscal) — vale a pena estar consultável em SQL.
- **Nomes de coluna das abas de metas foram sanitizados** (ex: `Meta Ano` → `Meta_Ano`): o Delta
  não aceita espaço em nome de coluna. Só o nome muda — o valor da célula continua intocado.
