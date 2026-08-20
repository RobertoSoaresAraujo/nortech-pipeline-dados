# Silver — Dimensões simples

## Pré-requisito

A Bronze precisa já ter rodado com sucesso (notebook `bronze/01_bronze_ingestion.py`).

## Como rodar

1. Coloque `01_silver_dimensoes.py` dentro da pasta `silver/` do repositório (mesmo fluxo de
   sempre: sobe no GitHub, dá `Pull` no Databricks Repos).
2. Abra o notebook, confirme o widget `catalog` e clique em `Run all`.
3. A última célula mostra a contagem de linhas de cada tabela Silver criada.
4. **Preste atenção nas células de validação, logo antes do resumo final** — elas mostram
   linhas cujo valor não bateve com nenhuma regra de mapeamento conhecida (segmento, região,
   situação ou data de cadastro não reconhecidos). O esperado é aparecer **0 linhas** em cada
   uma. Se aparecer alguma, é sinal de uma variante nova que não foi vista antes — me avisa
   com o print, que eu ajusto o mapeamento.

## Decisões registradas

- **Valores não mapeados viram `NULL`, nunca um valor inventado.** A linha continua na tabela;
  a decisão de descartar ou não fica para a etapa de quarentena (notebook 6), quando o quadro
  completo (vendas + devoluções) estiver fechado.
- **`clientes.segmento` e `clientes.regiao`** tinham 15+ variantes de grafia reais no arquivo
  (`ATACADO`, `Atac.`, `atacado`, `Poder Publico`, `Nord este` com espaço, `CO`, etc.) — foram
  mapeadas para os 4 segmentos e 5 regiões canônicos do dicionário de dados via um dicionário
  de normalização explícito no código (fácil de auditar e estender).
- **`clientes.data_cadastro`** tinha 5 formatos misturados no mesmo arquivo, incluindo datas
  seriais do Excel (ex: `44648`) e abreviação de mês em português (ex: `07/jan/20`). Uma função
  única (`parse_flexible_date`) tenta cada formato em sequência.
- **CNPJ duplicado não foi resolvido, só sinalizado** (`cnpj_duplicado = true`). O dicionário
  avisa que o mesmo CNPJ pode ter mais de um cadastro por causa de migração de sistema — fundir
  os cadastros muda a granularidade da dimensão e essa é uma decisão de modelagem que precisa
  ser explícita (e defendida na entrevista), não escondida dentro do ETL.
- **`id_produto_chave` (upper+trim) criado à parte do `id_produto` original**: o dicionário
  avisa que `vendas_2026.csv` não preserva a caixa do código do produto — essa coluna existe
  para servir de chave de join robusta na hora de montar o fato de vendas.
- **`vendedores.email_corporativo` e `seguranca_acessos.email` normalizados para minúsculo**:
  são a chave de ligação entre as duas tabelas (RLS), e e-mail não deveria ser case-sensitive
  para esse fim.
- **`clientes.regiao` vazia foi inferida a partir da `uf`** em 34 linhas onde o campo veio
  realmente em branco na origem (não era problema de grafia — confirmado nos dados brutos).
  Como `uf` é descrito no dicionário como campo controlado e confiável, usamos a classificação
  oficial de estados por região (IBGE) como fallback, em vez de deixar a região nula ou de
  descartar essas linhas. A coluna `regiao_inferida_por_uf` sinaliza exatamente quais linhas
  passaram por essa inferência, para não esconder a decisão.
- **`clientes` guarda o valor original ao lado do valor tratado** (`segmento_original`,
  `regiao_original`, `situacao_original`, `data_cadastro_original`): permite auditar qualquer
  linha sem precisar voltar na Bronze, e é o que alimenta as células de validação do notebook.
