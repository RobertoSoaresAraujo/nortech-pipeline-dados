# Silver — Fato de devoluções

## Pré-requisito

`silver/03_silver_vendas.py` já rodado (o join de valorização depende de `silver.vendas`).

## Como rodar

Mesmo fluxo: coloque `04_silver_devolucoes.py` na pasta `silver/`, suba no GitHub, `Pull` no
Databricks (conferindo o widget `catalog`), `Run all`.

## Decisões registradas

- **Devolução não tem preço próprio — é valorizada pelo item de origem.** O join usa
  `id_pedido_origem + item` contra `silver.vendas`. Confirmamos nos dados brutos que essa chave
  não colide entre safras diferentes, então o join não corre risco de duplicar linha.
- **Devoluções órfãs (pedido de origem não encontrado) não são descartadas.** Ficam na tabela
  principal `silver.devolucoes` com `pedido_origem_encontrado = false` e os campos de valorização
  em `NULL` (não dá pra valorizar sem saber o preço do item original) — e também numa tabela
  dedicada `silver.devolucoes_orfas`, porque o case pede explicitamente esse dado na Página 3 do
  dashboard ("devoluções sem pedido de origem"). 120 das 2.296 devoluções são órfãs, todas
  referenciando pedidos com prefixo `PV2023...` — de fato anteriores ao período coberto pela
  base de vendas (2024 em diante), como o dicionário já avisava.
- **`receita_devolucao_liquida` fica pronta para ser subtraída na Gold pela `data_devolucao`**,
  não pela data da venda original — é isso que a R5 pede. A tabela de devoluções não altera
  `silver.vendas`; a composição (venda − devolução, cada uma na sua própria data) acontece na
  camada Gold, na hora de montar a série temporal de receita.
- **`quantidade_excede_faturado`** sinaliza devoluções cuja quantidade devolvida é maior que a
  quantidade faturada no item de origem — o dicionário afirma que isso não deveria acontecer
  (`quantidade_devolvida ≤ quantidade faturada`), então essa flag é uma checagem de sanidade,
  não uma regra de negócio aplicada (não corrigimos nada automaticamente, só sinalizamos).
- **`status_pedido_origem` é mantido na tabela** para dar visibilidade a um caso estranho (mas
  não necessariamente um erro): devolução referenciando um pedido que consta como Cancelado na
  origem. A validação do notebook mostra se isso ocorre.
