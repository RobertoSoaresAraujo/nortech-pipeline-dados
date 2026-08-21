# Gold — fMetas

## Pré-requisito

`gold/01_gold_dimensoes.py` já rodado (usa `dRegiaoSegmento`).

## Como rodar

Mesmo fluxo: `gold/04_gold_fmetas.py` na pasta `gold/`, sobe no GitHub, `Pull`, confere o
widget `catalog`, `Run all`.

## Decisões registradas

- **Um fato só, não dois** (`fMetas`, grão região×segmento×mês), mesmo com a Meta Ano guardada
  separada na Silver. A Meta Ano vem denormalizada como `meta_ano_referencia`, repetida nas 12
  linhas do mesmo ano/região/segmento — mantém o modelo com um fato por conceito do case (que
  pede `fVendas`, `fDevolucoes`, `fMetas`), sem precisar de uma quarta tabela.
- **`meta_ano_referencia` não pode ser somada direto** — como está repetida 12x, um `SUM` ingênuo
  multiplicaria o orçamento anual por 12. Isso fica documentado no próprio comentário do
  notebook e será importante lembrar na hora de escrever a medida `% Atingimento da Meta` (usar
  `SUMX` sobre combinações distintas de região+segmento+ano, não `SUM` direto na coluna). A
  célula de validação final demonstra esse efeito de propósito, pra não esquecer.
- **Sem chave órfã aqui** — região/segmento da planilha de metas sempre batem com
  `dRegiaoSegmento` (construída a partir da união de clientes + metas no notebook 1), então não
  existe sentinela "Não Identificado" necessário neste fato.
- **`meta_mensal` nula é preservada** (24 casos — Governo em Norte/Centro-Oeste, alguns meses).
  Não é erro, é meta genuinamente não definida (R7/dicionário) — vai virar `% Atingimento` em
  branco na Gold/DAX, não zero, exatamente como o case exige.
