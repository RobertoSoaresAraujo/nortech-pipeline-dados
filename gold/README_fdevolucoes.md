# Gold — fDevolucoes

## Pré-requisito

`gold/02_gold_fvendas.py` já rodado (usa `fVendas` para denormalizar cliente/produto).

## Como rodar

Mesmo fluxo: `gold/03_gold_fdevolucoes.py` na pasta `gold/`, sobe no GitHub, `Pull`, confere o
widget `catalog`, `Run all`.

## Decisões registradas

- **Sem relação física com `fVendas` no modelo final** — decisão já explicada na conversa de
  arquitetura. `id_cliente`/`id_produto` são denormalizados uma única vez aqui no ETL, puxando
  as chaves já resolvidas em `fVendas` (não refaz o trabalho de resolução, só reaproveita).
- **Órfãs (pedido de origem não encontrado) caem no mesmo sentinela "Não Identificado" (`-1`)**
  usado em `fVendas` — consistência entre os fatos: a mesma chave `-1` sempre significa a mesma
  coisa em qualquer tabela do modelo.
- **Join contra `fVendas` é seguro por construção**: o grão de `fVendas` já foi validado como
  único (`safra+id_pedido+item`) no notebook anterior, então não existe risco de fan-out aqui —
  mas a validação final confere `id_devolucao` de qualquer forma, por hábito de nunca assumir.
