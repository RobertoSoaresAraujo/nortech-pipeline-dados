# Gold — fVendas

## Pré-requisito

`gold/01_gold_dimensoes.py` já rodado (usa `dClientes`, `dProdutos`, `dVendedores`).

## Como rodar

Mesmo fluxo: `gold/02_gold_fvendas.py` na pasta `gold/`, sobe no GitHub, `Pull`, confere o
widget `catalog`, `Run all`.

## Decisões registradas

- **Vendedor responsável resolvido no ETL, não em DAX** (decisão já explicada na conversa de
  arquitetura). Prioridade: `carteira_historica` (vigente na data de emissão) > vendedor gravado
  no pedido > sem vendedor. `vendedor_fonte` deixa essa escolha auditável linha a linha.
- **Nenhum join toca diretamente no código `V001` (ambíguo).** Toda resolução de vendedor separa
  primeiro nulo/ambíguo (vira sentinela sem nenhum join) dos casos normais (só esses tocam
  `dVendedores`) — porque `dVendedores` tem duas linhas reais pra esse código, e um join direto
  causaria fan-out (a venda apareceria em dobro, uma vez por pessoa). A célula de validação final
  confirma isso: `(safra, id_pedido, item)` não pode se repetir depois da junção.
- **`sk_vendedor_pedido` e `sk_vendedor_responsavel` guardados separados**, mesmo quando
  coincidem. Dá pra comparar os dois na Página 3 (governança) — quantas vendas o vendedor
  "oficial" da carteira diverge do vendedor gravado no pedido é, em si, um dado de qualidade
  interessante.
- **Chave de data é a própria `data_emissao` (tipo Date), não um inteiro substituto.** Diferente
  das outras dimensões, a relação com `dCalendario` deve ser por coluna Date de verdade — é
  isso que habilita a Time Intelligence nativa do Power BI (`TOTALYTD`, `SAMEPERIODLASTYEAR`
  etc.), que não funciona bem sobre uma chave inteira.
- **Reconciliação de linhas obrigatória na validação**: `silver.vendas` e `gold.fVendas` têm que
  ter exatamente a mesma contagem — resolver chave nunca deveria adicionar nem remover linha,
  só trocar a chave de negócio pela substituta.
