# Silver — Quarentena consolidada e qualidade de dados

## Pré-requisito

Todos os notebooks anteriores (1 a 5) já rodados — este junta o que cada um já produziu.

## Como rodar

Mesmo fluxo: coloque `06_silver_quarentena.py` na pasta `silver/`, suba no GitHub, `Pull` no
Databricks (conferindo o widget `catalog`), `Run all`.

## O que este notebook entrega (mapeado direto pra Página 3 do case)

| Exigência do case (seção 5.5) | Tabela produzida |
|---|---|
| Volume processado por safra | `silver.qualidade_volume_processado` |
| Registros rejeitados por motivo (R12) | `silver.qualidade_rejeicoes` |
| Chaves órfãs por dimensão | `silver.qualidade_chaves_orfas` |
| Devoluções sem pedido de origem (R5) | `silver.devolucoes_orfas` (já existia, do notebook 4) |
| Data/hora da última atualização e período coberto | `silver.qualidade_metadata_atualizacao` |

Bônus (não pedido explicitamente, mas decorre do que já tínhamos sinalizado):
`silver.qualidade_chaves_ambiguas` — chaves que **existem** mas apontam para mais de um
registro (CNPJ duplicado em clientes, `id_vendedor` V001 duplicado e sua propagação).

## Decisões registradas

- **Detecção separada de correção.** Este notebook só quantifica e classifica — não decide
  "membro Não Identificado" nem preenche nada. Essa é uma decisão de modelagem dimensional
  (seção 5.2 do case) que pertence à camada Gold. Misturar as duas coisas aqui dificultaria
  auditar separadamente "o que está órfão" de "como resolvemos isso no modelo".
- **Checagem de integridade referencial é nova aqui, não retrabalho.** Os notebooks 1-5 sinalizam
  casos *específicos* que apareceram durante o desenvolvimento (V001, CNPJ duplicado). Este
  notebook faz uma varredura *sistemática* de toda chave estrangeira do fato de vendas e da
  carteira histórica contra as dimensões correspondentes — pode revelar órfãos que não tinham
  aparecido antes por acaso.
- **`id_vendedor` nulo não conta como chave órfã.** É um valor legítimo (canal E-commerce não
  tem executivo associado, conforme o dicionário) — contá-lo como "órfão" infuflaria o número
  artificialmente e mascararia os órfãos de verdade.
- **Duas tabelas de "chave problemática", não uma**: `qualidade_chaves_orfas` (a chave não existe
  em lugar nenhum na dimensão) e `qualidade_chaves_ambiguas` (a chave existe, só que mais de uma
  vez, com significados diferentes). São problemas estruturalmente diferentes e pedem tratamento
  diferente no modelo — vale a pena não misturar os dois na mesma métrica.
