# Silver — Fato de vendas consolidado

## Pré-requisito

`silver/01_silver_dimensoes.py` (usa `silver.cambio_usd` e `silver.vendedores`) já rodado.

## Como rodar

Mesmo fluxo: coloque `03_silver_vendas.py` na pasta `silver/`, suba no GitHub, `Pull` no
Databricks, `Run all`.

## Decisões registradas

- **Grão do fato: `safra + id_pedido + item`.** O `safra` evita colisão entre pedidos de anos
  diferentes que por acaso compartilhem o mesmo número.
- **`valor_unitario` de 2024 mistura dois formatos na mesma coluna** (`135,33` puro e
  `R$ 5.478,44` com símbolo e separador de milhar). Uma única função (`parse_valor_brl`) trata
  os dois casos — remove `R$`, remove separador de milhar, troca vírgula por ponto.
- **Semântica de desconto é diferente em cada safra** (conforme dicionário): 2024 e 2026 vêm em
  pontos percentuais (`12,50` = 12,5%, precisa dividir por 100); 2025 já vem em fração decimal
  (`0.1250` = 12,5%, usa direto). Todas convertidas para a mesma fração antes de calcular receita.
- **Conversão cambial (R3) via "última cotação anterior"**, não um join direto por data — como
  `cambio_usd` só tem dias de pregão, um join direto perderia toda venda em fim de semana ou
  feriado. Implementado com join por intervalo (`data_cotacao <= data_emissao`) seguido de
  `row_number()` pegando a cotação mais próxima.
- **R4 (cancelamento) unificado num único `status_pedido`**: 2024 deriva de `quantidade < 0`
  (não tem coluna de status); 2025/2026 usam `status_pedido` da origem direto. A receita de
  pedidos cancelados é zerada explicitamente (`receita_bruta = 0`), não apenas fica negativa por
  causa da quantidade negativa — isso evita que cancelamentos "compensem" receita de outros
  pedidos de forma pouco clara em somas.
- **R12 aplicado antes de qualquer cálculo de receita**: linhas sem quantidade, sem valor ou sem
  data válida vão para `silver.vendas_rejeitadas` com o motivo, e não entram no fato. A conversão
  cambial e as regras de negócio só rodam em cima do que sobrou (dado válido).
- **`frete_rateado` (2026) mantido como coluna separada**, nunca somado à receita — o dicionário
  é explícito que ele não compõe receita.
- **`id_vendedor_ambiguo` propagado para o fato**, reaproveitando o alerta criado no notebook 1
  para o código `V001` duplicado. Nenhuma venda é atribuída "no chute" a um dos dois vendedores.
- **`id_produto_chave` (upper+trim) criado no fato também**, espelhando a mesma chave já criada
  em `silver.produtos`, para o join da Gold funcionar independente da caixa do código.

- **`vendas_2025` tinha 420 linhas exatamente duplicadas** (mesmo `id_pedido`+`item`, todos os
  campos idênticos) — confirmado nos dados brutos, e coerente com o aviso do dicionário sobre
  reprocessamento incremental sem controle de idempotência. Removidas com `dropDuplicates()`
  logo após unir as 3 safras, **antes** da conversão cambial e do cálculo de receita. Sem essa
  remoção, as 403 duplicatas em BRL contariam receita em dobro silenciosamente, e as 17 em USD
  interagiriam de forma imprevisível com o join assíncrono de câmbio (a ordem das operações aqui
  importa: dedup primeiro, cálculo depois).
- **Grão do fato `safra+id_pedido+item` é único de verdade após a remoção de duplicatas** —
  conferido em cada execução pela célula de reconciliação (bronze = válidas + rejeitadas +
  duplicatas removidas, por safra).

## O que ainda falta (próximos notebooks)

- Devoluções (R5) — reduzem a receita líquida na data da devolução, não na data da venda.
- Metas (R7) — grão diferente (região×segmento×mês), sem desdobrar para o grão do fato de vendas.
- Consolidação final da quarentena (notebook 6) — hoje `vendas_rejeitadas` já existe isolada;
  falta juntar com as rejeições de devoluções e outras fontes num único lugar.
