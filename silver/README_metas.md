# Silver — Fato de metas comerciais

## Pré-requisito

Bronze rodado (usa `bronze.metas_comerciais_2024/2025/2026`).

## Como rodar

Mesmo fluxo: coloque `05_silver_metas.py` na pasta `silver/`, suba no GitHub, `Pull` no
Databricks (conferindo o widget `catalog`), `Run all`.

## Decisões registradas

- **Duas tabelas, não uma**: `silver.metas_anuais` (grão ano×região×segmento, vem de `Meta_Ano`)
  e `silver.metas_mensais` (grão ano×região×segmento×mês, vem das colunas Jan–Dez
  desnormalizadas). A aba `Premissas` da planilha é explícita: a Meta Ano **prevalece** sobre a
  soma dos meses. Forçar as duas a baterem seria inventar uma regra de rateio que a Diretoria não
  definiu — por isso ficam separadas, e uma célula de validação mostra exatamente onde e quanto
  elas divergem, para decisão consciente na hora de montar as medidas de "% Atingimento" na Gold.
- **`Regiao`/`Segmento` já vêm em maiúsculas padronizadas na planilha** (`CENTRO-OESTE`,
  `INDUSTRIA` etc.) — reaproveitamos exatamente o mesmo dicionário de normalização
  (`SEGMENTO_MAP`/`REGIAO_MAP`) do notebook 1, sem precisar de ajuste.
- **Valores de meta misturam número puro com `R$ 1.135.400,00` formatado**, no mesmo padrão já
  visto em `vendas_2024`/`2025`/`2026`. Reaproveitado o mesmo parser (`parse_valor_brl`).
- **Linha `TOTAL GERAL` isolada, não descartada silenciosamente**: filtrar por "segmento
  reconhecido" já exclui essa linha, a linha em branco e a observação sobre o segmento Governo —
  mas a `TOTAL GERAL` é guardada à parte (`df_total_geral`) e usada numa célula de validação para
  conferir, mês a mês, se a soma dos nossos dados desnormalizados bate com o total que a própria
  planilha já calculava. Isso funciona como um teste de regressão para a lógica de parsing.
- **`data_referencia` (primeiro dia do mês) criada em `metas_mensais`** para facilitar o join
  futuro com `dCalendario` na Gold. Atenção: a planilha organiza a meta por **ano civil**
  (Jan–Dez), enquanto a receita usa **ano fiscal** (abr–mar, R6) — o join por data resolve isso
  naturalmente (`dCalendario` já traduz qualquer data pro ano fiscal correto), mas é importante
  ter isso em mente na Gold: "Meta do ano fiscal FY2026" não é a mesma coisa que "Meta Ano da
  aba Metas 2026" — a primeira mistura meses de duas planilhas diferentes (abr–dez/2025 da aba
  2025 + jan–mar/2026 da aba 2026).
- **A observação da planilha sobre o segmento Governo** ("metas dependem de homologação de
  contratos públicos") foi lida mas não virou regra automática — é contexto de negócio para
  quem for interpretar os números de Governo, não uma transformação a aplicar.

## Achado: 24 células de meta mensal vêm vazias (NULL) — e isso está correto

Todas em `segmento = Governo`, regiões `Norte` e `Centro-Oeste`, nos meses 3/6/9/12 (fechamento
de trimestre), nos 3 anos. O dicionário de dados já avisava: *"Célula vazia = meta não definida
para a combinação (≠ meta zero)"*. Mantivemos como `NULL` propositalmente — **não** convertemos
para `0`. Isso importa porque o próprio case exige, na seção de DAX, que `% Atingimento da Meta`
fique **em branco (não zero)** quando não houver meta definida; se tivéssemos zerado aqui, esse
requisito ficaria impossível de cumprir depois na Gold (não haveria como distinguir "meta zero"
de "meta não definida").

## Aba Premissas — texto completo e o que cada item confirma

1. *"Metas definidas por Regiao x Segmento; nao ha desdobramento por cliente, produto ou
   vendedor."* — implementado (grão de `metas_anuais`/`metas_mensais`).
2. *"Valores expressos em BRL, liquidos de impostos e de devolucoes."* — **confirma** que a
   comparação Meta × Realizado na Gold precisa usar a Receita Líquida já descontada da devolução
   (`receita_liquida_venda − receita_devolucao_liquida`, notebook 4), não a receita bruta da
   venda isolada. Sobre impostos: nenhum arquivo do case traz dado de imposto, então não há
   ajuste a fazer — registrar isso no `DECISOES.md` como escopo consciente, não esquecimento.
3. *"Exportacoes (moeda USD) entram na meta convertidas pela PTAX de venda da data de emissao."*
   — mesma metodologia de R3, aplicada no fato de vendas. Nenhuma ação adicional necessária.
4. *"A coluna 'Meta Ano' reflete o orcamento aprovado em comite e prevalece sobre a soma dos
   meses."* — implementado (tabelas separadas, sem reconciliação forçada).
5. *"Ano fiscal da companhia: abril a marco (FY26 = abr/2025 a mar/2026)."* — implementado no
   notebook 2, e o próprio exemplo do arquivo bate com a fórmula usada e validada na virada
   mar→abr/2026.
