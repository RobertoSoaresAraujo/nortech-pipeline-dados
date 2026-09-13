# Gold — Qualidade de dados e governança

## Pré-requisito

`silver/06_silver_quarentena.py` já rodado.

## Como rodar

Mesmo fluxo: `gold/05_gold_qualidade.py` na pasta `gold/`, sobe no GitHub, `Pull`, confere o
widget `catalog`, `Run all`.

## Decisões registradas

- **Passthrough direto para 5 das 6 tabelas** — o trabalho de cálculo já foi feito na Silver
  (notebook 6); a Gold aqui só torna os dados endereçáveis pro Power BI, sem reprocessar nada.
- **`atualizado_em` é recalculado na Gold, não herdado da Silver.** `periodo_inicio`/`periodo_fim`
  (o que a base cobre) continuam vindo de lá sem mudança, mas o timestamp de "quando os dados
  foram atualizados pela última vez" precisa refletir a execução mais recente do pipeline
  completo — que termina aqui, na Gold, não na Silver. Se alguém rodar só a Silver de novo sem
  rodar a Gold em seguida, esse timestamp não deveria dizer que está tudo atualizado.

## Com isso, a Gold está completa

6 dimensões + 3 fatos (`fVendas`, `fDevolucoes`, `fMetas`) + 5 tabelas de qualidade/quarentena —
10 tabelas ao todo, todas prontas para consumo direto no Power BI.
