# Silver — dCalendario

## Pré-requisito

`silver/01_silver_dimensoes.py` já rodado com sucesso (usa `silver.feriados`).

## Como rodar

Mesmo fluxo dos notebooks anteriores: coloque `02_silver_dcalendario.py` na pasta `silver/`,
suba no GitHub, `Pull` no Databricks, `Run all`.

## Decisões registradas

- **Intervalo do calendário é fixo (2023-01-01 a 2027-03-31 por padrão), não derivado dos dados.**
  Cobre o histórico de `carteira_historica` (que começa em 2023) e a FY2027 inteira, mesmo com a
  base de vendas terminando em 12/08/2026 — assim o ano fiscal corrente não fica cortado no meio
  quando o dashboard precisar mostrar a meta do ano completo (R11).
- **Ano fiscal calculado por deslocamento de 3 meses** (`ano_fiscal = ano + 1` se `mes >= 4`).
  Testado explicitamente na validação ao redor da virada mar/abr de 2026.
- **`flag_dia_util` é nacional, não por UF.** Feriados estaduais existem em `feriados.csv` mas
  um único flag por data só é exato em nível nacional — aplicar por UF exigiria um calendário
  segmentado por estado (cruzando com a UF do cliente/filial), que fica como extensão possível,
  não como algo esquecido.
- **`data_sk` (inteiro `yyyyMMdd`) criado ao lado de `data`**: convenção comum de chave
  substituta para tabela de datas em modelo estrela, útil se o modelo for carregado no Power BI.
