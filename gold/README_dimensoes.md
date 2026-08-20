# Gold — Dimensões

## Pré-requisito

Toda a Silver (notebooks 1-6) já rodada.

## Como rodar

Novo padrão de pastas: crie `gold/` no seu repositório (mesmo processo de sempre: pasta nova no
GitHub, arquivo dentro, `Pull` no Databricks). Confira o widget `catalog`, `Run all`.

## Decisões registradas

- **Chave substituta por `row_number()` sobre a chave de negócio ordenada, não hash nem
  `monotonically_increasing_id()`.** Como o pipeline sempre faz overwrite completo (não é
  upsert incremental), não existe risco de "chave muda entre execuções para o mesmo registro
  causar inconsistência incremental" — mas ainda assim, ordenar pela chave de negócio garante
  que a mesma linha sempre caia na mesma posição/chave a cada rebuild, o que ajuda a debugar
  (chave previsível) e evita colisão teoricamente possível (ainda que improvável) de um hash.
- **Sentinelas com chave negativa, dados reais começando em 1.** Zero colisão garantida por
  construção, sem depender de probabilidade.
- **Três sentinelas em `dVendedores`, não uma.** "Sem vendedor" (0, canal E-commerce — value
  válido, não é erro), "não identificado" (-1, chave que não existe — defensivo, não ocorreu nos
  dados mas o modelo precisa aguentar se ocorrer) e "ambíguo" (-2, o caso do `V001`) são conceitos
  diferentes — misturar todos num "Desconhecido" genérico esconderia a causa raiz de cada um.
- **Hierarquia matriz/filial achatada em `id_grupo_economico`/`nome_grupo_economico`**, resolvida
  no ETL (decisão já explicada na conversa de arquitetura — hierarquia de 2 níveis não justifica
  parent-child em DAX).
- **`dRegiaoSegmento` construída a partir da união de `clientes` e `metas_anuais`**, não só de
  um dos dois — garante que a dimensão cubra qualquer combinação usada por qualquer um dos dois
  fatos que vão se conectar a ela (`fVendas` via `dClientes`, `fMetas` diretamente).
- **Bug pego antes de rodar**: `Row(**dict)` no PySpark reordena campos alfabeticamente por
  baixo dos panos — usar isso pra montar as linhas sentinela desalinharia os valores com o
  schema real silenciosamente (ex: o valor de `cargo` cairia na coluna errada). Corrigido
  construindo tuplas posicionais explícitas na ordem exata de `df_real.columns`.
