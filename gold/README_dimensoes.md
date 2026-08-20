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
- **Bug pego rodando de verdade**: colunas booleanas derivadas de expressões como `isNull()`
  (ex: `eh_matriz`) são inferidas pelo Spark como `NOT NULL` — a expressão nunca retorna nulo,
  então o Spark marca assim no schema. Isso quebrava a criação da linha sentinela (que
  propositalmente tem `None` nesses campos). Corrigido forçando `nullable=True` em todo o
  schema antes de montar a sentinela.
- **Achado real nos dados: matriz órfã.** `C00018` declara `id_matriz = C09999`, mas `C09999`
  não existe como `id_cliente` em lugar nenhum da base (`9999` é um clássico código-placeholder
  de sistema legado). A primeira versão da lógica mascarava isso: como o `COALESCE` caía no
  nome do próprio `C00018` quando a matriz não era encontrada, parecia que ele era "sua própria
  matriz" — o que é diferente de "não tem matriz declarada". Corrigido com uma flag
  `matriz_orfa` explícita e um `nome_grupo_economico` que deixa claro que a matriz não foi
  encontrada, em vez de mascarar com o nome da filial.
- **Aviso "No Partition Defined for Window operation" é esperado e inofensivo aqui.** A geração
  de chave substituta via `row_number()` sem `partitionBy` ordena a tabela inteira numa única
  partição — para os volumes desse projeto (poucas centenas a poucos milhares de linhas por
  dimensão) isso não é problema. Numa dimensão de milhões de linhas, valeria trocar a estratégia
  (ex: `monotonically_increasing_id()` combinada com um offset por partição).
