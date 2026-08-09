# Exercícios de Preparação — Teste Técnico do Sistema de Educação

Simulação de exercícios prováveis para um teste de engenharia/arquitetura de dados, cobrindo arquitetura medallion (Bronze/Silver/Gold), Delta Lake, Databricks e modelagem para consumo por LLM via MCP.

---

## Exercício 1 — Deduplicação e "registro mais recente" (Bronze → Silver)

**Contexto de negócio:** O sistema de educação tem um sistema de matrícula de alunos que envia eventos toda vez que um cadastro é criado ou atualizado (mudança de turma, status, dados cadastrais). Esses eventos chegam na camada Bronze via ingestão de CDC (Change Data Capture) de um banco transacional, e podem chegar fora de ordem, duplicados, ou com updates parciais.

**Dataset de exemplo** (`bronze_matriculas.csv`):

```csv
evento_id,aluno_id,nome,turma,status,data_evento,fonte_ingestao
ev001,1001,Maria Silva,7A,ativo,2026-01-05T08:00:00,cdc
ev002,1001,Maria Silva,7A,ativo,2026-01-05T08:00:00,cdc
ev003,1001,Maria Silva,7B,ativo,2026-01-10T09:15:00,cdc
ev004,1002,João Souza,8A,ativo,2026-01-06T10:00:00,cdc
ev005,1001,Maria Silva,7B,trancado,2026-01-09T14:00:00,cdc
ev006,1002,João Souza,8A,inativo,2026-01-20T11:30:00,cdc
ev007,1003,,9A,ativo,2026-01-07T07:45:00,cdc
ev008,1002,João Souza,8B,ativo,2026-01-15T09:00:00,backfill
ev009,1003,Ana Costa,9A,ativo,2026-01-07T07:50:00,cdc
```

**Observações sobre os dados:**

- `ev001` e `ev002` são duplicatas exatas.
- `ev003` e `ev005` têm `data_evento` diferentes mas não estão em ordem cronológica de chegada — repare que `ev005` (14:00) tem timestamp posterior a `ev003` (09:15), mas o campo `turma` de `ev003` já reflete uma mudança que só é seguida pelo `status` em `ev005`.
- `ev004`, `ev006` e `ev008` mostram um caso onde a mesma entidade (`aluno_id=1002`) recebe eventos de fontes diferentes (`cdc` e `backfill`) que podem conflitar quanto a qual é "mais recente" na prática.
- `ev007` tem `nome` nulo, que é corrigido em `ev009`.

**Enunciado:**
Escreva um pipeline (PySpark ou SQL) que transforme `bronze_matriculas` em uma tabela Silver `silver_matriculas`, contendo **apenas o registro mais recente e correto por aluno**, resolvendo:

1. Duplicatas exatas.
2. Definição de "mais recente" quando existem múltiplas fontes de ingestão com timestamps próximos ou conflitantes.
3. Tratamento de campos nulos que foram corrigidos em eventos posteriores.

**Entregável esperado:**

- Código do pipeline.
- Justificativa da estratégia de desempate (qual coluna/critério você usou para definir "mais recente" e por quê).
- Schema final da tabela Silver.

---

## Exercício 2 — Schema Evolution (novo campo no meio do fluxo)

**Contexto de negócio:** O sistema de educação captura eventos de acesso a uma plataforma de ensino (logins, acessos a videoaulas, submissões de exercícios). Em um certo dia, o time de produto adiciona um novo campo (`dispositivo`) ao evento sem avisar o time de dados, e mais tarde adiciona também um campo aninhado (`metadata`) com informações de app version.

**Dataset de exemplo — dia 1** (`bronze_eventos_dia1.csv`):

```csv
evento_id,aluno_id,tipo_evento,timestamp
e1001,2001,login,2026-02-01T08:00:00
e1002,2001,acesso_videoaula,2026-02-01T08:05:00
e1003,2002,submissao_exercicio,2026-02-01T09:20:00
```

**Dataset de exemplo — dia 2** (`bronze_eventos_dia2.csv`, campo novo `dispositivo`):

```csv
evento_id,aluno_id,tipo_evento,timestamp,dispositivo
e2001,2001,login,2026-02-02T08:00:00,mobile
e2002,2003,login,2026-02-02T08:30:00,web
e2003,2003,acesso_videoaula,2026-02-02T08:45:00,web
```

**Dataset de exemplo — dia 3** (`bronze_eventos_dia3.json`, campo aninhado novo):

```json
[
  {"evento_id": "e3001", "aluno_id": 2001, "tipo_evento": "login", "timestamp": "2026-02-03T08:00:00", "dispositivo": "mobile", "metadata": {"app_version": "4.2.1", "os": "android"}},
  {"evento_id": "e3002", "aluno_id": 2002, "tipo_evento": "submissao_exercicio", "timestamp": "2026-02-03T09:00:00", "dispositivo": "web", "metadata": null}
]
```

**Enunciado:**
Você precisa consolidar esses três lotes em uma única tabela Delta `bronze_eventos_acesso`, considerando que:

1. O schema muda ao longo do tempo (campo novo, depois campo aninhado).
2. A ingestão precisa continuar funcionando sem quebrar quando chegarem lotes futuros com colunas adicionais.
3. Você não pode perder ou truncar dados dos lotes anteriores que não tinham essas colunas.

Descreva/implemente como configurar a ingestão para lidar com schema evolution automaticamente, e como isso afetaria consultas na camada Silver que dependem de `dispositivo` ou `metadata.app_version`.

**Entregável esperado:**

- Configuração de escrita Delta (opções relevantes de schema evolution).
- Explicação de como tratar valores ausentes de `dispositivo`/`metadata` nos registros antigos.
- Uma consulta de exemplo que funcione independentemente de o registro ter ou não o campo `metadata`.

---

## Exercício 3 — Agregações e métricas de negócio (Silver → Gold)

**Contexto de negócio:** A camada Silver tem uma tabela de submissões de exercícios por aluno, já limpa e deduplicada. O time de produto quer métricas de engajamento por escola e por turma para um dashboard executivo, e também para alimentar consultas em linguagem natural via LLM (ex: "qual turma teve a maior taxa de acerto em fevereiro?").

**Dataset de exemplo** (`silver_submissoes.csv`):

```csv
submissao_id,aluno_id,escola_id,turma_id,disciplina,data_submissao,correta,tempo_gasto_segundos
s001,2001,esc01,7A,matematica,2026-02-01,true,120
s002,2001,esc01,7A,matematica,2026-02-02,false,300
s003,2002,esc01,7A,matematica,2026-02-01,true,90
s004,2003,esc01,7B,matematica,2026-02-03,true,150
s005,2004,esc02,8A,portugues,2026-02-01,false,200
s006,2004,esc02,8A,portugues,2026-02-02,true,60
s007,2005,esc02,8A,portugues,2026-02-04,true,80
s008,2001,esc01,7A,portugues,2026-02-05,true,110
s009,2003,esc01,7B,matematica,2026-02-10,false,400
s010,2002,esc01,7A,matematica,2026-02-15,true,95
```

**Enunciado:**
Construa uma tabela Gold `gold_engajamento_turma` com métricas agregadas por `escola_id`, `turma_id`, `disciplina` e mês, incluindo pelo menos:

- Total de submissões.
- Taxa de acerto (% de `correta = true`).
- Tempo médio gasto por submissão.
- Número de alunos distintos que submeteram pelo menos um exercício.

Considere que essa tabela Gold será consultada por um agente LLM via MCP para responder perguntas em linguagem natural, então pense em nomes de colunas e granularidade que facilitem esse uso.

**Entregável esperado:**

- Código da agregação.
- Schema final da tabela Gold, com comentário sobre por que escolheu essa granularidade.
- Um exemplo de pergunta em linguagem natural que essa tabela conseguiria responder diretamente, e uma que exigiria uma tabela adicional.

---

## Exercício 4 — Modelagem dimensional simples para a camada Gold

**Contexto de negócio:** O sistema de educação quer uma camada Gold estruturada como star schema para relatórios de desempenho escolar, cobrindo alunos, escolas, disciplinas e avaliações (provas), permitindo cruzar desempenho ao longo do tempo.

**Dataset de exemplo — tabela de fatos bruta** (`silver_avaliacoes.csv`):

```csv
avaliacao_id,aluno_id,escola_id,disciplina_id,data_avaliacao,nota,nota_maxima,tipo_avaliacao
a001,2001,esc01,disc_mat,2026-01-15,8.5,10,prova_bimestral
a002,2002,esc01,disc_mat,2026-01-15,7.0,10,prova_bimestral
a003,2001,esc01,disc_port,2026-01-20,9.0,10,prova_bimestral
a004,2004,esc02,disc_port,2026-01-22,6.5,10,simulado
a005,2005,esc02,disc_mat,2026-01-25,10.0,10,simulado
```

**Dataset de exemplo — dimensões auxiliares** (`dim_escolas.csv`, `dim_disciplinas.csv`):

```csv
escola_id,nome_escola,cidade,rede
esc01,Colégio Alfa,Curitiba,privada
esc02,Escola Beta,Belo Horizonte,publica
```

```csv
disciplina_id,nome_disciplina,area_conhecimento
disc_mat,Matemática,exatas
disc_port,Português,linguagens
```

**Enunciado:**
Modele um star schema para a camada Gold contendo:

- Uma tabela fato de avaliações (`fato_avaliacoes`), com granularidade de uma linha por avaliação/aluno.
- Dimensões: `dim_aluno`, `dim_escola`, `dim_disciplina`, `dim_tempo`.
- Defina as chaves (naturais vs. surrogate keys) e como você trataria mudanças em atributos da dimensão escola (ex: uma escola muda de rede de ensino) — Slowly Changing Dimension.

**Entregável esperado:**

- Diagrama ou descrição textual do esquema estrela (tabelas e relacionamentos).
- Definição de chaves primárias/estrangeiras.
- Explicação de qual tipo de SCD (0, 1, 2) você usaria para `dim_escola` e por quê.

---

## Exercício 5 — Particionamento e performance no Spark

**Contexto de negócio:** A tabela Bronze de eventos de acesso à plataforma (do Exercício 2, mas agora em escala real) tem bilhões de linhas acumuladas ao longo de 2 anos, com todas as escolas do Brasil. Um job diário que calcula métricas do dia anterior está demorando cada vez mais e sofrendo com data skew (algumas escolas geram 100x mais eventos que outras).

**Dataset de exemplo (amostra representativa)** (`bronze_eventos_sample.csv`):

```csv
evento_id,aluno_id,escola_id,tipo_evento,timestamp
e1,2001,esc01,login,2026-02-01T08:00:00
e2,2001,esc01,acesso_videoaula,2026-02-01T08:05:00
e3,2002,esc01,login,2026-02-01T08:10:00
e4,2004,esc02,login,2026-02-01T08:00:00
e5,2010,esc03,login,2026-02-01T08:00:00
e6,2011,esc03,login,2026-02-01T08:01:00
e7,2012,esc03,login,2026-02-01T08:02:00
e8,2013,esc03,acesso_videoaula,2026-02-01T08:03:00
e9,2014,esc03,submissao_exercicio,2026-02-01T08:04:00
e10,2020,esc04,login,2026-02-01T08:00:00
```

*(Considere que, na base real, `esc03` representa uma rede municipal inteira com milhões de eventos/dia, enquanto escolas como `esc01`, `esc02` e `esc04` têm poucas centenas.)*

**Enunciado:**

1. Proponha uma estratégia de particionamento físico para a tabela Delta (`PARTITIONED BY`) considerando os padrões de consulta (jobs diários que filtram por data, e consultas ad-hoc que filtram por escola).
2. Explique como você identificaria e mitigaria o data skew causado por `esc03` durante um `groupBy(escola_id)`.
3. Descreva pelo menos duas técnicas de otimização do Spark que aplicaria aqui (ex: salting, AQE, broadcast join, Z-ordering, bucketing) e quando cada uma faz sentido.

**Entregável esperado:**

- Estratégia de particionamento justificada (colunas e granularidade).
- Explicação técnica de como detectar skew (ex: via Spark UI) e a solução escolhida.
- Trecho de código ilustrando a técnica de mitigação escolhida.

---

## Exercício 6 — Delta Lake: merge/upsert, time travel e versionamento

**Contexto de negócio:** Um sistema acadêmico envia diariamente um snapshot completo de notas de alunos, mas às vezes reprocessa notas de dias anteriores (correção de nota lançada errada) ou envia uma nota duplicada com pequenas variações. O sistema de educação também precisa auditar mudanças de notas para investigar reclamações de pais/alunos.

**Dataset de exemplo — tabela Delta existente** (`silver_notas` estado atual):

```csv
aluno_id,disciplina_id,bimestre,nota,ultima_atualizacao
2001,disc_mat,1,7.5,2026-02-01T10:00:00
2002,disc_mat,1,8.0,2026-02-01T10:00:00
2003,disc_port,1,6.0,2026-02-01T10:00:00
```

**Dataset de exemplo — novo lote recebido** (`novo_lote_notas.csv`):

```csv
aluno_id,disciplina_id,bimestre,nota,ultima_atualizacao
2001,disc_mat,1,8.5,2026-02-05T09:00:00
2003,disc_port,1,6.0,2026-02-05T09:00:00
2004,disc_mat,1,9.0,2026-02-05T09:00:00
```

**Enunciado:**

1. Escreva a lógica de `MERGE INTO` que atualiza `silver_notas` com o `novo_lote_notas`, considerando que:
   - `2001` teve a nota corrigida (deve atualizar).
   - `2003` enviou o mesmo valor de novo (não deveria gerar uma escrita desnecessária, idealmente).
   - `2004` é um aluno novo (deve inserir).
2. Um pai de aluno reclama que a nota do filho (`aluno_id=2001`) "mudou sem aviso". Descreva como você usaria **time travel** do Delta Lake para investigar o que aconteceu e reconstituir o histórico de mudanças daquele registro.
3. Explique como configuraria `VACUUM` e retenção de logs nessa tabela para equilibrar auditoria histórica com custo de armazenamento.

**Entregável esperado:**

- Comando `MERGE INTO` completo.
- Consulta usando `VERSION AS OF` ou `TIMESTAMP AS OF` para investigar o histórico.
- Recomendação de política de retenção (dias) com justificativa considerando o caso de uso de auditoria.

---

## Exercício 7 — Unity Catalog: governança e dados sensíveis (LGPD)

**Contexto de negócio:** O sistema de educação lida com dados de alunos menores de idade (nome, CPF do responsável, endereço, notas). O time de dados precisa dar acesso à camada Gold para analistas de produto e para o agente LLM/MCP, mas sem expor dados pessoais sensíveis, e precisa provar isso em uma auditoria de LGPD.

**Dataset de exemplo** (`gold_alunos_perfil.csv`):

```csv
aluno_id,nome_completo,cpf_responsavel,data_nascimento,escola_id,email_responsavel,nota_media
2001,Maria Silva,111.222.333-44,2014-03-10,esc01,resp.maria@email.com,8.2
2002,João Souza,222.333.444-55,2013-11-02,esc01,resp.joao@email.com,7.5
2003,Ana Costa,333.444.555-66,2015-01-20,esc02,resp.ana@email.com,9.0
```

**Enunciado:**

1. Desenhe a estrutura de catálogo/schema/tabela no Unity Catalog (ex: `catalog.schema.tabela`) para separar dados sensíveis de dados analíticos, considerando pelo menos três grupos de acesso: `analistas_produto`, `agente_llm_mcp`, `auditoria_lgpd`.
2. Proponha uma estratégia de mascaramento/anonimização de coluna (ex: `cpf_responsavel`, `email_responsavel`) para que `analistas_produto` e `agente_llm_mcp` nunca vejam o dado bruto, mas `auditoria_lgpd` consiga ver quando necessário.
3. Explique como você implementaria isso tecnicamente (views com mascaramento, column-level ou row-level security do Unity Catalog, dynamic views) e como isso se propaga se essa tabela virar fonte de uma tool MCP consumida por um LLM externo.

**Entregável esperado:**

- Estrutura proposta de catálogo/schema/grants (pode ser em texto ou pseudo-SQL `GRANT`).
- Definição de qual mecanismo de mascaramento você usaria e por quê.
- Observação sobre o risco específico de expor esses dados via um agente LLM (ex: prompt injection tentando pedir o CPF "só pra confirmar o cadastro").

---

## Exercício 8 — Camada Gold para consulta em linguagem natural via MCP (com pegadinha de granularidade)

**Contexto de negócio:** O sistema de educação quer expor a camada Gold como uma tool MCP para um agente LLM responder perguntas como "qual escola teve a maior evolução de nota entre o 1º e o 2º bimestre?" ou "quantos alunos estão com risco de reprovação em matemática?". O time te entrega duas tabelas Gold já prontas e pede pra você validar se elas são suficientes, sem te contar que existe um problema de modelagem escondido.

**Dataset de exemplo** (`gold_notas_bimestre.csv`, granularidade aluno+disciplina+bimestre):

```csv
aluno_id,escola_id,disciplina_id,bimestre,nota
2001,esc01,disc_mat,1,7.5
2001,esc01,disc_mat,2,8.5
2002,esc01,disc_mat,1,8.0
2002,esc01,disc_mat,2,6.0
2003,esc02,disc_mat,1,5.0
2003,esc02,disc_mat,2,7.0
```

**Dataset de exemplo** (`gold_frequencia_bimestre.csv`, granularidade aluno+bimestre, **sem** `disciplina_id`):

```csv
aluno_id,escola_id,bimestre,percentual_presenca
2001,esc01,1,95
2001,esc01,2,90
2002,esc01,1,80
2002,esc01,2,70
2003,esc02,1,88
2003,esc02,2,92
```

**Enunciado:**

1. O agente LLM recebe a pergunta "quais alunos de matemática tiveram queda de nota E queda de frequência do bimestre 1 para o 2?". Um cruzamento ingênuo entre as duas tabelas (join simples por `aluno_id` e `bimestre`) pode gerar um problema de granularidade/duplicação quando o aluno tem mais de uma disciplina. Identifique exatamente qual é o problema e por que a resposta do LLM poderia sair sutilmente errada (não um erro óbvio, mas um número plausível e errado).
2. Redesenhe o schema Gold (uma tabela única, ou uma visão semântica) para eliminar essa ambiguidade antes de expor via MCP.
3. Escreva 2-3 perguntas em linguagem natural que essa nova modelagem consegue responder de forma inequívoca, e 1 pergunta que ainda exigiria uma tabela adicional (ex: comparação com a média da rede).

**Entregável esperado:**

- Explicação por escrito do problema de granularidade (o "porquê" importa mais que o código aqui).
- Novo schema Gold proposto (DDL ou descrição de colunas/granularidade).
- Lista de perguntas em linguagem natural cobertas e não cobertas pela nova modelagem.

---

## Exercício 9 — A pegadinha do "overwrite" ao virar a Gold (reprocessamento incremental)

**Contexto de negócio:** A tabela Gold `gold_frequencia_diaria` (frequência de alunos por escola e dia) já tem 6 meses de histórico acumulado. Todo dia o job reprocessa **apenas o dia anterior** (Silver → Gold) e precisa atualizar só aquela partição, sem tocar no resto do histórico. Um estagiário reporta que "a Gold ficou vazia, só apareceu o dia de ontem".

**Dataset de exemplo** (`gold_frequencia_diaria` — estado atual, já particionada por `dia`):

```csv
dia,escola_id,total_alunos,total_presentes
2026-02-01,esc01,120,110
2026-02-01,esc02,80,75
2026-02-02,esc01,120,115
2026-02-02,esc02,80,78
2026-02-03,esc01,120,112
```

**Dataset de exemplo — lote reprocessado, só o dia 2026-02-03** (`silver_frequencia_novo_lote.csv`):

```csv
dia,escola_id,total_alunos,total_presentes
2026-02-03,esc01,120,118
2026-02-03,esc02,80,79
```

**Enunciado:**

1. O estagiário escreveu algo equivalente a:
   ```python
   df_novo_lote.write.mode("overwrite").saveAsTable("gold_frequencia_diaria")
   ```

   Explique exatamente por que isso apaga os dias `2026-02-01` e `2026-02-02` mesmo o lote novo contendo só dados de `2026-02-03`, e por que o job "roda com sucesso" sem nenhum erro, o que torna esse bug perigoso (passa batido em code review rápido e nos logs).
2. Proponha pelo menos duas formas corretas de resolver isso, considerando que a tabela é particionada por `dia`:
   - Usando `partitionOverwriteMode` dinâmico do Spark/Delta.
   - Usando `replaceWhere` do Delta Lake.
   - (Bônus) Usando `MERGE INTO` como alternativa a overwrite.
3. Explique a diferença de comportamento entre `saveMode="overwrite"` estático vs dinâmico quando a tabela **não** é particionada pela coluna que você está tentando substituir.

**Entregável esperado:**

- Explicação escrita do porquê do bug (causa raiz, não só "o comando estava errado").
- Código corrigido usando pelo menos uma das técnicas acima.
- Uma frase de checklist que você adicionaria numa revisão de PR para pegar esse tipo de erro antes de ir pra produção.

---

## Observação geral

Todos os exercícios assumem o padrão medallion (Bronze → Silver → Gold) que o sistema de educação usa. Os exercícios 3, 6, 7 e 8 já apontam explicitamente para o consumo via MCP/LLM e para governança, que parecem ser diferenciais da vaga. O exercício 9 cobre a leitura alternativa da pegadinha que o Nielk comentou (overwrite mode), caso não seja a de window function coberta em `treino_teste`. Vale treinar explicando em voz alta *por que* cada decisão de modelagem facilita ou dificulta consultas em linguagem natural, e *por que* uma decisão de governança protege ou não protege dado sensível — isso pode ser um ponto de discussão na entrevista, não só no teste técnico.

Manda sua solução de qualquer um deles que eu reviso.
