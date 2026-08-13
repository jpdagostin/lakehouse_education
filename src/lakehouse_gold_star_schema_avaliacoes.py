"""
Pipeline Silver -> Gold: Star Schema de Avaliações.

Contexto de negócio
--------------------
O sistema de educação quer uma camada Gold estruturada como star schema
para relatórios de desempenho escolar (alunos, escolas, disciplinas,
avaliações), permitindo cruzar desempenho ao longo do tempo.

Entrada
-------
- `silver_avaliacoes.csv`: fato bruto, uma linha por avaliação/aluno.
- `silver_escolas.csv` / `silver_disciplinas.csv`: atributos descritivos
  de escola e disciplina.
- `silver_matriculas` (Delta, já existente de outro pipeline): única fonte
  com atributos de aluno (nome, turma, status) disponível no projeto hoje;
  não existe uma tabela `silver_alunos` dedicada.

Saída / efeito colateral
-------------------------
Tabelas Delta em `<gold_path>/{fato_avaliacoes,dim_aluno,dim_escola,
dim_disciplina,dim_tempo}`:

- `fato_avaliacoes`: uma linha por avaliação, com chaves substitutas (sk)
  para cada dimensão.
- `dim_aluno` / `dim_disciplina`: dimensões de estado atual (upsert por
  chave natural), sem histórico — o enunciado só exige tratamento de SCD
  para `dim_escola`.
- `dim_escola`: SCD tipo 2 (mantém histórico de mudanças de atributo, ex.:
  mudança de `rede`), com `data_inicio_validade`, `data_fim_validade` e
  `flag_atual`.
- `dim_tempo`: calendário completo (todos os dias) do ano de referência,
  gerado, não derivado dos dados de fato.

As chaves substitutas de `dim_aluno`/`dim_disciplina` são hashes
determinísticos (`sha2`) da chave natural, para que reprocessar o mesmo
lote não gere ids diferentes nem duplique linhas — mesmo racional de
idempotência já usado nos outros pipelines do projeto. `dim_escola`
(SCD2) é a exceção: sua chave substituta é gerada a partir de um timestamp
de escrita (não precisa ser reproduzível entre execuções, já que uma nova
versão só é criada quando o atributo realmente muda — a idempotência vem
dessa checagem, não do hash em si).
"""

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from lakehouse_bronze_matriculas import create_spark_session

DEFAULT_PATH = "/Users/jpdagostin/Desktop/personal/lakehouseEducation/lakehouse_education/data/"

SILVER_ESCOLAS_SCHEMA = StructType(
    [
        StructField("escola_id", StringType(), False),
        StructField("nome_escola", StringType(), True),
        StructField("cidade", StringType(), True),
        StructField("rede", StringType(), True),
    ]
)

SILVER_DISCIPLINAS_SCHEMA = StructType(
    [
        StructField("disciplina_id", StringType(), False),
        StructField("nome_disciplina", StringType(), True),
        StructField("area_conhecimento", StringType(), True),
    ]
)

SILVER_AVALIACOES_SCHEMA = StructType(
    [
        StructField("avaliacao_id", StringType(), False),
        StructField("aluno_id", StringType(), False),
        StructField("escola_id", StringType(), False),
        StructField("disciplina_id", StringType(), False),
        StructField("data_avaliacao", DateType(), True),
        StructField("nota", DoubleType(), True),
        StructField("nota_maxima", DoubleType(), True),
        StructField("tipo_avaliacao", StringType(), True),
    ]
)

# Atributos de dim_escola que, ao mudar, disparam uma nova versão SCD2
# (ex.: escola muda de rede pública para privada).
DIM_ESCOLA_ATTR_COLS = ["nome_escola", "cidade", "rede"]

MESES_PT = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]  # fmt: skip

# dayofweek do Spark retorna 1=domingo..7=sábado; a ordem aqui segue essa
# convenção para indexação direta via element_at.
DIAS_SEMANA_PT = [
    "Domingo", "Segunda-feira", "Terça-feira", "Quarta-feira",
    "Quinta-feira", "Sexta-feira", "Sábado",
]  # fmt: skip


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------


def read_silver_escolas(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(SILVER_ESCOLAS_SCHEMA).csv(path)


def read_silver_disciplinas(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(SILVER_DISCIPLINAS_SCHEMA).csv(path)


def read_silver_avaliacoes(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).schema(SILVER_AVALIACOES_SCHEMA).csv(path)


def read_silver_matriculas(spark: SparkSession, path: str) -> DataFrame:
    # silver_matriculas já é Delta (gravada pelo pipeline de matrículas) e já
    # tem um único registro por aluno_id, então não há dedup a fazer aqui.
    return spark.read.format("delta").load(path)


# ---------------------------------------------------------------------------
# Dimensões de estado atual (sem histórico): dim_aluno, dim_disciplina
# ---------------------------------------------------------------------------


def build_dim_aluno(matriculas_df: DataFrame) -> DataFrame:
    # Sem uma silver_alunos dedicada, o estado do aluno vem da Silver de
    # matrículas (que já reflete o registro mais recente e correto por
    # aluno). O enunciado do Exercício 4 não pede histórico para dim_aluno,
    # então tratamos como dimensão de estado atual (upsert por aluno_id).
    return matriculas_df.select("aluno_id", "nome", "turma", "status").withColumn(
        "aluno_sk", F.sha2(F.col("aluno_id"), 256)
    )


def build_dim_disciplina(disciplinas_df: DataFrame) -> DataFrame:
    return disciplinas_df.withColumn("disciplina_sk", F.sha2(F.col("disciplina_id"), 256))


def _upsert_by_key(spark: SparkSession, df: DataFrame, path: str, key_cols: list[str]) -> None:
    # Mesmo padrão de MERGE idempotente já usado em write_silver_matriculas
    # e write_gold_engajamento_turma, parametrizado pela chave de merge.
    if DeltaTable.isDeltaTable(spark, path):
        table = DeltaTable.forPath(spark, path)
        condition = " AND ".join(f"t.{c} = b.{c}" for c in key_cols)
        (
            table.alias("t")
            .merge(df.alias("b"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)


def write_dim_aluno(spark: SparkSession, dim_aluno_df: DataFrame, path: str) -> None:
    _upsert_by_key(spark, dim_aluno_df, path, ["aluno_id"])


def write_dim_disciplina(spark: SparkSession, dim_disciplina_df: DataFrame, path: str) -> None:
    _upsert_by_key(spark, dim_disciplina_df, path, ["disciplina_id"])


# ---------------------------------------------------------------------------
# dim_escola: SCD tipo 2
# ---------------------------------------------------------------------------


def build_dim_escola_incoming(escolas_df: DataFrame) -> DataFrame:
    # attributes_hash resume os atributos versionáveis numa única coluna:
    # comparar esse hash contra o da versão ativa é o que decide se uma nova
    # versão SCD2 precisa ser criada, sem comparar coluna a coluna.
    return escolas_df.withColumn(
        "attributes_hash", F.sha2(F.concat_ws("||", *DIM_ESCOLA_ATTR_COLS), 256)
    )


# Sentinela de "início dos tempos" para a primeira versão de cada escola: na
# primeira carga não sabemos quando o atributo passou a valer de fato, então
# a versão inicial precisa cobrir qualquer data de negócio anterior à
# primeira execução do pipeline (ex.: avaliações de silver_avaliacoes já
# datadas de janeiro/2026, processadas só quando o pipeline rodar). Usar
# current_date() aqui deixaria avaliações antigas sem nenhuma versão de
# escola válida para casar no join da fato.
MIN_DATA_INICIO_VALIDADE = date(1900, 1, 1)


def _com_escola_sk(df: DataFrame) -> DataFrame:
    # A chave substituta usa current_timestamp() (não current_date()) porque
    # o objetivo aqui é só garantir uma chave única por versão gerada nesta
    # escrita — se duas versões novas fossem criadas no mesmo dia (ex.:
    # pipeline reprocessado após corrigir um erro), current_date() geraria a
    # mesma chave para as duas. Não precisamos que o hash seja reproduzível
    # entre execuções, porque a idempotência já é garantida antes disso: uma
    # nova versão só é gerada quando attributes_hash realmente muda.
    versao_ts = F.current_timestamp().cast("string")
    return df.withColumn("escola_sk", F.sha2(F.concat_ws("||", F.col("escola_id"), versao_ts), 256))


def write_dim_escola_scd2(spark: SparkSession, incoming_df: DataFrame, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        primeira_carga = (
            _com_escola_sk(incoming_df)
            .withColumn("data_inicio_validade", F.lit(MIN_DATA_INICIO_VALIDADE))
            .withColumn("data_fim_validade", F.lit(None).cast(DateType()))
            .withColumn("flag_atual", F.lit(True))
        )
        primeira_carga.write.format("delta").mode("overwrite").save(path)
        return

    dim_table = DeltaTable.forPath(spark, path)
    versao_ativa = (
        dim_table.toDF()
        .filter(F.col("flag_atual"))
        .select("escola_id", F.col("attributes_hash").alias("attributes_hash_atual"))
    )

    # Escolas novas (sem versão ativa prévia) ou com attributes_hash
    # diferente do atual precisam de uma nova versão. Escolas sem mudança
    # ficam de fora e não geram escrita nenhuma.
    versoes_novas = (
        incoming_df.join(versao_ativa, "escola_id", "left")
        .where(
            F.col("attributes_hash_atual").isNull()
            | (F.col("attributes_hash") != F.col("attributes_hash_atual"))
        )
        .drop("attributes_hash_atual")
    )

    if versoes_novas.isEmpty():
        # Nada mudou desde a última carga: não expira nem insere nada, para
        # manter a operação idempotente em reexecuções do mesmo lote.
        return

    # Expira a versão ativa das escolas que tiveram algum atributo alterado
    # (escolas totalmente novas não têm versão ativa prévia para casar, então
    # esse merge simplesmente não afeta linha nenhuma para elas).
    (
        dim_table.alias("d")
        .merge(
            versoes_novas.select("escola_id").alias("b"),
            "d.escola_id = b.escola_id AND d.flag_atual = true",
        )
        .whenMatchedUpdate(set={"flag_atual": F.lit(False), "data_fim_validade": F.current_date()})
        .execute()
    )

    novas_com_validade = (
        _com_escola_sk(versoes_novas)
        .withColumn("data_inicio_validade", F.current_date())
        .withColumn("data_fim_validade", F.lit(None).cast(DateType()))
        .withColumn("flag_atual", F.lit(True))
    )
    novas_com_validade.write.format("delta").mode("append").save(path)


# ---------------------------------------------------------------------------
# dim_tempo: calendário completo (gerado, não derivado do fato)
# ---------------------------------------------------------------------------


def build_dim_tempo(spark: SparkSession, ano: int) -> DataFrame:
    calendario = spark.range(1).select(
        F.explode(F.sequence(F.lit(date(ano, 1, 1)), F.lit(date(ano, 12, 31)))).alias("data")
    )
    return (
        calendario.withColumn("data_sk", F.date_format("data", "yyyyMMdd").cast("int"))
        .withColumn("ano", F.year("data"))
        .withColumn("mes", F.month("data"))
        .withColumn("dia", F.dayofmonth("data"))
        # Bimestre (1 a 6) usado pelos exercícios anteriores para agrupar
        # notas/frequência; trimestre incluído como granularidade alternativa
        # comum em relatórios escolares.
        .withColumn("bimestre", F.ceil(F.month("data") / 2).cast("int"))
        .withColumn("trimestre", F.quarter("data"))
        .withColumn(
            "nome_mes", F.element_at(F.array(*[F.lit(m) for m in MESES_PT]), F.month("data"))
        )
        .withColumn(
            "dia_semana",
            F.element_at(F.array(*[F.lit(d) for d in DIAS_SEMANA_PT]), F.dayofweek("data")),
        )
    )


def write_dim_tempo(dim_tempo_df: DataFrame, path: str) -> None:
    # dim_tempo é inteiramente derivada (calendário gerado), não incremental:
    # regenerar e sobrescrever a cada execução já é idempotente por
    # definição, sem necessidade de merge.
    dim_tempo_df.write.format("delta").mode("overwrite").save(path)


# ---------------------------------------------------------------------------
# fato_avaliacoes
# ---------------------------------------------------------------------------


def build_fato_avaliacoes(
    avaliacoes_df: DataFrame,
    dim_aluno_df: DataFrame,
    dim_escola_df: DataFrame,
    dim_disciplina_df: DataFrame,
) -> DataFrame:
    aluno_dim = dim_aluno_df.select("aluno_id", "aluno_sk")
    disciplina_dim = dim_disciplina_df.select("disciplina_id", "disciplina_sk")
    # escola_id renomeada para evitar ambiguidade no join por intervalo de
    # datas abaixo (join por condição, não por nome de coluna).
    escola_dim = dim_escola_df.select(
        F.col("escola_id").alias("escola_id_dim"),
        "escola_sk",
        "data_inicio_validade",
        "data_fim_validade",
    )

    fato = (
        avaliacoes_df.join(aluno_dim, "aluno_id", "left")
        .join(disciplina_dim, "disciplina_id", "left")
        # Join por versão vigente na data da avaliação (não pela versão mais
        # recente): uma avaliação antiga deve casar com o atributo de escola
        # que era verdade na época dela, não com o estado atual da escola.
        .join(
            escola_dim,
            (F.col("escola_id") == F.col("escola_id_dim"))
            & (F.col("data_avaliacao") >= F.col("data_inicio_validade"))
            & (
                F.col("data_fim_validade").isNull()
                | (F.col("data_avaliacao") < F.col("data_fim_validade"))
            ),
            "left",
        )
    )

    return fato.select(
        "avaliacao_id",
        "aluno_sk",
        "escola_sk",
        "disciplina_sk",
        "data_avaliacao",
        "nota",
        "nota_maxima",
        F.round(F.col("nota") / F.col("nota_maxima") * 100, 2).alias("percentual_nota"),
        "tipo_avaliacao",
    ).withColumn("dt_processamento_gold", F.current_timestamp())


def write_fato_avaliacoes(spark: SparkSession, fato_df: DataFrame, path: str) -> None:
    _upsert_by_key(spark, fato_df, path, ["avaliacao_id"])


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------


def run_gold_star_schema_avaliacoes(
    spark: SparkSession,
    avaliacoes_path: str,
    escolas_path: str,
    disciplinas_path: str,
    matriculas_path: str,
    fato_path: str,
    dim_aluno_path: str,
    dim_escola_path: str,
    dim_disciplina_path: str,
    dim_tempo_path: str,
    ano_calendario: int = 2026,
) -> DataFrame:
    avaliacoes_df = read_silver_avaliacoes(spark, avaliacoes_path)
    escolas_df = read_silver_escolas(spark, escolas_path)
    disciplinas_df = read_silver_disciplinas(spark, disciplinas_path)
    matriculas_df = read_silver_matriculas(spark, matriculas_path)

    write_dim_tempo(build_dim_tempo(spark, ano_calendario), dim_tempo_path)
    write_dim_aluno(spark, build_dim_aluno(matriculas_df), dim_aluno_path)
    write_dim_disciplina(spark, build_dim_disciplina(disciplinas_df), dim_disciplina_path)
    write_dim_escola_scd2(spark, build_dim_escola_incoming(escolas_df), dim_escola_path)

    # Relidas do disco (não reaproveitadas em memória) porque dim_escola só
    # ganha seu escola_sk/data_inicio_validade definitivos após o merge
    # SCD2 acima; dim_aluno/dim_disciplina poderiam ser reaproveitadas (sk é
    # função pura da chave natural), mas relemos por consistência com o que
    # está de fato persistido.
    dim_aluno_df = spark.read.format("delta").load(dim_aluno_path)
    dim_disciplina_df = spark.read.format("delta").load(dim_disciplina_path)
    dim_escola_df = spark.read.format("delta").load(dim_escola_path)

    fato_df = build_fato_avaliacoes(avaliacoes_df, dim_aluno_df, dim_escola_df, dim_disciplina_df)
    write_fato_avaliacoes(spark, fato_df, fato_path)
    return fato_df


if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_to_gold_star_schema_avaliacoes")
    run_gold_star_schema_avaliacoes(
        spark,
        avaliacoes_path=f"{DEFAULT_PATH}/silver/silver_avaliacoes/silver_avaliacoes.csv",
        escolas_path=f"{DEFAULT_PATH}/silver/silver_escolas/silver_escolas.csv",
        disciplinas_path=f"{DEFAULT_PATH}/silver/silver_disciplinas/silver_disciplinas.csv",
        matriculas_path=f"{DEFAULT_PATH}/silver/silver_matriculas",
        fato_path=f"{DEFAULT_PATH}/gold/fato_avaliacoes",
        dim_aluno_path=f"{DEFAULT_PATH}/gold/dim_aluno",
        dim_escola_path=f"{DEFAULT_PATH}/gold/dim_escola",
        dim_disciplina_path=f"{DEFAULT_PATH}/gold/dim_disciplina",
        dim_tempo_path=f"{DEFAULT_PATH}/gold/dim_tempo",
    )
