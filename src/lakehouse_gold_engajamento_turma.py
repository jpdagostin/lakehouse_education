"""
Pipeline Silver -> Gold: Engajamento por Turma.

Contexto de negócio
--------------------
A camada Silver tem uma tabela de submissões de exercícios por aluno, já
limpa e deduplicada (uma linha por submissão, sem trabalho de conciliação
de fonte/duplicata a fazer aqui — diferente do pipeline de matrículas).
O time de produto quer métricas de engajamento por escola e turma para um
dashboard executivo, e também para alimentar consultas em linguagem
natural via um agente LLM/MCP (ex.: "qual turma teve a maior taxa de
acerto em fevereiro?").

Entrada
-------
CSV em "<silver_path>" com uma linha por submissão de exercício
(submissao_id, aluno_id, escola_id, turma_id, disciplina, data_submissao,
correta, tempo_gasto_segundos).

Saída / efeito colateral
-------------------------
Tabela Delta em "<gold_path>", com uma linha por
(escola_id, turma_id, disciplina, mes_referencia). Na primeira execução
(tabela ainda não existe) grava via overwrite; nas execuções seguintes,
faz MERGE (upsert) por essa chave de granularidade, mantendo a operação
idempotente: reprocessar o mesmo lote não duplica nem corrompe dados.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from lakehouse_bronze_matriculas import create_spark_session

DEFAULT_PATH = "/Users/jpdagostin/Desktop/personal/lakehouse_education/data/"

SILVER_SUBMISSOES_SCHEMA = StructType(
    [
        StructField("submissao_id", StringType(), False),
        StructField("aluno_id", StringType(), False),
        StructField("escola_id", StringType(), False),
        StructField("turma_id", StringType(), False),
        StructField("disciplina", StringType(), False),
        StructField("data_submissao", DateType(), True),
        StructField("correta", BooleanType(), True),
        StructField("tempo_gasto_segundos", IntegerType(), True),
    ]
)

# Chave de granularidade da Gold: uma linha por escola/turma/disciplina/mês.
# Justificativa: taxa de acerto e tempo médio variam por disciplina dentro da
# mesma turma (ex.: a turma "7A" tem submissões de matemática e português
# misturadas no dataset de exemplo); agregar sem "disciplina" juntaria
# assuntos diferentes e produziria uma taxa de acerto sem sentido de
# negócio — o mesmo tipo de armadilha de granularidade tratada no
# Exercício 8 do material de estudo.
GOLD_GRANULARITY_COLS = ["escola_id", "turma_id", "disciplina", "mes_referencia"]


def read_silver_submissoes(spark: SparkSession, silver_path: str) -> DataFrame:
    # Schema explícito (em vez de inferSchema): no CSV, "correta" chega como
    # string "true"/"false" e "tempo_gasto_segundos" como texto. Sem forçar
    # os tipos aqui, avg()/soma sobre essas colunas quebrariam ou dariam
    # resultado errado (ex.: contagem de string em vez de agregação
    # booleana). Mesmo racional do BRONZE_MATRICULAS_SCHEMA no pipeline de
    # matrículas: leitura determinística, resiliente a inferência ambígua.
    return spark.read.option("header", True).schema(SILVER_SUBMISSOES_SCHEMA).csv(silver_path)


def transform_silver_to_gold(silver_df: DataFrame) -> DataFrame:
    # mes_referencia: trunca data_submissao para o 1º dia do mês. Mantido
    # como tipo "date" (não string livre) para permanecer ordenável e
    # filtrável por intervalo (ex.: "entre janeiro e fevereiro"); uma tool
    # MCP que traduza perguntas em linguagem natural ("em fevereiro") para
    # um filtro de data faz esse mapeamento a partir desse tipo, sem
    # ambiguidade de formato.
    df = silver_df.withColumn("mes_referencia", F.trunc("data_submissao", "month"))

    return (
        df.groupBy(*GOLD_GRANULARITY_COLS)
        .agg(
            F.count("submissao_id").alias("total_submissoes"),
            F.sum(F.col("correta").cast("int")).alias("total_acertos"),
            (F.avg(F.col("correta").cast("int")) * 100).alias("taxa_acerto_pct"),
            F.avg("tempo_gasto_segundos").alias("tempo_medio_segundos"),
            F.countDistinct("aluno_id").alias("alunos_distintos"),
        )
        .withColumn("dt_processamento_gold", F.current_timestamp())
    )


def write_gold_engajamento_turma(spark: SparkSession, gold_df: DataFrame, gold_path: str) -> None:
    # Grava na Gold via MERGE (idempotente, upsert incremental) pela chave
    # de granularidade (escola/turma/disciplina/mês).
    # Justificativa: como o pipeline pode ser reexecutado sobre o mesmo lote
    # (reprocessamento), um MERGE por essa chave garante que cada execução
    # apenas atualiza as métricas do grupo para o resultado recalculado
    # acima, sem duplicar linhas — ao contrário de um append puro, e sem o
    # risco de um overwrite total apagar meses já consolidados que não
    # fazem parte do lote reprocessado (a pegadinha do Exercício 9).
    if DeltaTable.isDeltaTable(spark, gold_path):
        gold_table = DeltaTable.forPath(spark, gold_path)
        merge_condition = " AND ".join(f"g.{c} = b.{c}" for c in GOLD_GRANULARITY_COLS)
        (
            gold_table.alias("g")
            .merge(gold_df.alias("b"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Primeira execução: tabela Gold ainda não existe, então criamos
        # via overwrite em vez de merge (não há nada para casar).
        gold_df.write.format("delta").mode("overwrite").save(gold_path)


def run_silver_to_gold_engajamento_turma(
    spark: SparkSession, silver_path: str, gold_path: str
) -> DataFrame:
    silver_df = read_silver_submissoes(spark, silver_path)
    gold_df = transform_silver_to_gold(silver_df)
    write_gold_engajamento_turma(spark, gold_df, gold_path)
    return gold_df


if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_to_gold_engajamento_turma")
    run_silver_to_gold_engajamento_turma(
        spark,
        silver_path=f"{DEFAULT_PATH}/silver/silver_submissoes/silver_submissoes.csv",
        gold_path=f"{DEFAULT_PATH}/gold/gold_engajamento_turma",
    )
