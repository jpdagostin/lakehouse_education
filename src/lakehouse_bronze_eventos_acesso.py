"""
Pipeline Raw -> Bronze: Eventos de Acesso à Plataforma.

Contexto de negócio
--------------------
A plataforma de ensino emite eventos de acesso (login, acesso a videoaula,
submissão de exercício) em lotes diários. O time de produto altera o
schema desses eventos sem avisar o time de dados: no dia 2 aparece um
campo novo ("dispositivo"); no dia 3 aparece um campo aninhado
("metadata") e o lote passa a chegar em JSON em vez de CSV. A ingestão
precisa continuar funcionando sem quebrar quando lotes futuros trouxerem
colunas adicionais (ou até formatos diferentes), e sem truncar ou perder
os lotes já consolidados.

Entrada
-------
Um ou mais arquivos crus em "data/raw/", cada um em qualquer formato
suportado nativamente pelo Spark (CSV, JSON, Parquet, ORC, Delta, etc.),
cada um representando um lote (ex.: um dia) de eventos. O schema de cada
lote pode variar (colunas novas, campos aninhados novos).

Saída / efeito colateral
-------------------------
Tabela Delta em "<bronze_path>", acumulando todos os lotes via append com
evolução automática de schema ("mergeSchema"). Registros de lotes antigos
que não tinham uma coluna nova passam a tê-la como null; nenhum dado é
sobrescrito ou truncado entre execuções de lotes diferentes.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse_bronze_matriculas import create_spark_session

DEFAULT_PATH = "/Users/jpdagostin/Desktop/personal/lakehouseEducation/lakehouse_education/data/"


def read_raw_eventos_batch(
    spark: SparkSession, path: str, formato: str, opcoes: dict | None = None
) -> DataFrame:
    # Leitura genérica por formato: o schema não é fixado aqui de propósito
    # (diferente de BRONZE_MATRICULAS_SCHEMA no pipeline de matrículas),
    # porque a evolução de schema é o próprio ponto deste pipeline. Cada
    # lote é lido com o schema que ele mesmo tem (inferido); quem reconcilia
    # isso entre lotes é a escrita Delta (mergeSchema), não o leitor. Um
    # lote futuro em outro formato (Parquet, ORC, Delta) já é suportado sem
    # mudar código, só trocando o parâmetro "formato"; formatos que exigem
    # opções extras (delimitador, multiline, etc.) usam "opcoes".
    reader = spark.read.format(formato)
    for chave, valor in (opcoes or {}).items():
        reader = reader.option(chave, valor)
    return reader.load(path)


def normalize_eventos_schema(df: DataFrame) -> DataFrame:
    # aluno_id chega como string no CSV (sem inferSchema, para leitura
    # determinística) e como número (long) no JSON, já que o leitor JSON
    # infere tipos a partir do valor literal. Sem essa normalização, o
    # mergeSchema do Delta falharia ao tentar conciliar tipos incompatíveis
    # (string vs long) para a mesma coluna entre lotes. Tratamos aluno_id
    # como identificador (string), não como número, seguindo o mesmo padrão
    # já usado no pipeline de matrículas (aluno_id como StringType).
    df = df.withColumn("aluno_id", F.col("aluno_id").cast("string"))
    # timestamp chega como string em todos os lotes; convertido aqui, na
    # leitura da Bronze, para timestamp real, seguindo o mesmo racional já
    # aplicado a data_evento no pipeline de matrículas: consultas e
    # ordenação cronológica dependem de um tipo de data real, não de string.
    df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
    return df


def write_bronze_eventos_acesso(df: DataFrame, bronze_path: str) -> None:
    # append + mergeSchema=true: cada lote é somado ao histórico acumulado,
    # nunca sobrescrevendo os lotes anteriores. Colunas novas trazidas por um
    # lote (ex.: "dispositivo" no dia 2, "metadata" no dia 3) são absorvidas
    # no schema da tabela; os registros já gravados de lotes anteriores
    # passam a ter essas colunas como null, sem que seus dados originais
    # sejam reescritos ou perdidos.
    (df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path))


def run_raw_to_bronze_eventos_acesso(
    spark: SparkSession, lotes: list[tuple[str, str, dict | None]], bronze_path: str
) -> None:
    # Processa os lotes em sequência, na ordem cronológica de chegada. Um
    # lote novo no futuro (formato diferente, colunas novas) exige apenas um
    # item a mais nesta lista, não uma função nova.
    for path, formato, opcoes in lotes:
        df = read_raw_eventos_batch(spark, path, formato, opcoes)
        df = normalize_eventos_schema(df)
        write_bronze_eventos_acesso(df, bronze_path)


def read_bronze_eventos_com_app_version(spark: SparkSession, bronze_path: str) -> DataFrame:
    # Consulta de exemplo tolerante à ausência de "metadata": acessar um
    # campo de struct em uma linha onde a coluna inteira é null (registros
    # dos dias 1 e 2, anteriores à existência de "metadata") retorna null
    # naturalmente no Spark, sem lançar erro. Não é necessário nenhum
    # tratamento condicional extra para os lotes antigos.
    df = spark.read.format("delta").load(bronze_path)
    return df.select(
        "evento_id",
        "aluno_id",
        "tipo_evento",
        "timestamp",
        "dispositivo",
        F.col("metadata.app_version").alias("app_version"),
    )


if __name__ == "__main__":
    spark = create_spark_session(app_name="raw_to_bronze_eventos_acesso")
    lotes = [
        (f"{DEFAULT_PATH}/raw/raw_eventos_dia_1.csv", "csv", {"header": True}),
        (f"{DEFAULT_PATH}/raw/raw_eventos_dia_2.csv", "csv", {"header": True}),
        (f"{DEFAULT_PATH}/raw/raw_eventos_dia_3.json", "json", {"multiLine": True}),
    ]
    run_raw_to_bronze_eventos_acesso(
        spark, lotes, bronze_path=f"{DEFAULT_PATH}/bronze/bronze_eventos_acesso"
    )
