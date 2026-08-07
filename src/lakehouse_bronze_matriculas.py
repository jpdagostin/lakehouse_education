"""
Pipeline Bronze -> Silver: Matrículas de Alunos.

Contexto de negócio
--------------------
A camada Bronze recebe eventos de matrícula vindos de múltiplas fontes de
ingestão (ex.: CDC de banco transacional, cargas batch manuais, etc.), o que
gera duplicidade, atrasos de propagação e campos nulos temporários (ex.: um
evento de CDC pode chegar antes de um enriquecimento síncrono terminar de
popular "turma"). Este script consolida esses eventos brutos em uma visão
"Silver" com um único registro por aluno, refletindo o estado mais recente
e mais confiável conhecido.

Entrada
-------
CSV em "/caminho/bronze_matriculas.csv" com uma linha por evento de
matrícula (múltiplos eventos por aluno_id são esperados e tratados aqui).

Saída / efeito colateral
-------------------------
Tabela Delta em "/caminho/silver_matriculas", com 1 linha por aluno_id.
Na primeira execução (tabela ainda não existe) grava via overwrite; nas
execuções seguintes, faz MERGE (upsert) para manter a operação idempotente
e incremental — reprocessar o mesmo lote não duplica nem corrompe dados.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

PATH = "/Users/jpdagostin/Desktop/personal/lakehouse_education/data/"

builder = (
    SparkSession.builder
    .appName("bronze_to_silver_matriculas")
    # Extensões necessárias para habilitar operações Delta (MERGE, time travel,
    # etc.) neste SparkSession; sem elas, DeltaTable.forPath/merge falhariam.
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
# delta-spark instalado via pip não traz os JARs do Delta embutidos;
# configure_spark_with_delta_pip adiciona spark.jars.packages para que o
# Spark baixe (e cacheie via Ivy) os JARs do Maven na criação da sessão.
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Schema aplicado explicitamente (em vez de inferido) para garantir leitura
# determinística e resiliente a variações no CSV de origem. Todos os campos,
# exceto as chaves obrigatórias (evento_id, aluno_id), são nullable=True
# porque fontes diferentes podem não preencher todos os atributos no mesmo
# evento (ver forward-fill no passo 3 abaixo).
schema = StructType([
    StructField("evento_id", StringType(), False),
    StructField("aluno_id", StringType(), False),
    StructField("nome", StringType(), True),
    StructField("turma", StringType(), True),
    StructField("status", StringType(), True),
    StructField("data_evento", StringType(), True),
    StructField("fonte_ingestao", StringType(), True),
])

bronze_df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(f"{PATH}/bronze/bronze_matriculas.csv")
    # data_evento chega como string no CSV; convertida para timestamp aqui
    # pois todo o pipeline depende de ordenação cronológica correta
    # (forward-fill e escolha do registro mais recente).
    .withColumn("data_evento", F.to_timestamp("data_evento"))
)

# 1. Remove duplicatas exatas (ev001/ev002), ignorando evento_id
#    Justificativa: o mesmo evento de negócio pode ser reenviado por engano
#    (retry de ingestão, replay de CDC) com evento_id diferente mas payload
#    idêntico. Ignorar evento_id no dedup evita contar esse reenvio como um
#    novo estado do aluno.
dedup_cols = ["aluno_id", "nome", "turma", "status", "data_evento", "fonte_ingestao"]
bronze_dedup = bronze_df.dropDuplicates(dedup_cols)

# 2. Prioridade de fonte para desempate
#    Justificativa de negócio: quando dois eventos têm o mesmo timestamp,
#    a fonte "cdc" é considerada mais confiável/autoritativa que outras
#    fontes de ingestão (ex.: cargas manuais), então recebe prioridade 1
#    (mais prioritária) contra 2 para as demais. Esse valor é usado tanto
#    no forward-fill (passo 3) quanto na seleção do registro final (passo 4).
bronze_dedup = bronze_dedup.withColumn(
    "fonte_prioridade",
    F.when(F.col("fonte_ingestao") == "cdc", F.lit(1)).otherwise(F.lit(2))
)

# 3. Forward-fill de campos nulos: pega o último valor não-nulo visto até aquele
#    ponto, por aluno, em ordem cronológica
#    Justificativa: eventos parciais são normais nesta origem (ex.: um evento
#    de mudança de status pode não reenviar "turma"). Em vez de tratar isso
#    como dado ausente, herdamos o último valor conhecido e válido daquele
#    aluno, para que o estado "corrente" de cada evento fique sempre completo
#    antes de decidirmos qual é o registro mais recente no passo 4.
w_asc = (
    Window.partitionBy("aluno_id")
    .orderBy("data_evento", "fonte_prioridade")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

healed_df = bronze_dedup
for c in ["nome", "turma", "status"]:
    healed_df = healed_df.withColumn(c, F.last(c, ignorenulls=True).over(w_asc))

# 4. Seleciona o registro mais recente por aluno
#    Critério de desempate em cascata: data_evento mais recente vence;
#    empatando, fonte mais prioritária (cdc) vence; ainda empatando,
#    evento_id maior (assumido como o mais recente/alto na sequência de
#    geração) desempata de forma determinística, evitando resultados
#    não-determinísticos em reprocessamentos.
w_desc = Window.partitionBy("aluno_id").orderBy(
    F.col("data_evento").desc(),
    F.col("fonte_prioridade").asc(),
    F.col("evento_id").desc(),
)

silver_df = (
    healed_df
    .withColumn("rn", F.row_number().over(w_desc))
    .filter(F.col("rn") == 1)
    # "rn" era só auxiliar de seleção; "fonte_prioridade" era só auxiliar de
    # desempate — nenhum dos dois é um atributo de negócio da Silver.
    .drop("rn", "fonte_prioridade")
    # Renomeado para deixar explícito, na Silver, que este é o identificador
    # do evento que originou o estado atual do aluno (rastreabilidade),
    # e não "o" evento_id genérico.
    .withColumnRenamed("evento_id", "ultimo_evento_id")
    .withColumn("dt_processamento_silver", F.current_timestamp())
)

# 5. Grava na Silver via MERGE (idempotente, upsert incremental)
#    Justificativa: como o pipeline pode ser reexecutado sobre o mesmo lote
#    (reprocessamento, backfill), um MERGE por aluno_id garante que cada
#    execução apenas atualiza o estado do aluno para o resultado final
#    calculado acima, sem duplicar linhas — ao contrário de um append puro.
silver_path = f"{PATH}/silver/silver_matriculas"

if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(spark, silver_path)
    (
        silver_table.alias("s")
        .merge(silver_df.alias("b"), "s.aluno_id = b.aluno_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # Primeira execução: tabela Silver ainda não existe, então criamos via
    # overwrite em vez de merge (não há nada para casar).
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
