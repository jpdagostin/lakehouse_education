from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse_gold_star_schema_avaliacoes import (
    build_dim_aluno,
    build_dim_disciplina,
    build_dim_escola_incoming,
    build_dim_tempo,
    build_fato_avaliacoes,
    read_silver_avaliacoes,
    read_silver_disciplinas,
    read_silver_escolas,
    run_gold_star_schema_avaliacoes,
    write_dim_escola_scd2,
)

ESCOLAS_HEADER = "escola_id,nome_escola,cidade,rede"
DISCIPLINAS_HEADER = "disciplina_id,nome_disciplina,area_conhecimento"
AVALIACOES_HEADER = (
    "avaliacao_id,aluno_id,escola_id,disciplina_id,data_avaliacao,nota,nota_maxima,tipo_avaliacao"
)


def _write_csv(tmp_path, name: str, header: str, rows: list[str]) -> str:
    csv_path = tmp_path / name
    csv_path.write_text("\n".join([header, *rows]) + "\n")
    return str(csv_path)


def test_build_dim_aluno_reflects_matriculas_state(spark, tmp_path):
    matriculas_path = str(tmp_path / "silver_matriculas")
    matriculas_df = spark.createDataFrame(
        [("2001", "Maria Silva", "7A", "ativo")],
        ["aluno_id", "nome", "turma", "status"],
    )
    matriculas_df.write.format("delta").save(matriculas_path)

    dim_aluno_df = build_dim_aluno(spark.read.format("delta").load(matriculas_path))
    result = dim_aluno_df.collect()[0].asDict()

    assert result["nome"] == "Maria Silva"
    assert result["turma"] == "7A"
    # aluno_sk precisa ser determinístico: mesma entrada, mesmo hash sempre,
    # senão reprocessamentos quebrariam a referência da fato.
    assert result["aluno_sk"] == matriculas_df.select(F.sha2("aluno_id", 256)).collect()[0][0]


def test_build_dim_disciplina_generates_deterministic_sk(spark, tmp_path):
    path = _write_csv(
        tmp_path,
        "silver_disciplinas.csv",
        DISCIPLINAS_HEADER,
        ["disc_mat,Matemática,exatas"],
    )
    df1 = build_dim_disciplina(read_silver_disciplinas(spark, path))
    df2 = build_dim_disciplina(read_silver_disciplinas(spark, path))
    assert df1.collect()[0]["disciplina_sk"] == df2.collect()[0]["disciplina_sk"]


def test_dim_escola_scd2_creates_active_row_on_first_run(spark, tmp_path):
    dim_escola_path = str(tmp_path / "dim_escola")
    escolas_path = _write_csv(
        tmp_path, "silver_escolas.csv", ESCOLAS_HEADER, ["esc02,Escola Beta,Belo Horizonte,publica"]
    )
    incoming_df = build_dim_escola_incoming(read_silver_escolas(spark, escolas_path))
    write_dim_escola_scd2(spark, incoming_df, dim_escola_path)

    result = spark.read.format("delta").load(dim_escola_path).collect()
    assert len(result) == 1
    assert result[0]["flag_atual"] is True
    assert result[0]["data_fim_validade"] is None


def test_dim_escola_scd2_is_idempotent_when_nothing_changes(spark, tmp_path):
    dim_escola_path = str(tmp_path / "dim_escola")
    escolas_path = _write_csv(
        tmp_path, "silver_escolas.csv", ESCOLAS_HEADER, ["esc02,Escola Beta,Belo Horizonte,publica"]
    )
    incoming_df = build_dim_escola_incoming(read_silver_escolas(spark, escolas_path))
    write_dim_escola_scd2(spark, incoming_df, dim_escola_path)
    write_dim_escola_scd2(spark, incoming_df, dim_escola_path)

    result = spark.read.format("delta").load(dim_escola_path).collect()
    assert len(result) == 1


def test_dim_escola_scd2_versions_on_attribute_change(spark, tmp_path):
    dim_escola_path = str(tmp_path / "dim_escola")

    escolas_path_v1 = _write_csv(
        tmp_path, "escolas_v1.csv", ESCOLAS_HEADER, ["esc02,Escola Beta,Belo Horizonte,publica"]
    )
    incoming_v1 = build_dim_escola_incoming(read_silver_escolas(spark, escolas_path_v1))
    write_dim_escola_scd2(spark, incoming_v1, dim_escola_path)

    # Escola muda de rede pública para privada.
    escolas_path_v2 = _write_csv(
        tmp_path, "escolas_v2.csv", ESCOLAS_HEADER, ["esc02,Escola Beta,Belo Horizonte,privada"]
    )
    incoming_v2 = build_dim_escola_incoming(read_silver_escolas(spark, escolas_path_v2))
    write_dim_escola_scd2(spark, incoming_v2, dim_escola_path)

    result = [row.asDict() for row in spark.read.format("delta").load(dim_escola_path).collect()]
    assert len(result) == 2

    ativa = [r for r in result if r["flag_atual"]]
    expirada = [r for r in result if not r["flag_atual"]]
    assert len(ativa) == 1
    assert len(expirada) == 1
    assert ativa[0]["rede"] == "privada"
    assert expirada[0]["rede"] == "publica"
    assert expirada[0]["data_fim_validade"] is not None
    # Versões diferentes da mesma escola precisam ter surrogate keys
    # diferentes, já que representam linhas distintas na dimensão.
    assert ativa[0]["escola_sk"] != expirada[0]["escola_sk"]


def test_build_dim_tempo_covers_full_year(spark):
    dim_tempo_df = build_dim_tempo(spark, 2026)
    result = dim_tempo_df.collect()

    # 2026 não é bissexto (não divisível por 4).
    assert len(result) == 365
    datas = {row["data"] for row in result}
    assert date(2026, 1, 1) in datas
    assert date(2026, 12, 31) in datas

    janeiro_1 = next(row for row in result if row["data"] == date(2026, 1, 1))
    assert janeiro_1["mes"] == 1
    assert janeiro_1["bimestre"] == 1
    assert janeiro_1["nome_mes"] == "Janeiro"


def test_build_fato_avaliacoes_resolves_escola_version_valid_at_evaluation_date(spark, tmp_path):
    # Avaliação antiga (antes da mudança de rede) precisa casar com a versão
    # "publica" da escola, não com a versão "privada" vigente hoje — senão um
    # relatório histórico mostraria um dado de escola que não existia na
    # época da avaliação.
    dim_escola_path = str(tmp_path / "dim_escola")
    escolas_path_v1 = _write_csv(
        tmp_path, "escolas_v1.csv", ESCOLAS_HEADER, ["esc02,Escola Beta,Belo Horizonte,publica"]
    )
    incoming_v1 = build_dim_escola_incoming(read_silver_escolas(spark, escolas_path_v1))
    write_dim_escola_scd2(spark, incoming_v1, dim_escola_path)
    dim_escola_df = spark.read.format("delta").load(dim_escola_path)

    matriculas_path = str(tmp_path / "silver_matriculas")
    spark.createDataFrame(
        [("2004", "Carlos Lima", "8A", "ativo")], ["aluno_id", "nome", "turma", "status"]
    ).write.format("delta").save(matriculas_path)
    dim_aluno_df = build_dim_aluno(spark.read.format("delta").load(matriculas_path))

    disciplinas_path = _write_csv(
        tmp_path, "silver_disciplinas.csv", DISCIPLINAS_HEADER, ["disc_port,Português,linguagens"]
    )
    dim_disciplina_df = build_dim_disciplina(read_silver_disciplinas(spark, disciplinas_path))

    avaliacoes_path = _write_csv(
        tmp_path,
        "silver_avaliacoes.csv",
        AVALIACOES_HEADER,
        ["a004,2004,esc02,disc_port,2026-01-22,6.5,10,simulado"],
    )
    avaliacoes_df = read_silver_avaliacoes(spark, avaliacoes_path)
    fato_df = build_fato_avaliacoes(avaliacoes_df, dim_aluno_df, dim_escola_df, dim_disciplina_df)
    result = fato_df.collect()[0].asDict()

    escola_ativa_sk = dim_escola_df.filter(F.col("flag_atual")).collect()[0]["escola_sk"]
    assert result["escola_sk"] == escola_ativa_sk
    assert result["aluno_sk"] is not None
    assert result["disciplina_sk"] is not None
    assert result["percentual_nota"] == 65.0


def test_run_gold_star_schema_avaliacoes_is_idempotent(spark, tmp_path):
    matriculas_path = str(tmp_path / "silver_matriculas")
    spark.createDataFrame(
        [("2001", "Maria Silva", "7A", "ativo")], ["aluno_id", "nome", "turma", "status"]
    ).write.format("delta").save(matriculas_path)

    escolas_path = _write_csv(
        tmp_path, "silver_escolas.csv", ESCOLAS_HEADER, ["esc01,Colégio Alfa,Curitiba,privada"]
    )
    disciplinas_path = _write_csv(
        tmp_path, "silver_disciplinas.csv", DISCIPLINAS_HEADER, ["disc_mat,Matemática,exatas"]
    )
    avaliacoes_path = _write_csv(
        tmp_path,
        "silver_avaliacoes.csv",
        AVALIACOES_HEADER,
        ["a001,2001,esc01,disc_mat,2026-01-15,8.5,10,prova_bimestral"],
    )

    paths = {
        "avaliacoes_path": avaliacoes_path,
        "escolas_path": escolas_path,
        "disciplinas_path": disciplinas_path,
        "matriculas_path": matriculas_path,
        "fato_path": str(tmp_path / "gold" / "fato_avaliacoes"),
        "dim_aluno_path": str(tmp_path / "gold" / "dim_aluno"),
        "dim_escola_path": str(tmp_path / "gold" / "dim_escola"),
        "dim_disciplina_path": str(tmp_path / "gold" / "dim_disciplina"),
        "dim_tempo_path": str(tmp_path / "gold" / "dim_tempo"),
    }

    run_gold_star_schema_avaliacoes(spark, **paths, ano_calendario=2026)
    run_gold_star_schema_avaliacoes(spark, **paths, ano_calendario=2026)

    assert DeltaTable.isDeltaTable(spark, paths["fato_path"])
    fato_result = spark.read.format("delta").load(paths["fato_path"]).collect()
    assert len(fato_result) == 1
    assert fato_result[0]["avaliacao_id"] == "a001"

    dim_escola_result = spark.read.format("delta").load(paths["dim_escola_path"]).collect()
    assert len(dim_escola_result) == 1
