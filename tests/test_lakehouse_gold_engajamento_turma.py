from datetime import date

from delta.tables import DeltaTable

from lakehouse_gold_engajamento_turma import (
    read_silver_submissoes,
    run_silver_to_gold_engajamento_turma,
    transform_silver_to_gold,
    write_gold_engajamento_turma,
)

HEADER = (
    "submissao_id,aluno_id,escola_id,turma_id,disciplina,"
    "data_submissao,correta,tempo_gasto_segundos"
)


def _write_silver_csv(tmp_path, rows: list[str]) -> str:
    csv_path = tmp_path / "silver_submissoes.csv"
    csv_path.write_text("\n".join([HEADER, *rows]) + "\n")
    return str(csv_path)


def _group(rows, escola_id, turma_id, disciplina, mes_referencia=date(2026, 2, 1)):
    for row in rows:
        if (
            row["escola_id"] == escola_id
            and row["turma_id"] == turma_id
            and row["disciplina"] == disciplina
            and row["mes_referencia"] == mes_referencia
        ):
            return row
    raise AssertionError(f"grupo não encontrado: {escola_id}/{turma_id}/{disciplina}")


def test_aggregates_match_expected_values_from_sample_dataset(spark, tmp_path):
    silver_path = _write_silver_csv(
        tmp_path,
        [
            "s001,2001,esc01,7A,matematica,2026-02-01,true,120",
            "s002,2001,esc01,7A,matematica,2026-02-02,false,300",
            "s003,2002,esc01,7A,matematica,2026-02-01,true,90",
            "s004,2003,esc01,7B,matematica,2026-02-03,true,150",
            "s005,2004,esc02,8A,portugues,2026-02-01,false,200",
            "s006,2004,esc02,8A,portugues,2026-02-02,true,60",
            "s007,2005,esc02,8A,portugues,2026-02-04,true,80",
            "s008,2001,esc01,7A,portugues,2026-02-05,true,110",
            "s009,2003,esc01,7B,matematica,2026-02-10,false,400",
            "s010,2002,esc01,7A,matematica,2026-02-15,true,95",
        ],
    )
    silver_df = read_silver_submissoes(spark, silver_path)
    gold_df = transform_silver_to_gold(silver_df)

    result = [row.asDict() for row in gold_df.collect()]

    # Nenhuma submissão perdida ou duplicada na agregação.
    assert sum(row["total_submissoes"] for row in result) == 10

    mat_7a = _group(result, "esc01", "7A", "matematica")
    assert mat_7a["total_submissoes"] == 4
    assert mat_7a["total_acertos"] == 3
    assert round(mat_7a["taxa_acerto_pct"], 2) == 75.0
    assert mat_7a["tempo_medio_segundos"] == 151.25
    assert mat_7a["alunos_distintos"] == 2

    port_7a = _group(result, "esc01", "7A", "portugues")
    assert port_7a["total_submissoes"] == 1
    assert port_7a["taxa_acerto_pct"] == 100.0
    assert port_7a["tempo_medio_segundos"] == 110
    assert port_7a["alunos_distintos"] == 1

    mat_7b = _group(result, "esc01", "7B", "matematica")
    assert mat_7b["total_submissoes"] == 2
    assert mat_7b["total_acertos"] == 1
    assert mat_7b["taxa_acerto_pct"] == 50.0
    assert mat_7b["tempo_medio_segundos"] == 275
    assert mat_7b["alunos_distintos"] == 1

    port_8a = _group(result, "esc02", "8A", "portugues")
    assert port_8a["total_submissoes"] == 3
    assert port_8a["total_acertos"] == 2
    assert round(port_8a["taxa_acerto_pct"], 2) == 66.67
    assert round(port_8a["tempo_medio_segundos"], 2) == 113.33
    assert port_8a["alunos_distintos"] == 2


def test_does_not_mix_disciplinas_of_the_same_turma(spark, tmp_path):
    # esc01/7A tem matematica e portugues no dataset de exemplo; a
    # granularidade precisa manter os dois grupos separados, sem misturar
    # taxa de acerto/tempo médio de disciplinas diferentes.
    silver_path = _write_silver_csv(
        tmp_path,
        [
            "s001,2001,esc01,7A,matematica,2026-02-01,true,120",
            "s008,2001,esc01,7A,portugues,2026-02-05,true,110",
        ],
    )
    silver_df = read_silver_submissoes(spark, silver_path)
    gold_df = transform_silver_to_gold(silver_df)

    result = [row.asDict() for row in gold_df.collect()]
    assert len(result) == 2
    assert {row["disciplina"] for row in result} == {"matematica", "portugues"}


def test_mes_referencia_groups_by_month_not_by_day(spark, tmp_path):
    # Caso sintético (não presente no dataset de exemplo, que é só de
    # fevereiro): duas submissões no mesmo mês em dias diferentes devem cair
    # no mesmo grupo, e uma submissão de outro mês não deve ser somada
    # junto.
    silver_path = _write_silver_csv(
        tmp_path,
        [
            "s001,2001,esc01,7A,matematica,2026-02-01,true,120",
            "s002,2002,esc01,7A,matematica,2026-02-28,true,90",
            "s003,2003,esc01,7A,matematica,2026-03-01,false,200",
        ],
    )
    silver_df = read_silver_submissoes(spark, silver_path)
    gold_df = transform_silver_to_gold(silver_df)

    result = [row.asDict() for row in gold_df.collect()]
    assert len(result) == 2

    fevereiro = _group(result, "esc01", "7A", "matematica", mes_referencia=date(2026, 2, 1))
    assert fevereiro["total_submissoes"] == 2
    assert fevereiro["alunos_distintos"] == 2

    marco = _group(result, "esc01", "7A", "matematica", mes_referencia=date(2026, 3, 1))
    assert marco["total_submissoes"] == 1
    assert marco["taxa_acerto_pct"] == 0.0


def test_write_gold_is_idempotent_and_upserts(spark, tmp_path):
    gold_path = str(tmp_path / "gold_engajamento_turma")

    silver_path_v1 = _write_silver_csv(
        tmp_path,
        [
            "s001,2001,esc01,7A,matematica,2026-02-01,true,120",
        ],
    )
    run_silver_to_gold_engajamento_turma(spark, silver_path_v1, gold_path)
    run_silver_to_gold_engajamento_turma(spark, silver_path_v1, gold_path)

    assert DeltaTable.isDeltaTable(spark, gold_path)
    first_result = spark.read.format("delta").load(gold_path).collect()
    assert len(first_result) == 1
    assert first_result[0]["total_submissoes"] == 1

    silver_path_v2 = _write_silver_csv(
        tmp_path,
        [
            "s001,2001,esc01,7A,matematica,2026-02-01,true,120",
            "s002,2002,esc01,7A,matematica,2026-02-02,false,300",
        ],
    )
    run_silver_to_gold_engajamento_turma(spark, silver_path_v2, gold_path)

    second_result = spark.read.format("delta").load(gold_path).collect()
    assert len(second_result) == 1
    assert second_result[0]["total_submissoes"] == 2


def test_write_gold_creates_delta_table_on_first_run(spark, tmp_path):
    gold_path = str(tmp_path / "gold_engajamento_turma_fresh")
    silver_path = _write_silver_csv(
        tmp_path,
        [
            "s001,2001,esc01,7A,matematica,2026-02-01,true,120",
        ],
    )
    silver_df = read_silver_submissoes(spark, silver_path)
    gold_df = transform_silver_to_gold(silver_df)

    assert not DeltaTable.isDeltaTable(spark, gold_path)
    write_gold_engajamento_turma(spark, gold_df, gold_path)
    assert DeltaTable.isDeltaTable(spark, gold_path)
