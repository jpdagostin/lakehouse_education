from delta.tables import DeltaTable

from lakehouse_bronze_matriculas import (
    read_bronze_matriculas,
    run_bronze_to_silver_matriculas,
    transform_bronze_to_silver,
    write_silver_matriculas,
)

HEADER = "evento_id,aluno_id,nome,turma,status,data_evento,fonte_ingestao"


def _write_bronze_csv(tmp_path, rows: list[str]) -> str:
    csv_path = tmp_path / "bronze_matriculas.csv"
    csv_path.write_text("\n".join([HEADER, *rows]) + "\n")
    return str(csv_path)


def test_exact_duplicates_are_collapsed(spark, tmp_path):
    bronze_path = _write_bronze_csv(
        tmp_path,
        [
            "ev001,1001,Maria Silva,7A,ativo,2026-01-05T08:00:00,cdc",
            "ev002,1001,Maria Silva,7A,ativo,2026-01-05T08:00:00,cdc",
        ],
    )
    bronze_df = read_bronze_matriculas(spark, bronze_path)
    silver_df = transform_bronze_to_silver(bronze_df)

    result = silver_df.collect()
    assert len(result) == 1
    assert result[0]["turma"] == "7A"


def test_source_priority_breaks_tie_on_same_timestamp(spark, tmp_path):
    bronze_path = _write_bronze_csv(
        tmp_path,
        [
            "ev001,2001,Ana,7A,ativo,2026-01-05T08:00:00,backfill",
            "ev002,2001,Ana,7B,ativo,2026-01-05T08:00:00,cdc",
        ],
    )
    bronze_df = read_bronze_matriculas(spark, bronze_path)
    silver_df = transform_bronze_to_silver(bronze_df)

    result = silver_df.collect()
    assert len(result) == 1
    assert result[0]["turma"] == "7B"
    assert result[0]["ultimo_evento_id"] == "ev002"


def test_forward_fill_heals_null_field_from_earlier_event(spark, tmp_path):
    bronze_path = _write_bronze_csv(
        tmp_path,
        [
            "ev001,3001,Joao,8A,ativo,2026-01-05T08:00:00,cdc",
            "ev002,3001,Joao,,trancado,2026-01-06T09:00:00,cdc",
        ],
    )
    bronze_df = read_bronze_matriculas(spark, bronze_path)
    silver_df = transform_bronze_to_silver(bronze_df)

    result = silver_df.collect()
    assert len(result) == 1
    assert result[0]["status"] == "trancado"
    assert result[0]["turma"] == "8A"


def test_selects_most_recent_record_per_aluno(spark, tmp_path):
    bronze_path = _write_bronze_csv(
        tmp_path,
        [
            "ev001,1002,Joao Souza,8A,ativo,2026-01-06T10:00:00,cdc",
            "ev002,1002,Joao Souza,8B,ativo,2026-01-15T09:00:00,backfill",
            "ev003,1002,Joao Souza,8A,inativo,2026-01-20T11:30:00,cdc",
        ],
    )
    bronze_df = read_bronze_matriculas(spark, bronze_path)
    silver_df = transform_bronze_to_silver(bronze_df)

    result = silver_df.collect()
    assert len(result) == 1
    assert result[0]["status"] == "inativo"
    assert result[0]["ultimo_evento_id"] == "ev003"


def test_write_silver_is_idempotent_and_upserts(spark, tmp_path):
    silver_path = str(tmp_path / "silver_matriculas")

    bronze_path_v1 = _write_bronze_csv(
        tmp_path,
        [
            "ev001,4001,Carla,9A,ativo,2026-01-05T08:00:00,cdc",
        ],
    )
    run_bronze_to_silver_matriculas(spark, bronze_path_v1, silver_path)
    run_bronze_to_silver_matriculas(spark, bronze_path_v1, silver_path)

    assert DeltaTable.isDeltaTable(spark, silver_path)
    first_result = spark.read.format("delta").load(silver_path).collect()
    assert len(first_result) == 1
    assert first_result[0]["status"] == "ativo"

    bronze_path_v2 = _write_bronze_csv(
        tmp_path,
        [
            "ev002,4001,Carla,9A,trancado,2026-01-10T08:00:00,cdc",
        ],
    )
    run_bronze_to_silver_matriculas(spark, bronze_path_v2, silver_path)

    second_result = spark.read.format("delta").load(silver_path).collect()
    assert len(second_result) == 1
    assert second_result[0]["status"] == "trancado"
    assert second_result[0]["ultimo_evento_id"] == "ev002"


def test_write_silver_creates_delta_table_on_first_run(spark, tmp_path):
    silver_path = str(tmp_path / "silver_matriculas_fresh")
    bronze_path = _write_bronze_csv(
        tmp_path,
        [
            "ev001,5001,Bia,6A,ativo,2026-01-05T08:00:00,cdc",
        ],
    )
    bronze_df = read_bronze_matriculas(spark, bronze_path)
    silver_df = transform_bronze_to_silver(bronze_df)

    assert not DeltaTable.isDeltaTable(spark, silver_path)
    write_silver_matriculas(spark, silver_df, silver_path)
    assert DeltaTable.isDeltaTable(spark, silver_path)
