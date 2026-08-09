import json

from lakehouse_bronze_eventos_acesso import (
    read_bronze_eventos_com_app_version,
    run_raw_to_bronze_eventos_acesso,
)

HEADER_DIA1 = "evento_id,aluno_id,tipo_evento,timestamp"
HEADER_DIA2 = "evento_id,aluno_id,tipo_evento,timestamp,dispositivo"


def _write_csv(tmp_path, filename: str, header: str, rows: list[str]) -> str:
    csv_path = tmp_path / filename
    csv_path.write_text("\n".join([header, *rows]) + "\n")
    return str(csv_path)


def _write_json(tmp_path, filename: str, registros: list[dict]) -> str:
    json_path = tmp_path / filename
    json_path.write_text(json.dumps(registros))
    return str(json_path)


def test_dia1_sozinho_grava_sem_dispositivo_nem_metadata(spark, tmp_path):
    dia1 = _write_csv(
        tmp_path,
        "dia1.csv",
        HEADER_DIA1,
        ["e1001,2001,login,2026-02-01T08:00:00"],
    )
    bronze_path = str(tmp_path / "bronze_eventos_acesso")

    run_raw_to_bronze_eventos_acesso(
        spark, [(dia1, "csv", {"header": True})], bronze_path=bronze_path
    )

    df = spark.read.format("delta").load(bronze_path)
    result = df.collect()
    assert len(result) == 1
    assert result[0]["aluno_id"] == "2001"


def test_dia2_adiciona_dispositivo_sem_afetar_dia1(spark, tmp_path):
    dia1 = _write_csv(
        tmp_path,
        "dia1.csv",
        HEADER_DIA1,
        ["e1001,2001,login,2026-02-01T08:00:00"],
    )
    dia2 = _write_csv(
        tmp_path,
        "dia2.csv",
        HEADER_DIA2,
        ["e2001,2001,login,2026-02-02T08:00:00,mobile"],
    )
    bronze_path = str(tmp_path / "bronze_eventos_acesso")

    lotes = [
        (dia1, "csv", {"header": True}),
        (dia2, "csv", {"header": True}),
    ]
    run_raw_to_bronze_eventos_acesso(spark, lotes, bronze_path=bronze_path)

    df = spark.read.format("delta").load(bronze_path)
    result = {row["evento_id"]: row for row in df.collect()}

    assert result["e1001"]["dispositivo"] is None
    assert result["e2001"]["dispositivo"] == "mobile"


def test_dia3_json_adiciona_metadata_sem_afetar_dias_anteriores(spark, tmp_path):
    dia1 = _write_csv(
        tmp_path,
        "dia1.csv",
        HEADER_DIA1,
        ["e1001,2001,login,2026-02-01T08:00:00"],
    )
    dia2 = _write_csv(
        tmp_path,
        "dia2.csv",
        HEADER_DIA2,
        ["e2001,2001,login,2026-02-02T08:00:00,mobile"],
    )
    dia3 = _write_json(
        tmp_path,
        "dia3.json",
        [
            {
                "evento_id": "e3001",
                "aluno_id": 2001,
                "tipo_evento": "login",
                "timestamp": "2026-02-03T08:00:00",
                "dispositivo": "mobile",
                "metadata": {"app_version": "4.2.1", "os": "android"},
            },
            {
                "evento_id": "e3002",
                "aluno_id": 2002,
                "tipo_evento": "submissao_exercicio",
                "timestamp": "2026-02-03T09:00:00",
                "dispositivo": "web",
                "metadata": None,
            },
        ],
    )
    bronze_path = str(tmp_path / "bronze_eventos_acesso")

    lotes = [
        (dia1, "csv", {"header": True}),
        (dia2, "csv", {"header": True}),
        (dia3, "json", {"multiLine": True}),
    ]
    run_raw_to_bronze_eventos_acesso(spark, lotes, bronze_path=bronze_path)

    df = read_bronze_eventos_com_app_version(spark, bronze_path)
    result = {row["evento_id"]: row for row in df.collect()}

    assert len(result) == 4
    # registros dos dias 1 e 2 não têm metadata: consulta retorna null, sem erro
    assert result["e1001"]["app_version"] is None
    assert result["e2001"]["app_version"] is None
    # e3002 tem metadata explicitamente null no JSON de origem
    assert result["e3002"]["app_version"] is None
    # e3001 tem metadata.app_version preenchido
    assert result["e3001"]["app_version"] == "4.2.1"
