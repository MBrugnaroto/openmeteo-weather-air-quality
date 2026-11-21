from pathlib import Path

import pandas as pd

from dags.issue_temp_alert_report import (
    ALERT_LABEL,
    TEMPERATURE_THRESHOLD_C,
    _get_partition,
    _write_temperature_alerts,
)
from include.openmeteo.agents.tools.weather import (
    _load_city_weather_report,
    _write_weather_report,
)


def test_loads_requested_partition_and_includes_threshold(tmp_path):
    older_partition = tmp_path / "year=2026" / "month=11" / "day=17"
    latest_partition = tmp_path / "year=2026" / "month=11" / "day=18"
    older_partition.mkdir(parents=True)
    latest_partition.mkdir(parents=True)

    pd.DataFrame(
        {
            "city": ["older"],
            "forcast_ts": ["2026-11-17T00:00:00+00:00"],
            "temperature_2m": [40],
        }
    ).to_parquet(
        older_partition / "older.parquet"
    )
    pd.DataFrame(
        {
            "city": ["Curitiba", "Curitiba", "Curitiba"],
            "forcast_ts": [
                "2026-11-18T00:00:00+00:00",
                "2026-11-18T01:00:00+00:00",
                "2026-11-18T03:00:00+00:00",
            ],
            "temperature_2m": [24.9, 25, 30],
        }
    ).to_parquet(
        latest_partition / "city_a.parquet"
    )
    pd.DataFrame(
        {
            "city": ["Porto Alegre", "Porto Alegre"],
            "forcast_ts": [
                "2026-11-18T00:30:00+00:00",
                "2026-11-18T02:00:00+00:00",
            ],
            "temperature_2m": [26, 27],
        }
    ).to_parquet(
        latest_partition / "city_b.parquet"
    )

    weather_path, relative_partition = _get_partition(tmp_path, "2026-11-18")
    weather_data = pd.read_parquet(weather_path)
    alerts = weather_data.loc[
        weather_data["temperature_2m"] >= TEMPERATURE_THRESHOLD_C
    ]

    assert alerts["temperature_2m"].tolist() == [25, 30, 26, 27]
    assert weather_path == tmp_path / "year=2026" / "month=11" / "day=18"
    assert relative_partition == Path("year=2026") / "month=11" / "day=18"

    output_path = (
        tmp_path
        / "semantic"
        / relative_partition
        / "weather_alerts.parquet"
    )
    city_alerts = _write_temperature_alerts(
        alerts,
        output_path,
        analysis_date="2026-11-18",
    )

    assert city_alerts == [
        {"city": "Curitiba", "date": "2026-11-18", "alert_label": ALERT_LABEL},
        {
            "city": "Porto Alegre",
            "date": "2026-11-18",
            "alert_label": ALERT_LABEL,
        },
    ]
    assert pd.read_parquet(output_path)["temperature_2m"].tolist() == [
        25,
        30,
        26,
        27,
    ]


def test_writes_empty_partition_and_returns_no_alert(tmp_path):
    alerts = pd.DataFrame(columns=["city", "forcast_ts", "temperature_2m"])
    output_path = tmp_path / "semantic" / "year=2026" / "weather_alerts.parquet"

    city_alerts = _write_temperature_alerts(
        alerts,
        output_path,
        analysis_date="2026-11-18",
    )

    assert city_alerts == []
    assert output_path.exists()
    assert pd.read_parquet(output_path).empty


def test_loads_city_report_for_requested_date(tmp_path):
    partition = tmp_path / "year=2026" / "month=11" / "day=18"
    partition.mkdir(parents=True)
    pd.DataFrame(
        {
            "city": ["Curitiba", "Curitiba", "Curitiba"],
            "forcast_ts": [
                "2026-11-18T00:00:00+00:00",
                "2026-11-18T01:00:00+00:00",
                "2026-11-19T00:00:00+00:00",
            ],
            "temperature_2m": [24.9, 25.0, 35.0],
        }
    ).to_parquet(partition / "curitiba.parquet")

    report_data = _load_city_weather_report(
        tmp_path,
        "Curitiba",
        "2026-11-18",
    )

    assert report_data["date"] == "2026-11-18"
    assert report_data["summary"] == {
        "min_c": 24.9,
        "max_c": 25.0,
        "mean_c": 24.95,
        "hours_at_or_above_threshold": 1,
        "first_alert_at": "2026-11-18T01:00:00+00:00",
        "last_alert_at": "2026-11-18T01:00:00+00:00",
    }
    assert report_data["hourly"] == [
        {"time": "2026-11-18T00:00:00+00:00", "temperature_c": 24.9},
        {"time": "2026-11-18T01:00:00+00:00", "temperature_c": 25.0},
    ]


def test_writes_formatted_report_to_city_partition(tmp_path):
    report_markdown = "# Alerta de temperatura\n\nTexto do relatório."

    output_path = _write_weather_report(
        tmp_path / "weather",
        "São Paulo",
        "2026-11-18",
        report_markdown,
    )

    assert output_path == (
        tmp_path
        / "weather"
        / "year=2026"
        / "month=11"
        / "day=18"
        / "reports"
        / "sao-paulo.md"
    )
    assert output_path.read_text(encoding="utf-8") == (
        "# Alerta de temperatura\n\nTexto do relatório.\n"
    )
