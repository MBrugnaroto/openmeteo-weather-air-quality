from pathlib import Path

import pandas as pd
from pydantic import TypeAdapter

from dags.issue_air_quality_report import (
    ALERT_LABEL,
    _build_city_alerts,
    _load_region_air_quality_data,
    _write_air_quality_alerts,
)
from include.openmeteo.agents.tools.air_quality import (
    _load_city_air_quality_report,
    _write_air_quality_report,
)


def test_alerts_use_daily_mean_and_either_pollutant_guideline():
    hourly_data = pd.DataFrame(
        {
            "region": ["sul"] * 6,
            "city": [
                "At threshold",
                "At threshold",
                "PM2.5 alert",
                "PM2.5 alert",
                "PM10 alert",
                "PM10 alert",
            ],
            "pm2_5": [15, 15, 16, 18, 5, 7],
            "pm10": [45, 45, 20, 30, 46, 48],
        }
    )

    alerts = _build_city_alerts(hourly_data)

    assert alerts["city"].tolist() == ["PM10 alert", "PM2.5 alert"]
    assert alerts["pm2_5_daily_mean_ug_m3"].tolist() == [6.0, 17.0]
    assert alerts["pm10_daily_mean_ug_m3"].tolist() == [47.0, 25.0]
    assert alerts["pm2_5_exceeds_guideline"].tolist() == [False, True]
    assert alerts["pm10_exceeds_guideline"].tolist() == [True, False]
    assert alerts["alert_label"].unique().tolist() == [ALERT_LABEL]


def test_loads_only_requested_region_and_date(tmp_path):
    partition = tmp_path / "year=2026" / "month=11" / "day=18"
    partition.mkdir(parents=True)
    pd.DataFrame(
        {
            "region": ["sul", "sul"],
            "city": ["Curitiba", "Curitiba"],
            "forcast_ts": [
                "2026-11-18T00:00:00+00:00",
                "2026-11-19T00:00:00+00:00",
            ],
            "pm2_5": [10, 100],
            "pm10": [20, 200],
        }
    ).to_parquet(partition / "sul_Curitiba.parquet")
    pd.DataFrame(
        {
            "region": ["nordeste"],
            "city": ["Recife"],
            "forcast_ts": ["2026-11-18T00:00:00+00:00"],
            "pm2_5": [30],
            "pm10": [40],
        }
    ).to_parquet(partition / "nordeste_Recife.parquet")

    weather_data, relative_partition = _load_region_air_quality_data(
        tmp_path,
        "sul",
        "2026-11-18",
    )

    assert relative_partition == Path("year=2026") / "month=11" / "day=18"
    assert weather_data["city"].tolist() == ["Curitiba"]
    assert weather_data["pm2_5"].tolist() == [10]


def test_writes_region_partitioned_alert_output(tmp_path):
    alerts = _build_city_alerts(
        pd.DataFrame(
            {
                "region": ["sul"],
                "city": ["Curitiba"],
                "pm2_5": [16],
                "pm10": [30],
            }
        )
    )
    output_path = (
        tmp_path
        / "year=2026"
        / "month=11"
        / "day=18"
        / "region=sul"
        / "air_quality_alerts.parquet"
    )

    report_alerts = _write_air_quality_alerts(
        alerts,
        output_path,
        analysis_date="2026-11-18",
    )

    assert report_alerts == [
        {
            "city": "Curitiba",
            "region": "sul",
            "date": "2026-11-18",
            "alert_label": ALERT_LABEL,
        }
    ]
    assert pd.read_parquet(output_path)["pm2_5_daily_mean_ug_m3"].tolist() == [
        16.0
    ]


def test_air_quality_tool_filters_by_city_region_and_date(tmp_path):
    partition = tmp_path / "year=2026" / "month=11" / "day=18"
    partition.mkdir(parents=True)
    pd.DataFrame(
        {
            "region": ["sul", "nordeste", "sul"],
            "city": ["São José", "São José", "São José"],
            "forcast_ts": [
                "2026-11-18T00:00:00+00:00",
                "2026-11-18T00:00:00+00:00",
                "2026-11-19T00:00:00+00:00",
            ],
            "pm2_5": [10, 30, 100],
            "pm10": [20, 40, 200],
        }
    ).to_parquet(partition / "sul_São José.parquet")

    report_data = _load_city_air_quality_report(
        tmp_path,
        "São José",
        "Sul",
        "2026-11-18",
    )

    assert report_data["region"] == "sul"
    assert report_data["summary"]["pm2_5_daily_mean_ug_m3"] == 10
    assert type(report_data["summary"]["pm2_5_exceeds_guideline"]) is bool
    TypeAdapter(dict).dump_json(report_data)
    assert report_data["hourly"] == [
        {
            "time": "2026-11-18T00:00:00+00:00",
            "pm2_5_ug_m3": 10,
            "pm10_ug_m3": 20,
        }
    ]


def test_writes_region_scoped_air_quality_report(tmp_path):
    output_path = _write_air_quality_report(
        tmp_path / "air_quality",
        "São José",
        "Sul",
        "2026-11-18",
        "# Alerta de qualidade do ar",
    )

    assert output_path == (
        tmp_path
        / "air_quality"
        / "year=2026"
        / "month=11"
        / "day=18"
        / "reports"
        / "region=sul"
        / "sao-jose.md"
    )
    assert output_path.read_text(encoding="utf-8") == (
        "# Alerta de qualidade do ar\n"
    )
