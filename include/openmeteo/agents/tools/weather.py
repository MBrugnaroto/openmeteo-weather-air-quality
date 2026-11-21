from datetime import date
import json
from pathlib import Path
import re
from typing import Any
import unicodedata

import pandas as pd
from pydantic_ai import FunctionToolset, RunContext


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[4]


def _load_city_weather_report(
    path: Path,
    city: str,
    analysis_date: str,
    temperature_threshold: float = 25,
    alert_label: str = "Alerta de temperatura: limite atingido",
) -> dict[str, Any]:
    partition_date = date.fromisoformat(analysis_date)
    partition = (
        Path(f"year={partition_date:%Y}")
        / f"month={partition_date:%m}"
        / f"day={partition_date:%d}"
    )
    weather_data = pd.read_parquet(path / partition)

    city_data = weather_data.loc[weather_data["city"] == city]
    if city_data.empty:
        raise ValueError(
            f"No weather data found for city {city!r} on {analysis_date}"
        )

    forecast_dates = pd.to_datetime(
        city_data["forcast_ts"],
        utc=True,
        errors="raise",
    ).dt.date
    city_data = city_data.loc[forecast_dates == date.fromisoformat(analysis_date)]
    if city_data.empty:
        raise ValueError(
            f"No weather data found for city {city!r} on {analysis_date}"
        )

    city_data = city_data.sort_values("forcast_ts")
    temperatures = pd.to_numeric(city_data["temperature_2m"], errors="raise")
    valid_temperatures = temperatures.dropna()
    if valid_temperatures.empty:
        raise ValueError(
            f"No valid temperatures found for city {city!r} on {analysis_date}"
        )

    alert_rows = city_data.loc[temperatures >= temperature_threshold]
    hourly = city_data[["forcast_ts", "temperature_2m"]].rename(
        columns={"forcast_ts": "time", "temperature_2m": "temperature_c"}
    )

    return {
        "city": city,
        "date": analysis_date,
        "region": (
            str(city_data["region"].iloc[0])
            if "region" in city_data.columns
            else None
        ),
        "timezone": "UTC",
        "threshold_c": temperature_threshold,
        "alert_label": alert_label,
        "data_kind": "forecast",
        "summary": {
            "min_c": float(valid_temperatures.min()),
            "max_c": float(valid_temperatures.max()),
            "mean_c": float(valid_temperatures.mean()),
            "hours_at_or_above_threshold": int(
                alert_rows["forcast_ts"].nunique()
            ),
            "first_alert_at": (
                str(alert_rows["forcast_ts"].iloc[0])
                if not alert_rows.empty
                else None
            ),
            "last_alert_at": (
                str(alert_rows["forcast_ts"].iloc[-1])
                if not alert_rows.empty
                else None
            ),
        },
        "hourly": json.loads(hourly.to_json(orient="records", date_format="iso")),
    }


def _write_weather_report(
    path: Path,
    city: str,
    analysis_date: str,
    report_markdown: str,
) -> Path:
    if not report_markdown.strip():
        raise ValueError("The Markdown report must not be empty")

    city_slug = re.sub(
        r"[^a-z0-9]+",
        "-",
        unicodedata.normalize("NFKD", city)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower(),
    ).strip("-")
    if not city_slug:
        raise ValueError(f"Cannot create a report filename for city {city!r}")

    partition_date = date.fromisoformat(analysis_date)
    partition = (
        Path(f"year={partition_date:%Y}")
        / f"month={partition_date:%m}"
        / f"day={partition_date:%d}"
    )
    output_path = path / partition / "reports" / f"{city_slug}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(f"{report_markdown.strip()}\n", encoding="utf-8")
    return output_path


weather_toolset = FunctionToolset[None]()


@weather_toolset.tool
def load_weather_data(
    ctx: RunContext[None],
    city: str,
    analysis_date: str,
) -> dict[str, Any]:
    """Load hourly forecast data and summary for a city and date."""
    return _load_city_weather_report(
        PROJECT_ROOT_PATH / "data" / "context" / "weather",
        city,
        analysis_date,
    )


@weather_toolset.tool
def write_weather_report(
    ctx: RunContext[None],
    city: str,
    analysis_date: str,
    report_markdown: str,
) -> str:
    """Write the final Markdown weather report and return its workspace path."""
    output_path = _write_weather_report(
        PROJECT_ROOT_PATH / "data" / "reports" / "weather",
        city,
        analysis_date,
        report_markdown,
    )
    return str(output_path.relative_to(PROJECT_ROOT_PATH))
