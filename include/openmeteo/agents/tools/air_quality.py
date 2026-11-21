from datetime import date
import json
from pathlib import Path
import re
from typing import Any
import unicodedata

import pandas as pd
from pydantic_ai import FunctionToolset, RunContext


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[4]
PM2_5_DAILY_GUIDELINE = 15.0
PM10_DAILY_GUIDELINE = 45.0


def _load_city_air_quality_report(
    path: Path,
    city: str,
    region: str,
    analysis_date: str,
) -> dict[str, Any]:
    region_key = region.strip().casefold()
    partition_date = date.fromisoformat(analysis_date)
    partition = (
        Path(f"year={partition_date:%Y}")
        / f"month={partition_date:%m}"
        / f"day={partition_date:%d}"
    )
    city_path = path / partition / f"{region_key}_{city}.parquet"
    if not city_path.exists():
        raise FileNotFoundError(
            f"No air quality data found for {city!r} in region {region!r} "
            f"on {analysis_date}"
        )

    city_data = pd.read_parquet(city_path)
    city_data = city_data.loc[
        (
            city_data["region"]
            .astype("string")
            .str.strip()
            .str.casefold()
            == region_key
        )
        & (city_data["city"] == city)
    ]
    forecast_dates = pd.to_datetime(
        city_data["forcast_ts"],
        utc=True,
        errors="raise",
    ).dt.date
    city_data = city_data.loc[forecast_dates == partition_date].sort_values(
        "forcast_ts"
    )
    if city_data.empty:
        raise ValueError(
            f"No air quality data found for {city!r} in region {region!r} "
            f"on {analysis_date}"
        )

    pm2_5 = pd.to_numeric(city_data["pm2_5"], errors="raise")
    pm10 = pd.to_numeric(city_data["pm10"], errors="raise")
    valid_pm2_5 = pm2_5.dropna()
    valid_pm10 = pm10.dropna()
    if valid_pm2_5.empty and valid_pm10.empty:
        raise ValueError(
            f"No valid PM2.5 or PM10 values for {city!r} on {analysis_date}"
        )

    hourly = city_data[["forcast_ts", "pm2_5", "pm10"]].rename(
        columns={
            "forcast_ts": "time",
            "pm2_5": "pm2_5_ug_m3",
            "pm10": "pm10_ug_m3",
        }
    )
    pm2_5_mean = valid_pm2_5.mean() if not valid_pm2_5.empty else None
    pm10_mean = valid_pm10.mean() if not valid_pm10.empty else None

    return {
        "city": city,
        "region": region_key,
        "date": analysis_date,
        "data_kind": "forecast",
        "guidelines_ug_m3": {
            "pm2_5_daily_mean": PM2_5_DAILY_GUIDELINE,
            "pm10_daily_mean": PM10_DAILY_GUIDELINE,
        },
        "summary": {
            "pm2_5_daily_mean_ug_m3": (
                float(pm2_5_mean) if pm2_5_mean is not None else None
            ),
            "pm10_daily_mean_ug_m3": (
                float(pm10_mean) if pm10_mean is not None else None
            ),
            "pm2_5_max_ug_m3": (
                float(valid_pm2_5.max()) if not valid_pm2_5.empty else None
            ),
            "pm10_max_ug_m3": (
                float(valid_pm10.max()) if not valid_pm10.empty else None
            ),
            "pm2_5_exceeds_guideline": bool(
                pm2_5_mean is not None
                and pm2_5_mean > PM2_5_DAILY_GUIDELINE
            ),
            "pm10_exceeds_guideline": bool(
                pm10_mean is not None and pm10_mean > PM10_DAILY_GUIDELINE
            ),
        },
        "hourly": json.loads(hourly.to_json(orient="records", date_format="iso")),
    }


def _write_air_quality_report(
    path: Path,
    city: str,
    region: str,
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
    output_path = (
        path
        / partition
        / "reports"
        / f"region={region.strip().casefold()}"
        / f"{city_slug}.md"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(f"{report_markdown.strip()}\n", encoding="utf-8")
    return output_path


air_quality_toolset = FunctionToolset[None]()


@air_quality_toolset.tool
def load_air_quality_data(
    ctx: RunContext[None],
    city: str,
    region: str,
    analysis_date: str,
) -> dict[str, Any]:
    """Load hourly air quality forecast and daily means for a city and region."""
    return _load_city_air_quality_report(
        PROJECT_ROOT_PATH / "data" / "context" / "air_quality",
        city,
        region,
        analysis_date,
    )


@air_quality_toolset.tool
def write_air_quality_report(
    ctx: RunContext[None],
    city: str,
    region: str,
    analysis_date: str,
    report_markdown: str,
) -> str:
    """Write the final Markdown air quality report and return its workspace path."""
    output_path = _write_air_quality_report(
        PROJECT_ROOT_PATH / "data" / "reports" / "air_quality",
        city,
        region,
        analysis_date,
        report_markdown,
    )
    return str(output_path.relative_to(PROJECT_ROOT_PATH))
