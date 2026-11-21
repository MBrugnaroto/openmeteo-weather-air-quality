from datetime import date
from pathlib import Path

import pandas as pd
import pendulum

from airflow.sdk import dag, get_current_context, task
from include.openmeteo.agents.prompts.air_quality import AIR_QUALITY_AGENT
from include.openmeteo.agents.tools.air_quality import (
    PM10_DAILY_GUIDELINE,
    PM2_5_DAILY_GUIDELINE,
    air_quality_toolset,
)
from include.openmeteo.assets import CONTEXT_AIR_QUALITY


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]
ALERT_LABEL = "Alerta de qualidade do ar: diretriz diária OMS excedida"


def _get_partition(
    context_path: Path,
    analysis_date: str,
) -> tuple[Path, Path]:
    partition_date = date.fromisoformat(analysis_date)
    relative_partition = (
        Path(f"year={partition_date:%Y}")
        / f"month={partition_date:%m}"
        / f"day={partition_date:%d}"
    )
    return context_path / relative_partition, relative_partition


def _load_region_air_quality_data(
    context_path: Path,
    region: str,
    analysis_date: str,
) -> tuple[pd.DataFrame, Path]:
    partition_path, relative_partition = _get_partition(
        context_path,
        analysis_date,
    )
    city_files = sorted(partition_path.glob(f"{region}_*.parquet"))
    if not city_files:
        raise FileNotFoundError(
            f"No air quality files found for region {region!r} "
            f"on {analysis_date}"
        )

    air_quality_data = pd.concat(
        [pd.read_parquet(city_file) for city_file in city_files],
        ignore_index=True,
    )
    forecast_dates = pd.to_datetime(
        air_quality_data["forcast_ts"],
        utc=True,
        errors="raise",
    ).dt.date
    air_quality_data = air_quality_data.loc[
        forecast_dates == date.fromisoformat(analysis_date)
    ].copy()
    if air_quality_data.empty:
        raise ValueError(
            f"No air quality forecast rows for region {region!r} "
            f"on {analysis_date}"
        )

    return air_quality_data, relative_partition


def _build_city_alerts(air_quality_data: pd.DataFrame) -> pd.DataFrame:
    data = air_quality_data.copy()
    data["pm2_5"] = pd.to_numeric(data["pm2_5"], errors="raise")
    data["pm10"] = pd.to_numeric(data["pm10"], errors="raise")

    daily_means = data.groupby(
        ["region", "city"],
        as_index=False,
        sort=True,
    ).agg(
        pm2_5_daily_mean_ug_m3=("pm2_5", "mean"),
        pm10_daily_mean_ug_m3=("pm10", "mean"),
    )
    missing = daily_means[
        daily_means[
            ["pm2_5_daily_mean_ug_m3", "pm10_daily_mean_ug_m3"]
        ].isna().all(axis=1)
    ]
    if not missing.empty:
        cities = ", ".join(missing["city"].astype(str))
        raise ValueError(f"No valid PM2.5 or PM10 values for: {cities}")

    daily_means["pm2_5_exceeds_guideline"] = (
        daily_means["pm2_5_daily_mean_ug_m3"] > PM2_5_DAILY_GUIDELINE
    )
    daily_means["pm10_exceeds_guideline"] = (
        daily_means["pm10_daily_mean_ug_m3"] > PM10_DAILY_GUIDELINE
    )
    daily_means["alert_label"] = ALERT_LABEL

    return daily_means.loc[
        daily_means["pm2_5_exceeds_guideline"]
        | daily_means["pm10_exceeds_guideline"]
    ].copy()


def _write_air_quality_alerts(
    alerts: pd.DataFrame,
    output_path: Path,
    analysis_date: str,
) -> list[dict[str, str]]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    alerts.to_parquet(output_path, index=False)

    return [
        {
            "city": str(alert["city"]),
            "region": str(alert["region"]),
            "date": analysis_date,
            "alert_label": ALERT_LABEL,
        }
        for _, alert in alerts.iterrows()
    ]


@dag(
    schedule=[CONTEXT_AIR_QUALITY],
    start_date=pendulum.datetime(2025, 4, 22),
    default_args={"retries": 2},
    tags=["air-quality"],
    dag_display_name="Open-Meteo Air Quality Alerts",
    is_paused_upon_creation=False,
)
def issue_air_quality_report():
    @task
    def get_air_quality_alerts() -> list[dict[str, str]]:
        events = get_current_context()["triggering_asset_events"].get(
            CONTEXT_AIR_QUALITY,
            [],
        )
        if not events:
            raise ValueError("No triggering air quality asset events were found")

        events_by_region_date: dict[tuple[str, str], bool] = {}
        for event in events:
            event_extra = event.extra or {}
            region = event_extra.get("region")
            analysis_date = event_extra.get("process_date")
            if not isinstance(region, str) or not region:
                raise ValueError("Air quality asset event is missing its region")
            if not isinstance(analysis_date, str) or not analysis_date:
                raise ValueError(
                    "Air quality asset event is missing its process_date"
                )

            key = (region, analysis_date)
            events_by_region_date[key] = (
                events_by_region_date.get(key, False)
                or not event_extra.get("is_backfill", False)
            )

        report_alerts = []
        context_path = PROJECT_ROOT_PATH / "data" / "context" / "air_quality"
        semantic_path = PROJECT_ROOT_PATH / "data" / "semantic" / "air_quality"
        for (region, analysis_date), should_report in sorted(
            events_by_region_date.items()
        ):
            air_quality_data, partition_date = _load_region_air_quality_data(
                context_path,
                region,
                analysis_date,
            )
            alerts = _build_city_alerts(air_quality_data)
            semantic_partition = (
                semantic_path
                / partition_date
                / f"region={region}"
                / "air_quality_alerts.parquet"
            )
            region_alerts = _write_air_quality_alerts(
                alerts,
                semantic_partition,
                analysis_date,
            )
            if should_report:
                report_alerts.extend(region_alerts)

        return report_alerts

    @task.short_circuit
    def need_alert(alerts: list[dict[str, str]]) -> bool:
        return bool(alerts)

    @task.agent(
        llm_conn_id="WeatherAlert",
        system_prompt=AIR_QUALITY_AGENT,
        toolsets=[air_quality_toolset],
    )
    def create_report(alert: dict[str, str]) -> str:
        return (
            f"""
                Crie o relatório de qualidade do ar para {alert['city']},
                região {alert['region']}, em {alert['date']}.
                Use o rótulo {alert['alert_label']!r}.
                Antes de escrever, chame load_air_quality_data com cidade,
                região e data. Baseie o relatório somente no resultado.
                Depois, chame write_air_quality_report com cidade, região,
                data e o conteúdo Markdown final. Retorne-o após salvá-lo.
            """
        )

    city_alerts = get_air_quality_alerts()
    alert_gate = need_alert(city_alerts)
    alert_gate >> create_report.expand(alert=city_alerts)


issue_air_quality_report()
