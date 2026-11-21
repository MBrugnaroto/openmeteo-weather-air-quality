from datetime import date
from pathlib import Path

import pandas as pd
import pendulum

from airflow.sdk import dag, task, get_current_context
from include.openmeteo.assets import CONTEXT_WEATHER
from include.openmeteo.agents.prompts.weather import WEATHER_AGENT
from include.openmeteo.agents.tools.weather import weather_toolset


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]
TEMPERATURE_THRESHOLD_C = 30
ALERT_LABEL = "Alerta de temperatura: limite atingido"


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


def _load_region_weather_data(
    context_path: Path,
    region: str,
    analysis_date: str,
) -> tuple[pd.DataFrame, Path]:
    weather_path, relative_partition = _get_partition(context_path, analysis_date)
    city_files = sorted(weather_path.glob(f"{region}_*.parquet"))
    if not city_files:
        raise FileNotFoundError(
            f"No weather files found for region {region!r} on {analysis_date}"
        )

    return (
        pd.concat(
            [pd.read_parquet(city_file) for city_file in city_files],
            ignore_index=True,
        ),
        relative_partition,
    )


def _write_temperature_alerts(
    alerts: pd.DataFrame,
    output_path: Path,
    analysis_date: str,
    region: str,
) -> list[dict[str, str]]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    alerts.to_parquet(output_path, index=False)

    if alerts.empty:
        return []

    first_alerts_by_city = alerts.drop_duplicates(subset="city", keep="first")

    return [
        {
            "city": str(alert["city"]),
            "region": region,
            "date": analysis_date,
            "alert_label": ALERT_LABEL,
        }
        for _, alert in first_alerts_by_city.iterrows()
    ]


@dag(
    schedule=[CONTEXT_WEATHER],
    start_date=pendulum.datetime(2025, 4, 22),
    default_args={"retries": 2},
    tags=["weather"],
    dag_display_name="Open-Meteo Weather Alerts",
    is_paused_upon_creation=False
)
def issue_temp_alert_report():
    @task
    def get_temperature_threshold() -> list[dict[str, str]]:
        events = get_current_context()["triggering_asset_events"].get(
            CONTEXT_WEATHER, []
        )
        if not events:
            raise ValueError("No triggering weather asset events were found")

        events_by_region_date: dict[tuple[str, str], bool] = {}
        for event in events:
            event_extra = event.extra or {}
            region = event_extra.get("region")
            analysis_date = event_extra.get("process_date")
            if not isinstance(region, str) or not region:
                raise ValueError("Weather asset event is missing its region")
            if not isinstance(analysis_date, str) or not analysis_date:
                raise ValueError("Weather asset event is missing its process_date")

            key = (region, analysis_date)
            events_by_region_date[key] = (
                events_by_region_date.get(key, False)
                or not event_extra.get("is_backfill", False)
            )

        report_alerts = []
        context_path = PROJECT_ROOT_PATH / "data" / "context" / "weather"
        semantic_path = PROJECT_ROOT_PATH / "data" / "semantic" / "weather"
        for (region, analysis_date), should_report in sorted(
            events_by_region_date.items()
        ):
            weather_data, partition_date = _load_region_weather_data(
                context_path,
                region,
                analysis_date,
            )
            alerts = weather_data.loc[
                weather_data["temperature_2m"] >= TEMPERATURE_THRESHOLD_C
            ]
            semantic_partition = (
                semantic_path
                / partition_date
                / f"region={region}"
                / "weather_alerts.parquet"
            )
            region_alerts = _write_temperature_alerts(
                alerts,
                semantic_partition,
                analysis_date,
                region,
            )
            if should_report:
                report_alerts.extend(region_alerts)

        return report_alerts

    @task.short_circuit
    def need_alert(alerts: list[dict[str, str]]) -> bool:
        return bool(alerts)

    @task.agent(
        llm_conn_id="WeatherAlert",
        system_prompt=WEATHER_AGENT,
        toolsets=[weather_toolset],
    )
    def create_report(alert: dict[str, str]) -> str:
        return (
            f"""
                Crie o relatório para a cidade {alert['city']} na região
                {alert['region']} em {alert['date']}.
                Use o rótulo {alert['alert_label']!r}.
                Antes de escrever, chame a ferramenta load_weather_data com a cidade,
                região e data informadas.
                Baseie o relatório somente no resultado da ferramenta.
                Depois, chame write_weather_report com o conteúdo Markdown final,
                a cidade, região e data. Retorne o relatório formatado após salvá-lo.
            """
        )

    city_alerts = get_temperature_threshold()
    alert_gate = need_alert(city_alerts)
    alert_gate >> create_report.expand(alert=city_alerts)


issue_temp_alert_report()
