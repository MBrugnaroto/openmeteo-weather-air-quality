from datetime import date
from pathlib import Path

import pandas as pd
import pendulum

from airflow.sdk import dag, task, get_current_context
from include.openmeteo.assets import CONTEXT_WEATHER
from include.openmeteo.agents.prompts.weather import WEATHER_AGENT
from include.openmeteo.agents.tools.weather import weather_toolset


PROJECT_ROOT_PATH = Path(__file__).resolve().parents[1]
TEMPERATURE_THRESHOLD_C = 25
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


def _write_temperature_alerts(
    alerts: pd.DataFrame,
    output_path: Path,
    analysis_date: str,
) -> list[dict[str, str]]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    alerts.to_parquet(output_path, index=False)

    if alerts.empty:
        return []

    first_alerts_by_city = alerts.drop_duplicates(subset="city", keep="first")

    return [
        {
            "city": str(alert["city"]),
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

        info_extra = events[-1].extra
        analysis_date = info_extra["process_date"]
        weather_path, partition_date = _get_partition(
            PROJECT_ROOT_PATH / "data" / "context" / "weather",
            analysis_date
        )

        weather_data = pd.read_parquet(weather_path)
        alerts = weather_data.loc[
            weather_data["temperature_2m"] >= TEMPERATURE_THRESHOLD_C
        ]
        semantic_partition = (
            PROJECT_ROOT_PATH
            / "data"
            / "semantic"
            / "weather"
            / partition_date
            / "weather_alerts.parquet"
        )

        return _write_temperature_alerts(
            alerts,
            semantic_partition,
            analysis_date,
        )

    @task.short_circuit
    def need_alert(alerts: list[dict[str, str]]) -> bool:
        events = get_current_context()["triggering_asset_events"].get(
            CONTEXT_WEATHER, []
        )

        return len(alerts) > 0 and bool(events) and any(
            not event.extra.get("is_backfill", False)
            for event in events
        )

    @task.agent(
        llm_conn_id="WeatherAlert",
        system_prompt=WEATHER_AGENT,
        toolsets=[weather_toolset],
    )
    def create_report(alert: dict[str, str]) -> str:
        return (
            f"""
                Crie o relatório para a cidade {alert['city']} em {alert['date']}.
                Use o rótulo {alert['alert_label']!r}.
                Antes de escrever, chame a ferramenta load_weather_data com a cidade e a data informadas.
                Baseie o relatório somente no resultado da ferramenta.
                Depois, chame write_weather_report com o conteúdo Markdown final,
                a cidade e a data. Retorne o relatório formatado após salvá-lo.
            """
        )

    city_alerts = get_temperature_threshold()
    alert_gate = need_alert(city_alerts)
    alert_gate >> create_report.expand(alert=city_alerts)


issue_temp_alert_report()
