from airflow.sdk import Asset


CONTEXT_WEATHER = Asset(
    uri="x-openmeteo://context/weather",
    name="context.weather"
)

CONTEXT_AIR_QUALITY = Asset(
    uri="x-openmeteo://context/air_quality",
    name="context.air_quality"
)