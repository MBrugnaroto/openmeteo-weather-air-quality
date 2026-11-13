FROM astrocrpublic.azurecr.io/runtime:3.3-12

# Match the host UID so the non-root Airflow user can edit bind-mounted files.
ARG AIRFLOW_UID=1000
USER root
RUN usermod --uid "${AIRFLOW_UID}" astro \
    && chown -R astro:0 /usr/local/airflow
USER astro

COPY --chown=astro:0 data/ /usr/local/airflow/data/
