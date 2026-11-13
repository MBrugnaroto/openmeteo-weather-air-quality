# Weather and Air Quality Monitor

## Editing DAGs in the Astro container

The local Astro image runs Airflow as the non-root `astro` user, configured
with UID `1000` to match the workspace owner. The image also assigns the
copied project files to that user, so files such as `requirements.txt` and
host-mounted `dags/` files are writable from VS Code. If your host uses a
different UID, change the `AIRFLOW_UID` default in the `Dockerfile` to match
it, then rebuild/restart the local Astro environment with `astro dev stop`
followed by `astro dev start`.

The ingestion DAG reads `data/input/cities.json` from the project root. The
`Dockerfile` copies `data/` into the Airflow image; rebuild/restart Astro after
changing files in that directory so the container receives the updated input.

If an existing Astro environment still has log files owned by the previous
`astro` UID (`50000`), repair its persistent log volume once and restart the
DAG processor:

```sh
docker exec --user root <scheduler-container> chown -R 1000:50000 /usr/local/airflow/logs
docker restart <dag-processor-container>
```
