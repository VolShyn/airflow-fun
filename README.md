# airflow_hw

Airflow homework: a daily OpenWeatherMap pipeline for five Ukrainian cities (Lviv,
Kyiv, Kharkiv, Odesa, Zhmerynka), evolved across three stages. Each stage has its own
reproduction guide:

- `hw1.md` — single DAG, stores into SQLite, backfills past dates via the timemachine endpoint.
- `hw2.md` — same pipeline on Postgres storage with the Celery executor and a Redis broker.
- `hw3.md` — split into a DAG factory plus a cross-DAG ingestion→processing pair connected
  by an Airflow 3 Asset, with file-based external storage and resume-from-failed-step.

Current code for HW3 lives under `dags/`. Start with the matching `hwN.md` for setup and
the connections/variables each stage needs.
