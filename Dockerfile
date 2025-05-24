FROM apache/airflow:2.10.0-python3.11

USER root

RUN apt-get update \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install poetry

ENV PATH="${PATH}:/home/airflow/.local/bin"

COPY --chown=airflow:root pyproject.toml poetry.lock* /opt/airflow/
COPY --chown=airflow:root src/ /opt/airflow/src/

WORKDIR /opt/airflow

# Отключаем poetry virtualenv, ставим зависимости, фиксируем SQLAlchemy
RUN poetry config virtualenvs.create false \
    && poetry install --no-root \
    && pip install --force-reinstall "SQLAlchemy==1.4.51"
