FROM apache/airflow:3.1.6

USER root

# Copy data
COPY /data/codes /opt/airflow/data/codes
COPY /data/raw /opt/airflow/data/raw

# Make airflow user the owner
RUN chown -R airflow: /opt/airflow/data

# Install project wheel
COPY dist/*.whl /tmp/
RUN uv pip install /tmp/*.whl && rm /tmp/*.whl

# Switch back to airflow user
USER airflow

ENV PROJECT_DATA_DIR=/opt/airflow/data
