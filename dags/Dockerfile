# Built from the repo root: docker build -f dags/Dockerfile .
FROM apache/airflow:2.11.2-python3.11
# Test tools pinned to the Airflow 2.11.2 constraints so pip never moves Airflow's own dependencies
RUN pip install --no-cache-dir "apache-airflow==2.11.2" pytest==8.2.1 pytest-cov==7.0.0 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.11.txt"
# dags, quality, sql and tests are mounted under /opt/airflow, so `import quality` resolves from here
ENV PYTHONPATH=/opt/airflow
