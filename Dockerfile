FROM apache/airflow:{{AIRFLOW_VERSION}}-python{{PYTHON_VERSION}}

# Switch to root to install system dependencies
USER root

# Install system dependencies for Oracle and PostgreSQL
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        gcc \
        g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set Airflow configuration
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
ENV AIRFLOW__CORE__BASE_LOG_FOLDER=/opt/airflow/logs
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__WEBSERVER__RBAC=True

# Create necessary directories with proper permissions
USER root
RUN mkdir -p /opt/airflow/dags/manual_dags \
    && mkdir -p /opt/airflow/logs \
    && mkdir -p /opt/airflow/plugins \
    && mkdir -p /opt/airflow/include \
    && mkdir -p /opt/airflow/config \
    && chown -R airflow:root /opt/airflow/dags \
    && chown -R airflow:root /opt/airflow/logs \
    && chown -R airflow:root /opt/airflow/plugins \
    && chown -R airflow:root /opt/airflow/include \
    && chown -R airflow:root /opt/airflow/config \
    && chmod -R 775 /opt/airflow/dags \
    && chmod -R 775 /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/plugins \
    && chmod -R 775 /opt/airflow/include \
    && chmod -R 775 /opt/airflow/config

# Copy project files
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
COPY --chown=airflow:root include/ /opt/airflow/include/
COPY --chown=airflow:root config/ /opt/airflow/config/

# Switch back to airflow user for runtime
USER airflow

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"

# Default command
CMD ["webserver"]
