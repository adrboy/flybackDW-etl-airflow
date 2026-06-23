FROM apache/airflow:2.9.3

USER root

RUN apt-get update \
    && apt-get install -y curl gnupg2 apt-transport-https \
    && apt-get remove -y unixodbc-dev libodbc1 unixodbc libodbcinst2 \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    polars connectorx sqlalchemy pymysql pyarrow \
    apache-airflow-providers-mysql \
    pyodbc apache-airflow-providers-odbc \
    pymssql==2.2.11 apache-airflow-providers-microsoft-mssql
