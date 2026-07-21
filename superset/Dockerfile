# superset/Dockerfile
FROM apache/superset:6.1.0

USER root
RUN pip install --no-cache-dir snowflake-sqlalchemy==1.11.0 snowflake-connector-python==3.15.0 psycopg2-binary
USER superset