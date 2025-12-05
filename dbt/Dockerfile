FROM python:3.11-slim

WORKDIR /dbt

RUN pip install --no-cache-dir dbt-postgres==1.7.4

COPY . .

# Entrypoint для запуска dbt команд
ENTRYPOINT ["dbt"]
CMD ["run"]
