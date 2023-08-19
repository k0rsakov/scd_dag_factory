# scd_dag_factory

Фабрика DAG через SCD-таблицу с конфигурациями.

## Установка Airflow через Docker

Для того чтобы развернуть Airflow, выполните команду:

```bash
docker-compose up -d
```

Airflow будет доступен по адресу – http://0.0.0.0:8080/home

- Login: `airflow` 
- Password: `airflow`

## Создания виртуального окружения

Для создания корректного виртуального окружения выполните следующие команды:

```bash
python3.11 -m venv venv
```

```bash
source venv/bin/activate 
```

```bash
pip install --upgrade pip
```

```bash
pip install -r requirements.txt
```
