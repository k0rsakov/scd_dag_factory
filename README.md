# Таблица-справочник – генератор DAG? А что так можно было?

Фабрика DAG через SCD-таблицу с конфигурациями.

Статья на [habr](https://habr.com/ru/articles/756978/)

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

___

Если вам необходима консультация/менторство/мок-собеседование и другие вопросы по дата-инженерии, то вы можете
обращаться ко мне. Все контакты указаны по
[ссылке](https://www.notion.so/korsak0v/Data-Engineer-185c62fdf79345eb9da9928356884ea0).

