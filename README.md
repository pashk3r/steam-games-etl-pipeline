# **Steam Games ETL Pipeline**

ETL-пайплайн для извлечения, трансформации и загрузки данных об играх Steam из CSV в PostgreSQL с использованием Apache Spark (PySpark)

## **Описание**

Пайплайн обрабатывает датасет игр Steam, полученный с Kaggle, и выполняет его очистку и структурирование перед загрузкой в базу данных:

Источник данных – CSV-файл `games.csv` 

Фреймворк обработки – Apache Spark (PySpark), все этапы ETL выполняются в единой Spark-сессии

Хранилище – PostgreSQL, загрузка осуществляется через Spark JDBC

## **Архитектура**

Проект построен по классическому паттерну Extract → Transform → Load:

```
           CSV
            │
            ▼
    Extract (PySpark)
            │
            ▼
    Transform (PySpark)
            │
            ▼
    Load (Spark JDBC)
            │
            ▼
        PostgreSQL
```

А так выглядит поток данных:
![data_flow.png](https://private-user-images.githubusercontent.com/209692999/642854914-1b9e3a8a-0a77-4783-a577-44d50103c0ee.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3ODc5Mzg1NzIsIm5iZiI6MTc4NzkzODI3MiwicGF0aCI6Ii8yMDk2OTI5OTkvNjQyODU0OTE0LTFiOWUzYThhLTBhNzctNDc4My1hNTc3LTQ0ZDUwMTAzYzBlZS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjYwODI4JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI2MDgyOFQxNzMxMTJaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT1mOGJhNjZlYWMzOTEyNjFjMGNhZmUyZjlkYmZmNmYwMjUxZWVkODQzYjI2ZDdmYmIyNDdkNzU0ZTRjY2Q4OGEyJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCZyZXNwb25zZS1jb250ZW50LXR5cGU9aW1hZ2UlMkZwbmcifQ.GiB-p6_Z7635gKc-hrf3W_DnOqJGuHFOZXYo3U5e9xQ)

## **Используемые технологии**

- **Python 3.12**
- **Apache Spark 3.5.1** (PySpark)
- **PostgreSQL**
- **python-dotenv**


## **Структура проекта**

```
steam-games-etl/
├── src/
│   ├── config.py
│   ├── exceptions.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── data/
│   └── raw/
│       └── games.csv
├── logs/
│   └── etl.log
├── main.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```


## **Как запустить?**

### **0. Предварительные требования**

- **Python 3.12**
- **Java 17**
- **PostgreSQL**
- **Для запуска пайплайна файл `games.csv` нужно скачать с [Kaggle](https://www.kaggle.com/datasets/fronkongames/steam-games-dataset) и положить его по пути `data/raw/`. Также нужно поставить запятую между `Discount` и `DLC count` в первой строчке файла**

### **1. Клонирование репозитория**

```bash
git clone https://github.com/pashk3r/steam-games-etl-pipeline.git
cd steam-games-etl-pipeline
```

### **2. Установка виртуального окружения и библиотек**

```bash
python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```


### **3. Настройка .env**

Создайте файл .env в корне проекта. Потом скопировать код из `.env.example` и вставить его в `.env`.

Или же можно выполнить команду:

```bash
cp .env.example .env
```
После нужно добавить пароль от суперпользователя `postgres` в `.env`


### **4. Создание базы данных**

Перед запуском пайплайна должен быть установлен PostgreSQL. После зайти в pgAdmin и создать базу данных с именем `steam_games`


### **5. Запуск пайплайна**

После создания базы данных и установки всех зависимостей, для запуска пайплайна в папке проекта нужно выполнить одну команду:

```bash
python main.py
```

Обновляем базу данных с помощью ПКМ + `Refresh` в pgAdmin и далее можем работать с данными!


## **Трансформация данных**

Все изменения в датасете выполняются с помощью PySpark. В таблице ниже показаны какие столбцы были изменены и какие изменения там проводились: 

| Исходный столбец | Изменение |
|---|---|
| `Name` | Удаляются строки, в которых значение отсутствует или содержит пустую строку |
| `Release date` | Разбивается на `Release day`, `Release month`, `Release year` |
| `Estimated owners` | Разбивается на `Estimated owners min` и `Estimated owners max`, значения приводятся к `int` |
| `Movies` | Столбец полностью удаляется |
| `Screenshots` | Строка преобразуется в массив |
| `Tags` | Строка преобразуется в массив |
| `Genres` | Строка преобразуется в массив |
| `Categories` | Строка преобразуется в массив |
| `Developers` | Строка преобразуется в массив |
| `Publishers` | Строка преобразуется в массив |
| `Supported languages` | Удаляются `[]` и `'`, после чего строка преобразуется в массив |
| `Full audio languages` | Удаляются `[]` и `'`, после чего строка преобразуется в массив |


## **Логирование**

Логи пишутся в:
- консоль
- файл `logs/etl.log`

Уровень логирования настраивается в `src/config.py` (по умолчанию стоит `INFO`)


## **TO-DO**

- [x] Заменить pandas на Spark


## **Автор**

**[pashk3r](https://github.com/pashk3r)**