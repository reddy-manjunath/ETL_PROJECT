# 🌦 Weather API ETL + Data Warehouse Project

A production-style, end-to-end **Weather ETL pipeline** built using Python and PostgreSQL.
This project fetches real-time weather data from the OpenWeather API, validates and transforms it, and loads it into a Star Schema data warehouse optimized for analytical queries.

---

## 🚀 Project Overview

This project demonstrates core Data Engineering concepts:

* API Data Extraction
* JSON Transformation
* Data Validation
* Star Schema Data Modeling
* Transactional Database Loading
* Index-Based Performance Optimization
* Structured Logging
* Modular ETL Architecture

The pipeline is designed to simulate production-level data workflows.

---

## 🏗 Architecture

```
OpenWeather API
        ↓
Extract Layer
        ↓
Transform Layer
        ↓
Validation Layer
        ↓
PostgreSQL Data Warehouse
        ↓
Indexed Analytical Queries
```

---

## 📂 Project Structure

```
weather-etl-project/
│
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── validate.py
│   └── load.py
│
├── sql/
│   ├── create_tables.sql
│   └── indexes.sql
│
├── logs/
│   └── etl.log
│
├── config.py
├── main.py
├── requirements.txt
└── README.md
```

---

## 🧱 Data Warehouse Design (Star Schema)

### ⭐ Fact Table

**fact_weather**

* weather_id (PK)
* city_id (FK)
* date_id (FK)
* temperature_celsius
* humidity
* pressure
* wind_speed

### 📦 Dimension Tables

**dim_city**

* city_id (PK)
* city_name
* country
* latitude
* longitude

**dim_date**

* date_id (PK)
* full_timestamp
* year
* month
* day
* hour

---

## 🔄 ETL Workflow

### 1️⃣ Extract

* Fetches real-time weather data using OpenWeather API.
* Handles HTTP errors.
* Logs extraction status.

### 2️⃣ Transform

* Converts Kelvin to Celsius (or uses metric units).
* Converts UNIX timestamps to datetime.
* Structures JSON into dimension and fact dataframes.

### 3️⃣ Validate

* Temperature range check (-50 to 60°C).
* Humidity range check (0–100%).
* Null city check.
* Duplicate entry check.

### 4️⃣ Load

* Inserts dimension tables first.
* Inserts fact table using foreign keys.
* Uses transactions with commit/rollback.
* Handles duplicate conflicts gracefully.

---

## ⚡ Performance Optimization

Indexes created:

* `fact_weather(city_id)`
* `fact_weather(date_id)`

Example performance test:

```sql
EXPLAIN ANALYZE
SELECT AVG(temperature_celsius)
FROM fact_weather
WHERE city_id = 1;
```

Indexing improves query speed for analytical workloads.

---

## 📊 Sample Data Loaded

| City      | Temp (°C) | Humidity (%) | Pressure (hPa) | Wind (m/s) |
| --------- | --------- | ------------ | -------------- | ---------- |
| Delhi     | 24.05     | 46           | 1009           | 2.57       |
| Mumbai    | 28.99     | 58           | 1010           | 3.09       |
| Bengaluru | 27.39     | 37           | 1012           | 5.81       |
| Chennai   | 27.77     | 73           | 1012           | 3.09       |
| Kolkata   | 26.97     | 36           | 1010           | 2.06       |

---

## ⏱ Pipeline Execution Metrics

* Status: ✅ Successful
* Execution Time: ~5–6 seconds
* Records Inserted: 15 (5 cities + 5 dates + 5 facts)
* Validations Passed: 4/4
* Indexes Created: 2

---

## 🛠 Setup Instructions

### 1️⃣ Clone Repository

```
git clone <your-repo-url>
cd weather-etl-project
```

### 2️⃣ Install Dependencies

```
pip install -r requirements.txt
```

### 3️⃣ Set Environment Variables

**Windows (PowerShell):**

```
$env:OPENWEATHER_API_KEY="your_api_key"
$env:DB_NAME="weather_dw"
$env:DB_USER="postgres"
$env:DB_PASSWORD="your_password"
```

**Mac/Linux:**

```
export OPENWEATHER_API_KEY="your_api_key"
export DB_NAME="weather_dw"
export DB_USER="postgres"
export DB_PASSWORD="your_password"
```

---

### 4️⃣ Create Tables

Run:

```
psql -U postgres -d weather_dw -f sql/create_tables.sql
```

---

### 5️⃣ Run Pipeline

```
python main.py
```

Optional scheduled mode:

```
python main.py --schedule
```

---

## 📜 Logging

Logs are stored in:

```
logs/etl.log
```

Format:

```
timestamp - level - module - message
```

Example:

```
2026-03-03 19:39:54 - INFO - load - Inserted 5 rows into fact_weather
```

---

## 🔐 Security

* No credentials are hardcoded.
* API keys and DB passwords must be set via environment variables.
* `.gitignore` excludes logs and sensitive files.

---

## 💼 Resume Highlight

Designed and implemented a production-style Weather API ETL pipeline using Python and PostgreSQL, built a star-schema data warehouse, implemented data validation checks, optimized analytical queries using indexing, and integrated structured logging with error handling.

---

## 🚀 Future Improvements

* Docker containerization
* AWS RDS deployment
* S3 raw data storage
* CI/CD integration
* Data quality monitoring dashboard
* Airflow orchestration

---

## 👨‍💻 Author

Manjunath
B.Tech CSE (Data Science & Machine Learning)

---

⭐ If you found this project interesting, feel free to star the repository!
