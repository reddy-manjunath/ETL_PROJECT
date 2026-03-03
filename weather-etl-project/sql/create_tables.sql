CREATE TABLE IF NOT EXISTS dim_city (
    city_id     SERIAL PRIMARY KEY,
    city_name   VARCHAR(100) NOT NULL,
    country     VARCHAR(10)  NOT NULL,
    latitude    NUMERIC(9, 6) NOT NULL,
    longitude   NUMERIC(9, 6) NOT NULL,
    CONSTRAINT uq_city_name UNIQUE (city_name)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_timestamp  TIMESTAMP NOT NULL,
    year            INT NOT NULL,
    month           INT NOT NULL,
    day             INT NOT NULL,
    hour            INT NOT NULL,
    CONSTRAINT uq_timestamp UNIQUE (full_timestamp)
);

CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id          SERIAL PRIMARY KEY,
    city_id             INT NOT NULL REFERENCES dim_city(city_id),
    date_id             INT NOT NULL REFERENCES dim_date(date_id),
    temperature_celsius NUMERIC(5, 2) NOT NULL,
    humidity            INT NOT NULL,
    pressure            INT NOT NULL,
    wind_speed          NUMERIC(6, 2) NOT NULL,
    CONSTRAINT uq_city_date UNIQUE (city_id, date_id)
);
