CREATE INDEX IF NOT EXISTS idx_fact_weather_city_id
    ON fact_weather (city_id);

CREATE INDEX IF NOT EXISTS idx_fact_weather_date_id
    ON fact_weather (date_id);
