DROP DATABASE IF EXISTS raingod;
CREATE DATABASE IF NOT EXISTS raingod DEFAULT CHARSET utf8mb4;

USE raingod;

DROP TABLE IF EXISTS Province;
CREATE TABLE IF NOT EXISTS Province
(
    province VARCHAR(50) NOT NULL
)
DEFAULT CHARSET = utf8mb4;

DROP TABLE IF EXISTS City;
CREATE TABLE IF NOT EXISTS City
(
    province VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL
)
DEFAULT CHARSET = utf8mb4;

DROP TABLE IF EXISTS RealTimeWeather;
CREATE TABLE IF NOT EXISTS RealTimeWeather
(
    province VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    publish_time VARCHAR(20),
    temperature VARCHAR(20),
    precipitation VARCHAR(20),
    wind_direction VARCHAR(50),
    wind_power VARCHAR(50),
    relative_humidity VARCHAR(20),
    sensible_temperature VARCHAR(20),
    aqi VARCHAR(20),
    comfort VARCHAR(50)
)
DEFAULT CHARSET = utf8mb4;

DROP TABLE IF EXISTS DateWeather;
CREATE TABLE IF NOT EXISTS DateWeather
(
    province VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    date_time DATE NOT NULL,
    highest_temperature VARCHAR(20),
    lowest_temperature VARCHAR(20),
    weather_description1 VARCHAR(20),
    weather_description2 VARCHAR(20),
    wind_direction1 VARCHAR(50),
    wind_direction2 VARCHAR(50),
    wind_level1 VARCHAR(50),
    wind_level2 VARCHAR(50)
)
DEFAULT CHARSET = utf8mb4;
