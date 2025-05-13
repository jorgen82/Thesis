-- Create the Database
CREATE DATABASE thesis
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1253'
    LC_CTYPE = 'English_United States.1253'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


-- Create Schemas
CREATE SCHEMA IF NOT EXISTS ais;
CREATE SCHEMA IF NOT EXISTS context_data;
CREATE SCHEMA IF NOT EXISTS data_analysis;
CREATE SCHEMA IF NOT EXISTS fixtures;
CREATE SCHEMA IF NOT EXISTS import;


-- Enable PostGIS (make sure you already installed it)
CREATE EXTENSION postgis;
