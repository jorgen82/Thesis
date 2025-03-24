CREATE TABLE import.imported_fixtures_data_2022 (
    id bigserial,
    fixture_date date,
    vessel_name varchar(30),
    vessel_built smallint,
    vessel_cubic_capacity integer,
    vessel_dwt integer,
    vessel_hull_type char(6),
    cargo_qty integer,
    cargo char(5),
    cargo_type varchar(30),
    charterer varchar(30),
    laycan_from date,
    laycan_to date,
    port_load varchar(50),
    port_delivery varchar(50),
    port_discharge varchar(50),
    port_redelivery varchar(50),
    freight_rate numeric(10,2),
    freight_unit char(10),
    vessel_owner varchar(30),
    vessel_type varchar(20),
    vessel_category char(7),
    CONSTRAINT fixtures_data_PKEY_2022 PRIMARY KEY (id)
);

/* Change the create table to match the destination table and copy expressions to match the csv file to be imported */
COPY import.imported_fixtures_data_2022 (fixture_date, vessel_name, vessel_built, vessel_cubic_capacity, vessel_dwt, vessel_hull_type, cargo_qty, cargo, cargo_type, charterer, laycan_from, laycan_to, 
    port_load, port_delivery, port_discharge, port_redelivery, freight_rate, freight_unit, vessel_owner, vessel_type, vessel_category)
FROM 'J:/OneDrive/Documents/Personal Files/Academic/Demokritos/Thesis/Complex Event Recognition/Data/Fixtures/filtered_fixtures2022.csv'
DELIMITER ';' CSV HEADER ;

