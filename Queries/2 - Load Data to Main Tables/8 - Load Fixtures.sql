/******************************************************************************************/
/* Create fixtures table                                                                  */
/******************************************************************************************/
CREATE SCHEMA IF NOT EXISTS fixtures;

CREATE TABLE IF NOT EXISTS fixtures.fixtures_data (
    id bigserial,
    vessel_id integer,
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
    freight_unit char(7),
    vessel_owner varchar(30),
    vessel_type varchar(20),
    vessel_category char(7),
	country varchar(255),
    CONSTRAINT fixtures_data_PKEY PRIMARY KEY (id)
);


/******************************************************************************************/
/* Insert fixtures data                                                                   */
/******************************************************************************************/

--truncate table fixtures.fixtures_data

INSERT INTO fixtures.fixtures_data (vessel_id, fixture_date, vessel_name, vessel_built, vessel_cubic_capacity, vessel_dwt, vessel_hull_type, cargo_qty, cargo, cargo_type
    ,charterer, laycan_from, laycan_to, port_load, port_delivery, port_discharge, port_redelivery, freight_rate, freight_unit, vessel_owner, vessel_type, vessel_category)
SELECT 
    null as "vessel_id",fixture_date,vessel_name,vessel_built,vessel_cubic_capacity,vessel_dwt,vessel_hull_type,cargo_qty,cargo,cargo_type,charterer,laycan_from,laycan_to
	    ,port_load,port_delivery,port_discharge,port_redelivery,freight_rate,freight_unit,vessel_owner,vessel_type,vessel_category
FROM (
	SELECT fixture_date,vessel_name,vessel_built,vessel_cubic_capacity,vessel_dwt,vessel_hull_type,cargo_qty,cargo,cargo_type,charterer,laycan_from,laycan_to
	    ,port_load,port_delivery,port_discharge,port_redelivery,freight_rate,freight_unit,vessel_owner,vessel_type,vessel_category
	FROM import.imported_fixtures_data_2019 
	UNION
	SELECT fixture_date,vessel_name,vessel_built,vessel_cubic_capacity,vessel_dwt,vessel_hull_type,cargo_qty,cargo,cargo_type,charterer,laycan_from,laycan_to
	    ,port_load,port_delivery,port_discharge,port_redelivery,freight_rate,freight_unit,vessel_owner,vessel_type,vessel_category
	FROM import.imported_fixtures_data_2020 
	UNION
	SELECT fixture_date,vessel_name,vessel_built,vessel_cubic_capacity,vessel_dwt,vessel_hull_type,cargo_qty,cargo,cargo_type,charterer,laycan_from,laycan_to
	    ,port_load,port_delivery,port_discharge,port_redelivery,freight_rate,freight_unit,vessel_owner,vessel_type,vessel_category
	FROM import.imported_fixtures_data_2021
	UNION
	SELECT fixture_date,vessel_name,vessel_built,vessel_cubic_capacity,vessel_dwt,vessel_hull_type,cargo_qty,cargo,cargo_type,charterer,laycan_from,laycan_to
	    ,port_load,port_delivery,port_discharge,port_redelivery,freight_rate,freight_unit,vessel_owner,vessel_type,vessel_category
	FROM import.imported_fixtures_data_2022
);


ALTER TABLE fixtures.fixtures_data
ALTER COLUMN laycan_from type timestamp;

ALTER TABLE fixtures.fixtures_data
ALTER COLUMN laycan_to type timestamp;

UPDATE fixtures.fixtures_data
SET laycan_to = laycan_to + interval '23:59:59';




