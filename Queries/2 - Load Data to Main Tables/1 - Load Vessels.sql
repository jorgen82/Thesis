/* 
	This script will load the Vessel table (ais.vessel)
	The data for the vessel table are exported from the AIS data
*/

CREATE SCHEMA IF NOT EXISTS ais;

CREATE TABLE IF NOT EXISTS ais.vessel (
    id bigserial
    ,vessel_name varchar(30)
    ,mmsi int
    ,imo char(7)
    --,call_sign VARCHAR(10)
    ,length smallint
    ,width smallint
    --,transceiver_class char(1)
    ,vessel_category varchar(10)
);

/* Insert some of the Vessel Info to the Vessel table (only distinct records) */
INSERT INTO ais.vessel (vessel_name, mmsi, imo, vessel_category)
SELECT vesselname, mmsi, imo, vessel_category
    FROM (
    SELECT vesselname, mmsi, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
        ,vessel_category
    FROM import.imported_ais_data_2019
	UNION
	SELECT vesselname, mmsi, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
        ,vessel_category
    FROM import.imported_ais_data_2020
	UNION
	SELECT vesselname, mmsi, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
        ,vessel_category
    FROM import.imported_ais_data_2021
	UNION
	SELECT vesselname, mmsi, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
        ,vessel_category
    FROM import.imported_ais_data_2022
	)
GROUP BY vesselname, mmsi, imo, vessel_category;


/* Insert the vessel lenght and width, based on the values that appeared most on the AIS data */
WITH vessels AS (
	SELECT CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
		,vesselname, mmsi
		,CASE length WHEN 'NaN' then null ELSE length END as "length"
	    ,CASE width WHEN 'NaN' then null ELSE width END as "width"
	FROM import.imported_ais_data_2019
	UNION
	SELECT CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
		,vesselname, mmsi
		,CASE length WHEN 'NaN' then null ELSE length END as "length"
	    ,CASE width WHEN 'NaN' then null ELSE width END as "width"
	FROM import.imported_ais_data_2020
	UNION
	SELECT CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
		,vesselname, mmsi
		,CASE length WHEN 'NaN' then null ELSE length END as "length"
	    ,CASE width WHEN 'NaN' then null ELSE width END as "width"
	FROM import.imported_ais_data_2021
	UNION
	SELECT CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo
		,vesselname, mmsi
		,CASE length WHEN 'NaN' then null ELSE length END as "length"
	    ,CASE width WHEN 'NaN' then null ELSE width END as "width"
	FROM import.imported_ais_data_2022
),
vessel_agg_data AS (
	SELECT imo,vesselname, mmsi, length, width, count(*) as cnt
	FROM vessels
	GROUP BY imo,vesselname, mmsi, length, width
),
vessel_ranked_data AS (
	SELECT imo,vesselname, mmsi, length, width, ROW_NUMBER() OVER (PARTITION BY imo, vesselname, mmsi ORDER BY cnt DESC) AS rank
	FROM vessel_agg_data
)

UPDATE ais.vessel as V
SET length = vrd.length, width = vrd.width
FROM vessel_ranked_data as vrd
WHERE vrd.rank = 1
	AND v.vessel_name = vrd.vesselname
	AND v.mmsi = vrd.mmsi
	AND v.imo = vrd.imo;


ALTER TABLE ais.vessel
ADD CONSTRAINT pkey_vessel_id PRIMARY KEY (id);

CREATE INDEX idx_vessel_id
ON ais.vessel(id);

ALTER TABLE ais.vessel
ADD CONSTRAINT unq_vessel_all_columns UNIQUE (vessel_name, mmsi, imo, length, width, vessel_category);
