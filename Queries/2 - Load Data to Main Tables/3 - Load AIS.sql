CREATE TABLE IF NOT EXISTS ais.ais (
    id bigserial
    ,vessel_id INTEGER
    ,latitude NUMERIC(8,6)
    ,longitude NUMERIC(9,6)
    ,geom GEOMETRY(POINT, 4326)
    ,speed_over_ground NUMERIC(4,1)
    ,course_over_ground NUMERIC(4,1)
    ,heading SMALLINT
    ,status SMALLINT
    ,draft NUMERIC(3,1)
    ,ts timestamp
);
--TRUNCATE ais.ais RESTART IDENTITY CASCADE;

INSERT INTO ais.ais (vessel_id, latitude, longitude, geom, speed_over_ground, course_over_ground, heading, status, draft, ts)
SELECT 
    s.id
    ,lat
    ,lon
    ,ST_SetSRID (ST_MakePoint (lon , lat) ,4326) 
    ,sog
    ,cog
    ,heading
    ,status
    ,CASE WHEN draft = 'NaN' then null else draft end as draft
    ,cast(to_timestamp(basedatetime_seconds) as timestamp)
FROM 
	(SELECT mmsi, lat, lon, sog, cog, heading, status, draft, vesselname, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo, vessel_category, basedatetime_seconds FROM import.imported_ais_data_2019
	UNION
	SELECT mmsi, lat, lon, sog, cog, heading, status, draft, vesselname, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo, vessel_category, basedatetime_seconds FROM import.imported_ais_data_2020
	UNION
	SELECT mmsi, lat, lon, sog, cog, heading, status, draft, vesselname, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo, vessel_category, basedatetime_seconds FROM import.imported_ais_data_2021
	UNION
	SELECT mmsi, lat, lon, sog, cog, heading, status, draft, vesselname, CASE WHEN LENGTH(REPLACE(imo, 'IMO', '')) > 7 THEN LEFT(REPLACE(imo, 'IMO', ''), 7) ELSE REPLACE(imo, 'IMO', '') END as imo, vessel_category, basedatetime_seconds FROM import.imported_ais_data_2022) a
LEFT JOIN ais.vessel s on s.vessel_name = a.vesselname 
	AND s.mmsi = a.mmsi
	AND s.imo = a.imo
	AND s.vessel_category = a.vessel_category;


ALTER TABLE ais.ais
ADD CONSTRAINT fk_ais_vessel_id
FOREIGN KEY (vessel_id) REFERENCES ais.vessel(id)
	ON DELETE CASCADE;

CREATE INDEX idx_ais_vessel_id ON ais.ais(vessel_id);

CREATE INDEX idx_ais_geom ON ais.ais
USING GiST (geom);

