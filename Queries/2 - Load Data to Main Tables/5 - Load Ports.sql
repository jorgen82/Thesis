/* Create Ports Table */
CREATE TABLE context_data.ports
	AS SELECT * FROM import.imported_ports_data;

/* Add id column and index */
ALTER TABLE context_data.ports 
RENAME COLUMN gid TO id;

ALTER TABLE context_data.ports
ADD CONSTRAINT pkey_ports_id PRIMARY KEY (id);

CREATE INDEX idx_ports_id ON context_data.ports (id);


/* Fixing issue with different port mapped to the same coordinates */
CREATE TEMP TABLE temp_concatenated_ports AS
SELECT 
    min(id) AS id,
    geom,
    STRING_AGG(port_name, ' / ') AS concatenated_names
FROM context_data.ports
GROUP BY geom;

UPDATE context_data.ports p
SET port_name = c.concatenated_names
FROM concatenated_data c
WHERE p.id = c.id;

DELETE FROM context_data.ports p
USING concatenated_data c
WHERE p.geom = c.geom
AND p.id <> c.id;

DROP TABLE temp_concatenated_ports;

/* Fix SOUTHWEST PASS wrong coordinates */
UPDATE context_data.ports 
SET latitude = 29.0317993, longitude = -89.3384171
where port_name = 'SOUTHWEST PASS';

UPDATE context_data.ports 
SET geom = ST_SetSRID (ST_MakePoint (longitude , latitude) ,4326)
where port_name = 'SOUTHWEST PASS';


/* Map port to a country */
ALTER TABLE context_data.ports
ADD COLUMN country_id integer;

UPDATE context_data.ports as p
SET country_id = c.id
FROM context_data.countries c
WHERE c.alpha2_code = p.country


-- remove duplicate port data

/*
DELETE FROM context_data.ports
USING (
	SELECT ROW_NUMBER () OVER (PARTITION BY geom ORDER BY index_no) "r"
		,id 
	FROM context_data.ports  
	WHERE geom IN (
		SELECT geom
		FROM context_data.ports 
		GROUP BY geom
		HAVING COUNT(id) > 1
		) 
	) del
WHERE del.r>1
	AND ports.gid = del.gid
*/
