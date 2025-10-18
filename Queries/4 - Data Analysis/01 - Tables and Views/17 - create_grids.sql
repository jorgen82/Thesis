/* 
	Create a grid of Hexagons for the Ocean. 
	We will use this to map the Tracks and AIS information to specific Hexagons
*/

-- Hexagon Grid of 6,2km (Area 99,87 km2) - We will use the GridVoyage geometry - To be used to find coast to coast track over Panama Canal and Cape Horn
CREATE TABLE context_data.grid_voyage(id serial, geom geometry);

WITH hex_grid AS (  --Get the GridVoyage geom from the oceans table and use the ST_HexagonGrid to create the hexagons
    SELECT ST_Transform((ST_HexagonGrid(6200, ST_Transform(geometry, 3857))).geom, 4326) AS geom
    FROM context_data.oceans
    WHERE featurecla = 'GridVoyage'
)
INSERT INTO context_data.grid_voyage (geom) 
SELECT geom AS geom
FROM hex_grid
WHERE ST_Intersects(geom, (SELECT geometry FROM context_data.oceans WHERE featurecla = 'GridVoyage')) --Only hexagons which intersects the GridVoyage geometry
    AND NOT ST_Crosses(ST_Boundary((SELECT geometry FROM context_data.oceans WHERE featurecla = 'GridVoyage')),geom); --and do not cross its boundaries

--Create the necessary indexes
CREATE INDEX idx_grid_voyage_id ON context_data.grid_voyage (id);
CLUSTER context_data.grid_voyage USING idx_grid_voyage_id;
ANALYZE context_data.grid_voyage;

CREATE INDEX idx_grid_voyage_geom ON context_data.grid_voyage USING GIST (geom);

--Add the centroid of each hexagon and create an index on it
ALTER TABLE context_data.grid_voyage ADD centr GEOMETRY(POINT, 4326);

UPDATE context_data.grid_voyage
SET centr = ST_Centroid(geom);

CREATE INDEX idx_grid_voyage_centr ON context_data.grid_voyage USING GIST (centr);


/* THE BELOW QUERIES CAN SERVE DIFFERENT GRID TYPES AND KEPT FOR REFERENCE. USE THOSE IF NEEDED */
/* Hexagon Grid of 6,2km (Area 99,87 km2) - We will use the GridOcean geometry - To be used to find paths between a position and the port (calculating potential ETA or other relevant metrics)
CREATE TABLE context_data.grid_hex(id serial, geom geometry);

WITH hex_grid AS (
    SELECT ST_Transform((ST_HexagonGrid(6200, ST_Transform(geometry, 3857))).geom, 4326) AS geom
    FROM context_data.oceans
    WHERE featurecla = 'GridOcean'
)
INSERT INTO context_data.grid_hex (geom)
SELECT geom AS geom
FROM hex_grid
WHERE ST_Intersects(geom, (SELECT geometry FROM context_data.oceans WHERE featurecla = 'GridOcean'))
    AND NOT ST_Crosses(ST_Boundary((SELECT geometry FROM context_data.oceans WHERE featurecla = 'GridOcean')),geom);

CREATE INDEX idx_grid_hex_id ON context_data.grid_hex (id);
CLUSTER context_data.grid_hex USING idx_grid_hex_id;
ANALYZE context_data.grid_hex;

CREATE INDEX idx_grid_hex_geom ON context_data.grid_hex USING GIST (geom);

ALTER TABLE context_data.grid_hex ADD centr GEOMETRY(POINT, 4326);

UPDATE context_data.grid_hex
SET centr = ST_Centroid(geom);

CREATE INDEX idx_grid_hex_centr ON context_data.grid_hex USING GIST (centr);
*/

/* Squared Grid of 10km (Area 100 km2) - We will use the GridOcean geometry - To be used to find paths between a position and the port (calculating potential ETA or other relevant metrics)
CREATE TABLE context_data.grid_square(id serial, geom geometry);

WITH square_grid AS (
    SELECT ST_Transform((ST_SquareGrid(10000, ST_Transform(geometry, 3857))).geom, 4326) AS geom
    FROM context_data.oceans
    WHERE featurecla = 'GridOcean'
)
INSERT INTO context_data.grid_square (geom)
SELECT geom AS geom
FROM square_grid
WHERE ST_Intersects(geom, (SELECT geometry FROM context_data.oceans WHERE featurecla = 'GridOcean'))
    AND NOT ST_Crosses(ST_Boundary((SELECT geometry FROM context_data.oceans WHERE featurecla = 'GridOcean')),geom);

CREATE INDEX idx_grid_square_geom ON context_data.grid_square USING GIST (geom);
CREATE INDEX idx_grid_square_id ON context_data.grid_square (id);
CLUSTER context_data.grid_square USING idx_grid_square_id;
ANALYZE context_data.grid_square;

ALTER TABLE context_data.grid_square ADD centr GEOMETRY(POINT, 4326);

UPDATE context_data.grid_square
SET centr = ST_Centroid(geom);

CREATE INDEX idx_grid_square_centr ON context_data.grid_square USING GIST (centr);
*/


/* Create Grid Network based on the Hexagons or Squares Created above */

/* Create Hex Network
CREATE TABLE context_data.grid_hex_network AS
WITH grid AS (
  SELECT id, ST_Transform(geom, 3857) AS geom, ST_Centroid(ST_Transform(geom, 3857)) AS centr
  FROM context_data.grid_square
),
edges AS (
    SELECT
        v1.id AS source_grid_id,
        v2.id AS target_grid_id,
        ST_MakeLine(
            ST_Transform(v1.centr, 4326),
            ST_Transform(v2.centr, 4326)
            ) AS track,
        ST_Distance(v1.centr, v2.centr) AS cost
    FROM grid v1
    INNER JOIN grid v2 ON v1.id < v2.id
        AND ST_DWithin(v1.centr, v2.centr, 11500)
)
SELECT * FROM edges;

ALTER TABLE context_data.grid_hex_network ADD id BIGSERIAL PRIMARY KEY;

CREATE INDEX idx_grid_hex_network_track ON context_data.grid_hex_network USING GIST (track);
CREATE INDEX idx_grid_hex_network_source ON context_data.grid_hex_network(source_grid_id);
CREATE INDEX idx_grid_hex_network_target ON context_data.grid_hex_network(target_grid_id);
*/

/* Create Square Network
CREATE TABLE context_data.grid_square_network AS
WITH grid AS (
  SELECT id, ST_Transform(geom, 3857) AS geom, ST_Centroid(ST_Transform(geom, 3857)) AS centr
  FROM context_data.grid_square
),
edges AS (
    SELECT
        v1.id AS source_grid_id,
        v2.id AS target_grid_id,
        ST_MakeLine(
            ST_Transform(v1.centr, 4326),
            ST_Transform(v2.centr, 4326)
            ) AS track,
        ST_Distance(v1.centr, v2.centr) AS cost
    FROM grid v1
    INNER JOIN grid v2 ON v1.id < v2.id
        AND ST_DWithin(v1.centr, v2.centr, 11500)
)
SELECT * FROM edges;

ALTER TABLE context_data.grid_square_network ADD id BIGSERIAL PRIMARY KEY;

CREATE INDEX idx_grid_square_network_track ON context_data.grid_square_network USING GIST (track);
CREATE INDEX idx_grid_square_network_source ON context_data.grid_square_network(source_grid_id);
CREATE INDEX idx_grid_square_network_target ON context_data.grid_square_network(target_grid_id);

*/



