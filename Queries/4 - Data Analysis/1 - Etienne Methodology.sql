/*********************************************************************************************************/
/***************** Create the voronoi tessellation for each port, based on port location *****************/
/*********************************************************************************************************/
CREATE TABLE data_analysis.ports_voronoi AS
    SELECT p.id as port_id , p.port_name, p.country, p.geom, voronoi_zone
    FROM context_data.ports p
    LEFT JOIN (
        SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))).geom as voronoi_zone
        FROM context_data.ports 
		) as vp ON (ST_Within( p.geom , vp.voronoi_zone)) ;


CREATE INDEX idx_ports_voronoi_zone ON data_analysis.ports_voronoi
	USING gist(voronoi_zone);


/*********************************************************************************************************/
/************************** Create the voronoi tessellation for each country *****************************/
/*********************************************************************************************************/
CREATE TABLE data_analysis.countries_voronoi AS
    SELECT c.id as country_id , c.country, c.geom, voronoi_zone
    FROM context_data.countries c
    LEFT JOIN (
        SELECT (ST_Dump(ST_VoronoiPolygons(ST_Collect(geom)))).geom as voronoi_zone
        FROM context_data.countries 
		) as vp ON (ST_Within( c.geom , vp.voronoi_zone)) ;


CREATE INDEX idx_countries_voronoi_zone ON data_analysis.countries_voronoi
	USING gist(voronoi_zone);


/*********************************************************************************************************/
/**************** Create stop positions and match those to the port, using the voronoi *******************/
/*********************************************************************************************************/
CREATE TABLE data_analysis.non_moving_positions AS
--TRUNCATE TABLE data_analysis.non_moving_positions RESTART IDENTITY;
--INSERT INTO data_analysis.non_moving_positions
	SELECT zero.id, zero.vessel_id, zero.ts, zero.geom, pv.port_id, pv.port_name
		,ST_DistanceSphere (zero.geom, pv.geom) as port_dist
	FROM (
		SELECT *
		FROM ais.ais
		WHERE speed_kn=0
		) as zero
	LEFT JOIN data_analysis.ports_voronoi pv ON ST_Within(zero.geom, pv.voronoi_zone);  --ships in voronoi areas

CREATE INDEX idx_non_moving_positions_geom ON data_analysis.non_moving_positions
	USING gist ( geom ) ;

CREATE INDEX idx_non_moving_positions_port_id ON data_analysis.non_moving_positions
	USING btree ( port_id ) ;

CREATE INDEX idx_non_moving_positions_ts ON data_analysis.non_moving_positions
	USING btree (ts);	


/*********************************************************************************************************/
/********************** Create a table with successive position pairs (segments) *************************/
/*********************************************************************************************************/
CREATE INDEX idx_ais_vesselid_ts ON ais.ais
	USING btree (vessel_id, ts) ;

CREATE TABLE data_analysis.segments AS
--TRUNCATE TABLE data_analysis.segments RESTART IDENTITY;
--INSERT INTO data_analysis.segments
	SELECT vessel_id
		,ts1 ,ts2  --starting and ending timestamps
		,speed1, speed2  --starting and ending speeds
		,p1 ,p2  --starting and ending points
		,st_makeline(p1,p2) as segment  --line segment connecting the two points
		,ST_DistanceSphere (p1 ,p2) as distance  --distance between points
		,extract(epoch FROM (ts2-ts1)) as duration_s  --timestamp in seconds
		,CASE WHEN extract (epoch FROM (ts2-ts1)) <> 0 THEN (ST_DistanceSphere (p1,p2) / extract (epoch FROM (ts2-ts1))) END as speed_m_s  --speed in m/s
	FROM (
		SELECT vessel_id
			,LEAD(vessel_id) OVER (ORDER BY vessel_id, ts) as vessel_id2  --next vessel_id
			,ts as ts1  --starting time
			,LEAD(ts) OVER (ORDER BY vessel_id, ts) as ts2  --ending time
			,speed_kn as speed1  --initial speed
			,LEAD(speed_kn) OVER (ORDER BY vessel_id, ts) as speed2  --final speed
			,geom as p1  --initial poing
			,LEAD(geom) OVER (ORDER BY vessel_id, ts) as p2  --final point
		FROM ais.ais) as q1
		WHERE vessel_id=vessel_id2;  --filtering out different vessel_id

CREATE INDEX idx_segments_speed ON data_analysis.segments
	USING btree (speed1, speed2);


/*********************************************************************************************************/
/********** Here we create potential stop beginnings and endings. The set threshold is 0.1 knots  ********/
/*********************************************************************************************************/
CREATE TABLE data_analysis.stop_begin AS
--TRUNCATE TABLE data_analysis.stop_begin RESTART IDENTITY;
--INSERT INTO data_analysis.stop_begin
	SELECT vessel_id, ts2 as ts_begin
	FROM data_analysis.segments
	WHERE speed1 >0.1 AND speed2 <=0.1;

CREATE INDEX idx_stop_begin_vessel_id_ts ON data_analysis.stop_begin
	USING btree (vessel_id, ts_begin);

CREATE TABLE data_analysis.stop_end AS
--TRUNCATE TABLE data_analysis.stop_end RESTART IDENTITY;
--INSERT INTO data_analysis.stop_end
	SELECT vessel_id , ts1 as ts_end
	FROM data_analysis.segments
	WHERE speed1 <=0.1 AND speed2 >0.1;

CREATE INDEX idx_stop_end_vessel_id_ts ON data_analysis.stop_end
	USING btree (vessel_id , ts_end ) ;


/*********************************************************************************************************/
/**************** Here we create a stops table, by coupling the stop beginnings and endings **************/
/*********************************************************************************************************/
CREATE TABLE data_analysis.stops AS
--TRUNCATE TABLE data_analysis.stops RESTART IDENTITY;
--INSERT INTO data_analysis.stops
	SELECT vessel_id, ts_begin, ts_end, extract (epoch FROM (ts_end - ts_begin)) as duration_s
	FROM data_analysis.stop_begin 
	INNER JOIN LATERAL (  --keep only stops that have an end
		SELECT ts_end
		FROM data_analysis.stop_end
		WHERE stop_begin.vessel_id=stop_end.vessel_id 
			AND ts_begin <= ts_end  --stop should follow the beginning
		ORDER BY ts_end LIMIT 1  --select only the first stop
		) AS q2 ON (true);

ALTER TABLE data_analysis.stops ADD COLUMN id bigserial;

ALTER TABLE data_analysis.stops ADD CONSTRAINT pk_stops_id PRIMARY KEY ("id");

ALTER TABLE data_analysis.stops ADD COLUMN centr geometry(Point,4326);
ALTER TABLE data_analysis.stops ADD COLUMN nb_pos integer;


-- Compute the centroid and number of positions
UPDATE data_analysis.stops
SET (centr, nb_pos) = (
	SELECT st_centroid(st_collect(geom)) --centroid of a multipoint
		,count(*) as nb  --number of points
	FROM ais.ais
	WHERE vessel_id = stops.vessel_id 
		AND ts >= stops.ts_begin 
		AND ts <= stops.ts_end
	) ;


-- Compute indicators on the dispersion of the vessel’s positions around the centroid of the cluster 
ALTER TABLE data_analysis.stops ADD COLUMN avg_dist_centroid numeric ;
ALTER TABLE data_analysis.stops ADD COLUMN max_dist_centroid numeric ;

UPDATE data_analysis.stops
SET (avg_dist_centroid, max_dist_centroid) = (
	SELECT avg(d) ,max (d)
	FROM (
		SELECT ST_DistanceSphere(centr, geom) as d  --distance to centroid
		FROM ais.ais
		WHERE vessel_id = stops.vessel_id 
			AND ts >=stops.ts_begin 
			AND ts <=stops.ts_end
	) as q1 
) ;
