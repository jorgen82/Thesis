/*********************************************************************************************************/
/******************************************* Create Port Stops *******************************************/
/*********************************************************************************************************/

--TRUNCATE TABLE data_analysis.port_stops RESTART IDENTITY;
CREATE TABLE data_analysis.port_stops AS
WITH port_stops AS (
	SELECT *
	FROM (
		SELECT s.vessel_id, s.ts_begin, s.ts_end, s.duration_s/60/60 as "duration_h", s.nb_pos, s.centr
			, pv.port_id, pv.port_name
			,ST_DistanceSphere (s.centr, pv.geom) as port_dist
		FROM data_analysis.stops s
		LEFT JOIN data_analysis.ports_voronoi pv ON ST_Within(s.centr, pv.voronoi_zone)
		--WHERE nb_pos >= 10
	)
	WHERE port_dist <= 50000
)
,dur_filter AS (
	SELECT s.*
		,CASE 
			WHEN harborsize = 'V' AND harbortype like 'R%' THEN 5000
			WHEN harborsize = 'V' AND harbortype like 'C%' THEN 3000  --PORT ARANSAS  --was 2000 in filtered2
			WHEN harborsize = 'S' AND harbortype like 'R%' THEN 6000  -- COATZACOALCOS  -- was 4000 in filtered2
			WHEN harborsize = 'S' AND harbortype like 'C%' THEN 3000
			WHEN harborsize = 'M' AND harbortype like 'R%' THEN 8000 
			WHEN harborsize = 'M' AND harbortype like 'C%' THEN 3000  --VERACRUZ  -- was 2000 in filtered2
			WHEN harborsize = 'L' AND harbortype like 'R%' THEN 10000
			WHEN harborsize = 'L' AND harbortype like 'C%' THEN 4000  --GALVESTON      --was 8000 in filtered2
			ELSE 3000
		END as eps
	FROM port_stops s
	INNER JOIN context_data.ports p on p.id = s.port_id
	WHERE s.duration_h >= 10
		--AND s.port_id = 2950
	)
,clusters AS (
	SELECT eps, vessel_id, ts_begin, ts_end, duration_h, nb_pos, centr, port_id, port_name, port_dist--, id
		,ST_ClusterDBSCAN(ST_Transform(centr, 3857) ,eps := eps, minpoints := 1) OVER (PARTITION BY port_id) + 1 AS cid_dbscan
		--,ST_ClusterDBSCAN(ST_Transform(centr, 3857) ,eps := eps, minpoints := 1) OVER () + 1 AS cid_dbscan_incr 
		,MIN(port_dist) OVER (PARTITION BY port_id) min_port_dist
	FROM dur_filter
)
,distinct_clusters AS (
	SELECT DISTINCT port_id, cid_dbscan
	FROM clusters
)
,incr_clusters AS (
	SELECT port_id, cid_dbscan, ROW_NUMBER() OVER (ORDER BY port_id, cid_dbscan) AS cid_dbscan_incr
	FROM distinct_clusters
)
,min_dist_clusters AS (
	SELECT cid_dbscan_incr
	FROM clusters
	INNER JOIN incr_clusters ON incr_clusters.port_id = clusters.port_id AND incr_clusters.cid_dbscan = clusters.cid_dbscan
	WHERE port_dist = min_port_dist
		AND min_port_dist <= eps
	)

--INSERT INTO data_analysis.port_stops(vessel_id, ts_begin, ts_end, duration_h, nb_pos, centr, clusters.port_id, port_name, port_dist, clusters.cid_dbscan, cid_dbscan_incr )
SELECT vessel_id, ts_begin, ts_end, duration_h, nb_pos, centr, clusters.port_id, port_name, port_dist, clusters.cid_dbscan, cid_dbscan_incr 
FROM clusters
INNER JOIN incr_clusters ON incr_clusters.port_id = clusters.port_id AND incr_clusters.cid_dbscan = clusters.cid_dbscan
WHERE cid_dbscan_incr IN (SELECT cid_dbscan_incr FROM min_dist_clusters);

ALTER TABLE data_analysis.port_stops ADD COLUMN id bigserial;

ALTER TABLE data_analysis.port_stops ADD CONSTRAINT pk_port_stops_id PRIMARY KEY ("id");

CREATE INDEX idx_port_stops_vessel_id ON data_analysis.port_stops
	USING btree (vessel_id);

CREATE INDEX idx_port_stops_centr ON data_analysis.port_stops USING gist (centr);


/*********************************************************************************************************/
/***************************************** Clustering Port Stops *****************************************/
/*********************************************************************************************************/
--TRUNCATE TABLE data_analysis.clusters_port_stops_hulls RESTART IDENTITY;
CREATE TABLE data_analysis.clusters_port_stops_hulls AS
--INSERT INTO data_analysis.clusters_port_stops_hulls
	SELECT cid_dbscan_incr 
		,ST_ConvexHull(st_collect(centr)) as convex_hull 
		,ST_ConcaveHull(st_collect(centr), 0.75) as concave_hull 
		,ST_MinimumBoundingCircle(st_collect(centr)) as bounding_circle 
		,ST_Centroid(st_collect(centr)) as centroid 
		,count (*) as nb_stops  --number of stops in this cluster ( area )
		,sum ( nb_pos ) as nb_pos --num of stops in the cluster
		,count ( DISTINCT vessel_id ) as nb_ships --num of unique ships
		,min(duration_h) as min_dur_h 
		,avg(duration_h) as avg_dur_h 
		,max(duration_h) as max_dur_h
	FROM data_analysis.port_stops
	WHERE cid_dbscan_incr IS NOT NULL  --exclude outliers
	GROUP BY cid_dbscan_incr ;


