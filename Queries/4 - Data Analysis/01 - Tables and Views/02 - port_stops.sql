/*********************************************************************************************************/
/******************************************* Create Port Stops *******************************************/
/*********************************************************************************************************/
/* The full logic explanation can be found in thesis section 4.1.5.2  ************************************/
/*********************************************************************************************************/


CREATE TABLE data_analysis.port_stops AS
WITH port_stops AS (  /*  Initial port stop identification using Voronoi polygons  */
	SELECT *
	FROM (
		SELECT s.vessel_id, s.ts_begin, s.ts_end, s.duration_s/60/60 as "duration_h", s.nb_pos, s.centr
			, pv.port_id, pv.port_name
			,ST_DistanceSphere (s.centr, pv.geom) as port_dist  -- Distance from stop centroid to actual port location
		FROM data_analysis.stops s
		-- Spatial join: assign stops to ports using Voronoi polygons. Each Voronoi cell contains all points closest to a specific port
		LEFT JOIN data_analysis.ports_voronoi pv ON ST_Within(s.centr, pv.voronoi_zone)
	)
	-- Filter: only include stops within 50km of their assigned port. This removes stops that are too far from any port to be meaningful
	WHERE port_dist <= 50000
)
,dur_filter AS (
	SELECT s.*
		,CASE -- The full logic explanation can be found in thesis section 4.1.5.2
			-- Very Small ports (V)
			WHEN harborsize = 'V' AND harbortype like 'R%' THEN 5000	-- River port: 5km
			WHEN harborsize = 'V' AND harbortype like 'C%' THEN 3000  	-- Coastal port: 3km
			-- Small ports (S)
			WHEN harborsize = 'S' AND harbortype like 'R%' THEN 6000  	-- River port: 6km
			WHEN harborsize = 'S' AND harbortype like 'C%' THEN 3000	-- Coastal port: 3km
			-- Medium ports (M)
			WHEN harborsize = 'M' AND harbortype like 'R%' THEN 8000 	-- River port: 8km 
			WHEN harborsize = 'M' AND harbortype like 'C%' THEN 3000  	-- Coastal port: 3km
			-- Large ports (L)
			WHEN harborsize = 'L' AND harbortype like 'R%' THEN 10000	-- River port: 10km
			WHEN harborsize = 'L' AND harbortype like 'C%' THEN 4000  	-- Coastal port: 4km
			ELSE 3000  -- Default fallback: 3km
        END as eps  -- Epsilon parameter for DBSCAN clustering
	FROM port_stops s
	INNER JOIN context_data.ports p on p.id = s.port_id
	 -- Duration filter: only include stops lasting at least 10 hours
    -- This excludes brief stops that are likely not actual port visits
	WHERE s.duration_h >= 10
	)
,clusters AS (  /*  Apply DBSCAN clustering to group stops within each port  */
	SELECT eps, vessel_id, ts_begin, ts_end, duration_h, nb_pos, centr, port_id, port_name, port_dist--, id
		-- Apply DBSCAN clustering within each port
		-- eps: dynamic epsilon based on port characteristics  
        -- minpoints := 1: any single stop can form a cluster
		,ST_ClusterDBSCAN(ST_Transform(centr, 3857) ,eps := eps, minpoints := 1) OVER (PARTITION BY port_id) + 1 AS cid_dbscan  
		-- Find the minimum port distance within each port
        -- Used later to identify the most representative stop for each cluster
		,MIN(port_dist) OVER (PARTITION BY port_id) min_port_dist
	FROM dur_filter
)
,distinct_clusters AS (  /* Identify unique cluster combinations */
	SELECT DISTINCT port_id, cid_dbscan
	FROM clusters
)
,incr_clusters AS (  /* Create globally unique cluster IDs across all ports */
	SELECT port_id, cid_dbscan, 
		ROW_NUMBER() OVER (ORDER BY port_id, cid_dbscan) AS cid_dbscan_incr  -- Create sequential cluster IDs across all ports for easy reference
	FROM distinct_clusters
)
,min_dist_clusters AS (  /* Identify the best representative stop for each cluster */
	SELECT cid_dbscan_incr
	FROM clusters
	INNER JOIN incr_clusters ON incr_clusters.port_id = clusters.port_id AND incr_clusters.cid_dbscan = clusters.cid_dbscan
	-- Select the stop closest to the port within each cluster
	WHERE port_dist = min_port_dist
		AND min_port_dist <= eps  -- Additional quality check: closest stop must be within epsilon distance
	)

SELECT vessel_id, ts_begin, ts_end, duration_h, nb_pos, centr, clusters.port_id, port_name, port_dist, clusters.cid_dbscan, cid_dbscan_incr 
FROM clusters
INNER JOIN incr_clusters ON incr_clusters.port_id = clusters.port_id AND incr_clusters.cid_dbscan = clusters.cid_dbscan
-- Filter: only include the best representative stops for each cluster. This ensures we have one high-quality stop per cluster
WHERE cid_dbscan_incr IN (SELECT cid_dbscan_incr FROM min_dist_clusters);

/* Create id columns and indexes */
ALTER TABLE data_analysis.port_stops ADD COLUMN id bigserial;
ALTER TABLE data_analysis.port_stops ADD CONSTRAINT pk_port_stops_id PRIMARY KEY ("id");

CREATE INDEX idx_port_stops_vessel_id ON data_analysis.port_stops
	USING btree (vessel_id);

CREATE INDEX idx_port_stops_centr ON data_analysis.port_stops USING gist (centr);


/*********************************************************************************************************/
/***************************************** Clustering Port Stops *****************************************/
/*********************************************************************************************************/
CREATE TABLE data_analysis.clusters_port_stops_hulls AS
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


