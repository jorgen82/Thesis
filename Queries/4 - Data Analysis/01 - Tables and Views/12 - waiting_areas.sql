/*
	Here we identify the waiting points and waiting areas.
	Then we perform a DBSCAN using as minPoints = 10 and eps = 1500 for River ports and 3500 for Coastal.
	-- Waiting points: The stops that do not exist in the port stops.
	-- Waiting areas: We group based the DBSCAN cluster id and export some basic statistics
	
	Then we will also create the waiting_points_seasonal and waiting_areas_seasonal.
	We based those tables on the previous ones, in order for the waiting areas id to match.
	For those tables we introduce the Year and Month information.
	To do this we need to check if a stop is across months and split it up to the respective months, making sure that the correct information is maintained per month (eg duration).
*/


/************************************************************************************************************/
/* 										Points ans waiting areas											*/
/************************************************************************************************************/
CREATE TABLE data_analysis.waiting_points AS
SELECT *
FROM (
	SELECT vessel_id, stop_id, ts_begin, ts_end, duration_seconds, duration_minutes, duration_hours, centr, avg_dist_centroid, max_dist_centroid,
		(ST_ClusterDBSCAN(ST_Transform(centr, 3857) ,eps := eps, minpoints := 10) OVER(PARTITION BY eps) + 1)  + (CASE WHEN eps = 1000 THEN 0 WHEN eps = 3500 THEN 1000000 ELSE 2000000 END) AS cid_dbscan
	FROM (
		SELECT s.vessel_id, s.id as stop_id, v.vessel_type, s.ts_begin, s.ts_end, s.duration_s AS duration_seconds,
            CAST(s.duration_s /60 AS decimal(11,3)) AS duration_minutes,
            CAST(s.duration_s /3600 AS decimal(12,4)) AS duration_hours,
			s.centr, s.avg_dist_centroid, s.max_dist_centroid,
			CASE WHEN p.harbortype LIKE 'C%' THEN 3500 WHEN p.harbortype LIKE 'R%' AND NOT ST_Within(s.centr, (SELECT geometry FROM context_data.oceans WHERE featurecla = 'Ocean')) THEN 1000 ELSE 3500 END as eps
		FROM data_analysis.stops s
		LEFT JOIN data_analysis.port_stops ps ON ps.vessel_id = s.vessel_id
			AND ps.ts_begin = s.ts_begin
			AND ps.ts_end = s.ts_end 
		LEFT JOIN ais.vessel v on v.id = s.vessel_id
		LEFT JOIN data_analysis.ports_voronoi pv ON ST_Within(s.centr, pv.voronoi_zone)
		LEFT JOIN context_data.ports p on p.id = pv.port_id
		WHERE ps.id is NULL
	)x
) y
WHERE cid_dbscan IS NOT NULL;  

-- Add id column and create appropriate indexes for the waiting_points table
ALTER TABLE data_analysis.waiting_points ADD ID bigserial;
CREATE INDEX idx_waiting_points_stop_id ON data_analysis.waiting_points(stop_id);
CREATE INDEX idx_waiting_points_cid_dbscan ON data_analysis.waiting_points(cid_dbscan);
CREATE INDEX idx_waiting_points_centr ON data_analysis.waiting_points USING gist (centr);



/*
	Group based on the cluster id and calculate the Concave Hull, Convex Hull and Bounding Circle of each cluster.
*/
CREATE TABLE data_analysis.waiting_areas AS
SELECT cid_dbscan,
	CASE WHEN cid_dbscan < 1000000 THEN 'River' WHEN cid_dbscan >= 1000000 AND cid_dbscan < 2000000 THEN 'Coastal' ELSE 'Other' END as river_coastal,
	ST_SetSRID(ST_ConvexHull(st_collect(centr)),4326) as convex_hull ,
	ST_SetSRID(data_analysis.safe_concave_hull(st_collect(centr), 0.75), 4326) as concave_hull,
	ST_SetSRID(ST_MinimumBoundingCircle(st_collect(centr)),4326) as bounding_circle ,
	ST_SetSRID(ST_Centroid(st_collect(centr)),4326) as centr,
	count(*) as nb_stops ,
	count(DISTINCT vessel_id) as nb_vessels_distinct,
	min(duration_seconds) as min_duration_seconds,
	max(duration_seconds) as max_duration_seconds,
	avg(duration_seconds) as avg_duration_seconds,
	min(duration_minutes) as min_duration_minutes,
	max(duration_minutes) as max_duration_minutes,
	avg(duration_minutes) as avg_duration_minutes,	
	min(duration_hours) as min_duration_hours,
	max(duration_hours) as max_duration_hours,
	avg(duration_hours) as avg_duration_hours,
	CAST(ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 as decimal(14,6)) as area_km2,
	CAST(count(*) / (ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000) as decimal(10,2)) as vessel_density,
	CAST(avg(duration_hours) * count(*) as decimal(10,2)) as total_vessel_hours_waiting,
	CAST((count(*) / (ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000)) *  avg(duration_hours) as decimal(10,2)) as utilization_rate	
FROM data_analysis.waiting_points
WHERE cid_dbscan IS NOT NULL
GROUP BY cid_dbscan;


-- Add id column and create appropriate indexes for the waiting_areas table
ALTER TABLE data_analysis.waiting_areas ADD ID bigserial;
CREATE INDEX idx_waiting_areas_cid_dbscan ON data_analysis.waiting_areas USING btree (cid_dbscan);
CREATE INDEX idx_waiting_areas_centr ON data_analysis.waiting_areas USING gist (centr);
CREATE INDEX idx_waiting_areas_convex_hull ON data_analysis.waiting_areas USING gist (convex_hull);
CREATE INDEX idx_waiting_areas_concave_hull ON data_analysis.waiting_areas USING gist (concave_hull);
CREATE INDEX idx_waiting_areas_bounding_circle ON data_analysis.waiting_areas USING gist (bounding_circle);





/************************************************************************************************************/
/* 				        Points ans waiting areas with seasonality information								*/
/************************************************************************************************************/
CREATE TABLE data_analysis.waiting_points_seasonal AS
WITH RECURSIVE stops_clustered AS (
    SELECT id as waiting_points_id, cid_dbscan, vessel_id, stop_id, ts_begin, ts_end, ts_end AS original_ts_end, duration_seconds, duration_minutes, duration_hours, centr, avg_dist_centroid, max_dist_centroid
	FROM data_analysis.waiting_points
),
split_monthly AS (
    SELECT
		waiting_points_id,
		cid_dbscan,
        stop_id,
        vessel_id,
        ts_begin,
        LEAST(ts_end, date_trunc('month', ts_begin + interval '1 month') - interval '1 second') AS ts_end,
        original_ts_end,
        centr, avg_dist_centroid, max_dist_centroid
    FROM stops_clustered
    UNION ALL
    SELECT
		waiting_points_id,
		cid_dbscan,
        stop_id,
        vessel_id,
        ts_end + interval '1 second' AS ts_begin,
        LEAST(original_ts_end, date_trunc('month', ts_end + interval '1 second' + interval '1 month') - interval '1 second') AS ts_end,
        original_ts_end,
        centr, avg_dist_centroid, max_dist_centroid
    FROM split_monthly
    WHERE ts_end < original_ts_end
)

SELECT s.waiting_points_id, s.cid_dbscan, s.stop_id, s.vessel_id, v.vessel_type, s.ts_begin, s.ts_end, 
    EXTRACT(YEAR FROM s.ts_begin) as "Year", EXTRACT(MONTH FROM s.ts_begin) as "Month", EXTRACT(QUARTER FROM s.ts_begin) as "Quarter", 
	CONCAT(EXTRACT(YEAR FROM s.ts_begin), '-', EXTRACT(MONTH FROM s.ts_begin)) as "Year-Month",
    CONCAT(EXTRACT(YEAR FROM s.ts_begin), '-', EXTRACT(QUARTER FROM s.ts_begin)) as "Year-Quarter",
	CAST(EXTRACT(EPOCH FROM (s.ts_end - s.ts_begin)) AS decimal(10,2)) AS duration_seconds,
    CAST(EXTRACT(EPOCH FROM (s.ts_end - s.ts_begin)) /60 AS decimal(11,3)) AS duration_minutes,
    CAST(EXTRACT(EPOCH FROM (s.ts_end - s.ts_begin)) /3600 AS decimal(12,4)) AS duration_hours,
    s.centr, s.avg_dist_centroid, s.max_dist_centroid
FROM split_monthly s
LEFT JOIN ais.vessel v on v.id = s.vessel_id
WHERE s.cid_dbscan IS NOT NULL
	and s.ts_end != s.ts_begin;


-- Add id column and create appropriate indexes for the waiting_points_seasonal table
ALTER TABLE data_analysis.waiting_points_seasonal ADD ID bigserial;
CREATE INDEX idx_waiting_points_seasonal_stop_id ON data_analysis.waiting_points_seasonal(stop_id);
CREATE INDEX idx_waiting_points_seasonal_cid_dbscan ON data_analysis.waiting_points_seasonal(cid_dbscan);
CREATE INDEX idx_waiting_points_seasonal_centr ON data_analysis.waiting_points_seasonal USING gist (centr);




/*
	Group based on the cluster id and calculate the Concave Hull, Convex Hull and Bounding Circle of each cluster.
*/
CREATE TABLE data_analysis.waiting_areas_seasonal AS
SELECT cid_dbscan,
	'Year' as temporal_cluster,
    "Year",
    0 as month_quarter,
    CONCAT("Year", '-0') as year_month_quarter,
	CASE WHEN cid_dbscan < 1000000 THEN 'River' WHEN cid_dbscan >= 1000000 AND cid_dbscan < 2000000 THEN 'Coastal' ELSE 'Other' END as river_coastal,
	ST_SetSRID(ST_ConvexHull(st_collect(centr)),4326) as convex_hull,
	ST_SetSRID(data_analysis.safe_concave_hull(st_collect(centr),0.75),4326) as concave_hull ,
	ST_SetSRID(ST_MinimumBoundingCircle(st_collect(centr)),4326) as bounding_circle ,
	ST_SetSRID(ST_Centroid(st_collect(centr)),4326) as centr,
	count (*) as nb_stops ,
	count (DISTINCT vessel_id) as nb_vessels_distinct ,
	min(duration_seconds) as min_duration_seconds,
	max(duration_seconds) as max_duration_seconds,
	avg(duration_seconds) as avg_duration_seconds,
	min(duration_minutes) as min_duration_minutes,
	max(duration_minutes) as max_duration_minutes,
	avg(duration_minutes) as avg_duration_minutes,	
	min(duration_hours) as min_duration_hours,
	max(duration_hours) as max_duration_hours,
	avg(duration_hours) as avg_duration_hours,
	CAST(
		CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END
		as decimal(14,6)) as area_km2,
	 CAST(
	 	count(*) / 
	 	(CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END) 		
	 	as decimal(30,2)) as vessel_density,
	 CAST(avg(duration_hours) * count(*) as decimal(20,2)) as total_vessel_hours_waiting,
	 CAST(
	 	(count(*) / 
	 	(CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END
	 	)) *  avg(duration_hours) 
	 	as decimal(30,2)) as utilization_rate	
FROM data_analysis.waiting_points_seasonal
WHERE cid_dbscan IS NOT NULL
GROUP BY cid_dbscan,"Year"
UNION
SELECT cid_dbscan,
	'Year-Quarter' as temporal_cluster,
    "Year",
    "Quarter" as month_quarter,
    "Year-Quarter" as year_month_quarter,
	CASE WHEN cid_dbscan < 1000000 THEN 'River' WHEN cid_dbscan >= 1000000 AND cid_dbscan < 2000000 THEN 'Coastal' ELSE 'Other' END as river_coastal,
	ST_SetSRID(ST_ConvexHull(st_collect(centr)),4326) as convex_hull ,
	ST_SetSRID(data_analysis.safe_concave_hull(st_collect(centr),0.75),4326) as concave_hull ,
	ST_SetSRID(ST_MinimumBoundingCircle(st_collect(centr)),4326) as bounding_circle ,
	ST_SetSRID(ST_Centroid(st_collect(centr)),4326) as centr,
	count (*) as nb_stops ,
	count (DISTINCT vessel_id) as nb_vessels_distinct ,
	min(duration_seconds) as min_duration_seconds,
	max(duration_seconds) as max_duration_seconds,
	avg(duration_seconds) as avg_duration_seconds,
	min(duration_minutes) as min_duration_minutes,
	max(duration_minutes) as max_duration_minutes,
	avg(duration_minutes) as avg_duration_minutes,	
	min(duration_hours) as min_duration_hours,
	max(duration_hours) as max_duration_hours,
	avg(duration_hours) as avg_duration_hours,
	CAST(
		CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END
		as decimal(14,6)) as area_km2,
	 CAST(
	 	count(*) / 
	 	(CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END) 		
	 	as decimal(30,2)) as vessel_density,
	 CAST(avg(duration_hours) * count(*) as decimal(20,2)) as total_vessel_hours_waiting,
	 CAST(
	 	(count(*) / 
	 	(CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END
	 	)) *  avg(duration_hours) 
	 	as decimal(30,2)) as utilization_rate	
FROM data_analysis.waiting_points_seasonal
WHERE cid_dbscan IS NOT NULL
GROUP BY cid_dbscan,"Year","Quarter","Year-Quarter"
UNION
SELECT cid_dbscan,
	'Year-Month' as temporal_cluster,
    "Year",
    "Month" as month_quarter,
    "Year-Month" as year_month_quarter,
	CASE WHEN cid_dbscan < 1000000 THEN 'River' WHEN cid_dbscan >= 1000000 AND cid_dbscan < 2000000 THEN 'Coastal' ELSE 'Other' END as river_coastal,
	ST_SetSRID(ST_ConvexHull(st_collect(centr)),4326) as convex_hull ,
	ST_SetSRID(data_analysis.safe_concave_hull(st_collect(centr),0.75),4326) as concave_hull ,
	ST_SetSRID(ST_MinimumBoundingCircle(st_collect(centr)),4326) as bounding_circle ,
	ST_SetSRID(ST_Centroid(st_collect(centr)),4326) as centr,
	count (*) as nb_stops ,
	count (DISTINCT vessel_id) as nb_vessels_distinct ,
	min(duration_seconds) as min_duration_seconds,
	max(duration_seconds) as max_duration_seconds,
	avg(duration_seconds) as avg_duration_seconds,
	min(duration_minutes) as min_duration_minutes,
	max(duration_minutes) as max_duration_minutes,
	avg(duration_minutes) as avg_duration_minutes,	
	min(duration_hours) as min_duration_hours,
	max(duration_hours) as max_duration_hours,
	avg(duration_hours) as avg_duration_hours,
	CAST(
		CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END
		as decimal(14,6)) as area_km2,
	 CAST(
	 	count(*) / 
	 	(CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END) 		
	 	as decimal(30,2)) as vessel_density,
	 CAST(avg(duration_hours) * count(*) as decimal(20,2)) as total_vessel_hours_waiting,
	 CAST(
	 	(count(*) / 
	 	(CASE WHEN count(*) = 1 THEN 0.0001
	 		WHEN count(*) = 2 AND ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 = 0 THEN 0.0001
			WHEN count(*) = 2 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000 
	 		WHEN count(*) > 2 AND ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 = 0 THEN ST_Area(ST_Transform(ST_MinimumBoundingCircle(ST_Collect(centr)), 3857)) / 1000000
			ELSE ST_Area(ST_Transform(data_analysis.safe_concave_hull(st_collect(centr),0.75), 3857)) / 1000000 
	 		END
	 	)) *  avg(duration_hours) 
	 	as decimal(30,2)) as utilization_rate	
FROM data_analysis.waiting_points_seasonal
WHERE cid_dbscan IS NOT NULL
GROUP BY cid_dbscan,"Year","Month","Year-Month";



-- Add id column and create appropriate indexes for the waiting_areas_seasonals table
ALTER TABLE data_analysis.waiting_areas_seasonal ADD ID bigserial;
CREATE INDEX idx_waiting_areas_seasonal_cid_dbscan ON data_analysis.waiting_areas_seasonal USING btree (cid_dbscan);
CREATE INDEX idx_waiting_areas_seasonal_centr ON data_analysis.waiting_areas_seasonal USING gist (centr);
CREATE INDEX idx_waiting_areas_seasonal_convex_hull ON data_analysis.waiting_areas_seasonal USING gist (convex_hull);
CREATE INDEX idx_waiting_areas_seasonal_concave_hull ON data_analysis.waiting_areas_seasonal USING gist (concave_hull);
CREATE INDEX idx_waiting_areas_seasonal_bounding_circle ON data_analysis.waiting_areas_seasonal USING gist (bounding_circle);
