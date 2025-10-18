/*********************************************************************************/
/******************* Create Relevant Index in Port Stops *************************/
/*********************************************************************************/
CREATE INDEX idx_port_stops_vessel_id_ts_begin_ts_end
ON data_analysis.port_stops(vessel_id,ts_begin,ts_end)

/*********************************************************************************/
/**************************** Create Tracks Table ********************************/
/*********************************************************************************/

CREATE TABLE data_analysis.tracks AS
WITH difference_calculation AS ( /* Calculate the time difference between AIS and bring in port stop information */
    SELECT
        a.*
		,a.ts - LAG(a.ts) OVER (PARTITION BY a.vessel_id ORDER BY a.ts) AS time_difference        -- Calculate time gap between consecutive AIS points for this vessel
		,CASE WHEN ps.vessel_id IS NULL THEN 0 ELSE 1 END as is_port_stop  -- Flag indicating if current point is during a port stop (1) or at sea (0)
		,CASE WHEN LAG(ps.vessel_id) OVER (PARTITION BY a.vessel_id ORDER BY a.ts) IS NULL THEN 0 ELSE 1 END AS prev_is_port_stop  -- Flag indicating if previous point was during a port stop
		,ps.id AS port_stop_id  -- Current port stop ID (if any)
		,LAG(ps.id) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)  AS prev_port_stops_grouped_id  -- Previous port stop ID for tracking transitions
		,LEAD(ps.id) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)  AS next_port_stops_grouped_id  -- Next port stop ID for tracking transitions  
    FROM ais.ais a
	-- Left join to port stops to identify when vessel is in port vs at sea
	LEFT JOIN data_analysis.port_stops_grouped ps on ps.vessel_id = a.vessel_id
		AND a.ts >= ps.first_ts_begin AND a.ts <= ps.last_ts_end  -- Match AIS points that fall within port stop time windows
)
,ais_grouping AS ( /* Change the group_id (increase a group) if the AIS timestamps are more than 2 days apart and if its a port stop. */
	SELECT vessel_id
		,LEAD(vessel_id) OVER (PARTITION BY vessel_id ORDER BY vessel_id, ts) as vessel_id2  --next vessel_id
		,ts as ts1  -- current point timestamp (segment start)
		,LEAD(ts) OVER (PARTITION BY vessel_id ORDER BY vessel_id, ts) as ts2  -- next point timestamp (segment end)
		,speed_kn as speed1  -- speed at start point
		,LEAD(speed_kn) OVER (PARTITION BY vessel_id ORDER BY vessel_id, ts) as speed2  -- speed at end point
		,geom as p1  -- geometry at start point
		,LEAD(geom) OVER (PARTITION BY vessel_id ORDER BY vessel_id, ts) as p2  -- geometry at end point
		,is_port_stop
		,prev_is_port_stop
		,port_stop_id
		,prev_port_stops_grouped_id
		,next_port_stops_grouped_id
		-- Core grouping logic: increment group_id when we encounter:
        -- - Time gaps > 2 days (data gaps or vessel offline)
        -- - NULL time differences (first point in sequence)  
        -- - Transitions between port/sea states
		,SUM(CASE WHEN time_difference > INTERVAL '2 days' OR time_difference IS NULL OR is_port_stop <> prev_is_port_stop THEN 1 ELSE 0 END) OVER (PARTITION BY vessel_id ORDER BY ts) AS group_id
	FROM difference_calculation	
)
,tracks_data AS (  /* Set the group_id to null if the AIS timestamps are more than 2 days apart and if its a port stop. */
	SELECT vessel_id
		,ts1
		,ts2
		,extract(epoch FROM (ts2-ts1)) as duration_s
		,speed1
		,speed2
		-- Calculated speed in m/s (handles edge case where ts1 = ts2)
		,CASE WHEN ts1 = ts2 THEN speed1 * 1852 ELSE ST_DistanceSphere (p1,p2) / extract (epoch FROM (ts2 - ts1)) END as speed_m_s
		-- Calculated speed in knots
		,CASE WHEN ts1 = ts2 THEN speed1 ELSE (ST_DistanceSphere(p1,p2) / 1852) / (extract(epoch FROM (ts2 - ts1)) / 3600) END AS speed_kn
		,p1  -- Start point geometry
		,p2  -- End point geometry
		,ST_DistanceSphere (p1 ,p2) as distance_m  -- Great-circle distance in meters
		,st_makeline(p1,p2) as track  -- Line segment between points
		,is_port_stop
		,port_stop_id
		,prev_port_stops_grouped_id
		,next_port_stops_grouped_id
		-- Critical filtering: NULL out groups that represent invalid tracks
        -- > 2 day gaps or port stops are not considered continuous tracks
		,CASE WHEN ts2 - ts1 > INTERVAL '2 days' OR is_port_stop = 1 THEN null ELSE group_id END AS group_id
	FROM ais_grouping 
)
,tracks_data_grouped AS (
	SELECT vessel_id, group_id, count(*) as points, MIN(ts1) as ts_start, MAX(ts2) as ts_end, SUM(duration_s) / 60 / 60 as duration_h, AVG(speed_m_s) AS avg_speed_m_s, AVG(speed_kn) AS avg_speed_kn, SUM(distance_m) as distance_m, SUM(distance_m) / 1852 as distance_nmi
		,ST_Union(track) as track  -- Combine all line segments into continuous track geometry
		,(ARRAY_AGG(p1 ORDER BY ts1))[1] AS first_point  -- Get first point of track using array aggregation
		,(ARRAY_AGG(p2 ORDER BY ts2 DESC))[1] AS last_point  -- Get last point of track using array aggregation
		,(ARRAY_AGG(prev_port_stops_grouped_id ORDER BY ts1))[1] AS from_port_stops_grouped_id   -- Determine origin port (from beginning of track)
		,(ARRAY_AGG(next_port_stops_grouped_id ORDER BY ts1 DESC))[1] AS to_port_stops_grouped_id  -- Determine destination port (from end of track)
	FROM tracks_data
	WHERE group_id IS NOT NULL  -- Only include valid track groups
	GROUP BY vessel_id, group_id
)

SELECT vessel_id, group_id, from_port_stops_grouped_id, to_port_stops_grouped_id, points, ts_start, ts_end, duration_h, avg_speed_m_s, avg_speed_kn, distance_m, distance_nmi
	-- Calculate cardinal direction of travel based on start/end coordinates
	,CASE 
		WHEN ST_X(last_point) > ST_X(first_point) AND ST_Y(last_point) > ST_Y(first_point) THEN 'North-East'
        WHEN ST_X(last_point) > ST_X(first_point) AND ST_Y(last_point) < ST_Y(first_point) THEN 'South-East'
        WHEN ST_X(last_point) < ST_X(first_point) AND ST_Y(last_point) > ST_Y(first_point) THEN 'North-West'
        WHEN ST_X(last_point) < ST_X(first_point) AND ST_Y(last_point) < ST_Y(first_point) THEN 'South-West'
        WHEN ST_X(last_point) = ST_X(first_point) AND ST_Y(last_point) > ST_Y(first_point) THEN 'North'
        WHEN ST_X(last_point) = ST_X(first_point) AND ST_Y(last_point) < ST_Y(first_point) THEN 'South'
        WHEN ST_X(last_point) > ST_X(first_point) AND ST_Y(last_point) = ST_Y(first_point) THEN 'East'
        WHEN ST_X(last_point) < ST_X(first_point) AND ST_Y(last_point) = ST_Y(first_point) THEN 'West'
        ELSE 'Stationary'
	END AS direction
	,track
FROM tracks_data_grouped;


/*********************************************************************************/
/***************************** Add ID and Indexes ********************************/
/*********************************************************************************/
ALTER TABLE data_analysis.tracks
ADD COLUMN id BIGSERIAL;

CREATE INDEX idx_tracks_track ON data_analysis.tracks USING gist (track) ;
CREATE INDEX idx_tracks_vessel_id ON data_analysis.tracks(vessel_id);
CREATE INDEX idx_tracks_vessel_id_ts_start_ts_end ON data_analysis.tracks(vessel_id, ts_start, ts_end);


/*********************************************************************************/
/*************************** Add Calculated Columns ******************************/
/*********************************************************************************/

-- Add info related to previous track
ALTER TABLE data_analysis.tracks
	ADD COLUMN previous_id bigint, 
	ADD COLUMN days_from_previous_track decimal(6,2);

UPDATE data_analysis.tracks tr
SET previous_id = tr_pre.previous_id
	,days_from_previous_track = tr_pre.days_from_previous_track
FROM (SELECT id
		,LAG(id) OVER (PARTITION BY vessel_id ORDER BY ts_start) as previous_id  -- Previous track id
		,CAST(EXTRACT(epoch from (ts_start - LAG(ts_end) OVER (PARTITION BY vessel_id ORDER BY ts_start))) / 86400 as decimal(6,2)) as days_from_previous_track  -- Days difference from previous track (Previous Track end to Current track start )
		FROM data_analysis.tracks
	)tr_pre
WHERE tr.id = tr_pre.id;


-- Add track region info (west - east coast)
ALTER TABLE data_analysis.tracks
	ADD COLUMN us_region char(4);

UPDATE data_analysis.tracks tr
SET us_region = r.region
FROM context_data.us_region r
WHERE ST_CONTAINS(r.geom, tr.track) = true;
