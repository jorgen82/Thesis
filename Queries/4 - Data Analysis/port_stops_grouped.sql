/* 
We will group the Port Stops in order to be able to match those to the Fixtures later
Not grouping the stops will lead to multiple matches, since a vessel might have more than one stops close to a port that are identified as port stop
The matching will be done is there are 2 or more port stops of the same vessel at the same port in less than 2 days period
*/

CREATE TABLE data_analysis.port_stops_grouped AS
	WITH time_diff AS (
	    SELECT *
	        -- Calculate the time difference between consecutive rows
	        ,COALESCE(EXTRACT(EPOCH FROM (ts_begin - LAG(ts_begin) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin)))/86400, 0) AS time_diff_days
			,COALESCE(EXTRACT(EPOCH FROM (ts_begin - LAG(ts_end) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin)))/3600, 0) AS non_stop_hours
	    FROM data_analysis.port_stops
	),
	grouped_data AS (
	    SELECT *
	        -- Create groups where the time difference is greater than 2 days
			,CASE WHEN time_diff_days > 2 THEN 0 ELSE non_stop_hours END AS non_stop_h 
	        ,SUM(CASE WHEN time_diff_days > 2 THEN 1 ELSE 0 END) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin) AS group_id
	    FROM 
	        time_diff
	)

	SELECT 
	    vessel_id,
	    port_id,
		port_name,
	    MIN(ts_begin) AS first_ts_begin,  
		MAX(ts_end) AS last_ts_end,
		EXTRACT(EPOCH FROM (MAX(ts_end) - MIN(ts_begin)))/60/60 AS total_duration_h,
	    EXTRACT(EPOCH FROM (MAX(ts_end) - MIN(ts_begin)))/60/60 - SUM(non_stop_h) AS total_stop_h,
		SUM(non_stop_h) AS total_non_stop_h,
	    SUM(nb_pos) AS total_nb_pos,
		MIN(port_dist) AS min_port_dist
	FROM 
	    grouped_data
	GROUP BY 
	    vessel_id, port_id, group_id, port_name;


ALTER TABLE data_analysis.port_stops_grouped
ADD COLUMN id BIGSERIAL;