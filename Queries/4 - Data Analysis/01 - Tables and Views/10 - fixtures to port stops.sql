/* Use this query to identify the most proper interval to be used as a +/- laycan days for matching a fixture to a port stop */

SELECT iteration_number as time_window, count_result as count
	,cast(((count_result - LAG(count_result) OVER (ORDER BY iteration_number)) / ((count_result + LAG(count_result) OVER (ORDER BY iteration_number))/.2) ) * 100 as decimal(5,2)) as pct_change
FROM data_analysis.fn_fixtures_to_port_stops_laycan_interval()

/* 
Create the fixtures_to_port_stops table
Make sure you change the interval to the one you selected before
*/


CREATE TABLE data_analysis.fixtures_to_port_stops AS
SELECT fixture_id, vessel_id, port_id, port_stops_grouped_id, vessel_name, laycan_from, laycan_to, first_ts_begin, last_ts_end, fixture_port_load, fixture_country, port_stop_port_name, port_stop_country,
	total_duration_h, onTime, arrival_after_laycan_from_days, arrival_after_laycan_to_days,
	-- Early arrival analysis (negative values only)
	CASE WHEN arrival_after_laycan_from_days < 0 THEN arrival_after_laycan_from_days END AS early_arrival_days,
	-- Late arrival analysis (positive values only)
	CASE WHEN arrival_after_laycan_to_days > 0 THEN arrival_after_laycan_to_days END AS late_laycan_days,
	 -- On-time arrival analysis - days after laycan start
	CASE WHEN onTime = 1 AND arrival_after_laycan_from_days > 0 THEN arrival_after_laycan_from_days END AS ontime_arrival_after_laycan_from,
	-- On-time arrival analysis - days before laycan end
	CASE WHEN onTime = 1 AND arrival_after_laycan_to_days < 0 THEN ABS(arrival_after_laycan_to_days) END AS ontime_arrival_before_laycan_to
FROM
	(
	SELECT *
		-- Assign priority ranking to find the best match for each fixture, ROW_NUMBER ensures we only keep the best match (row_num = 1)
		,ROW_NUMBER() OVER (PARTITION BY fixture_id ORDER BY priority, closest_difference_in_days) as row_num
	FROM 
		(
		-- Core matching logic: join fixtures with port stops
		SELECT 
			f.id as fixture_id,
			f.vessel_id,
			p.id as port_id,
			ps.id as port_stops_grouped_id,
			f.vessel_name,
			f.laycan_from,
		    f.laycan_to,
		    ps.first_ts_begin, 
		    ps.last_ts_end,  
			f.port_load as fixture_port_load,
			f.country as fixture_country,
			ps.port_name as port_stop_port_name,
			c.country as port_stop_country,
			ps.total_duration_h,
			-- On-time indicator: 1 if arrival within laycan window
			CASE WHEN ps.first_ts_begin >= f.laycan_from AND ps.first_ts_begin <= f.laycan_to THEN 1 ELSE 0 END AS onTime,
			-- Days difference from laycan start (positive = late, negative = early)
			CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_from)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_from_days,
			-- Days difference from laycan end (positive = late, negative = early)
			CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_to)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_to_days, 
			-- Priority system for matching:
            -- 1 = Best: Port stop overlaps with laycan window
            -- 2 = Acceptable: Port stop after laycan (vessel late)
            -- 3 = Acceptable: Port stop before laycan (vessel early)
            -- 4 = Catch-all for edge cases
			CASE 
	            WHEN first_ts_begin <= laycan_to AND last_ts_end >= laycan_from THEN 1 -- Within laycan
	            WHEN first_ts_begin > laycan_to THEN 2 -- After laycan (late)
	            WHEN last_ts_end < laycan_from THEN 3  -- Before laycan (early)
	            ELSE 4 -- Catch-all, unlikely needed
	        END AS priority,
			-- Complex calculation to find the closest temporal match
            -- Used as tie-breaker when multiple port stops have same priority
			LEAST(
				-- Negative overlap duration (penalizes long overlaps outside laycan)
				GREATEST(
		            0, 
		            EXTRACT(EPOCH FROM LEAST(laycan_to, last_ts_end) - GREATEST(laycan_from, first_ts_begin))
		        ) * -1,
				-- Smallest absolute time difference between any laycan/stop boundaries
				LEAST(
			        ABS(EXTRACT(EPOCH FROM (first_ts_begin - laycan_from)) / 86400),  	-- Arrival vs laycan start
			        ABS(EXTRACT(EPOCH FROM (first_ts_begin - laycan_to)) / 86400),		-- Arrival vs laycan end
			        ABS(EXTRACT(EPOCH FROM (last_ts_end - laycan_from)) / 86400),		-- Departure vs laycan start
			        ABS(EXTRACT(EPOCH FROM (last_ts_end - laycan_to)) / 86400)			-- Departure vs laycan end
		    	)
			) as closest_difference_in_days
		FROM fixtures.fixtures_data f
		INNER JOIN (SELECT v.vessel_name, ps.* FROM data_analysis.port_stops_grouped ps INNER JOIN ais.vessel v on v.id = ps.vessel_id) ps
			ON ps.vessel_name = f.vessel_name 
			-- Temporal matching with buffer: port stop should be within 4 days of laycan window
			AND ps.first_ts_begin <= (f.laycan_to + INTERVAL '4 days')  --Stop starts before laycan end + buffer		/* Change the interval here */
			AND ps.last_ts_end >= (f.laycan_from - INTERVAL '4 days')	-- Stop ends after laycan start - buffer		/* Change the interval here */
		INNER JOIN context_data.ports p on p.id = ps.port_id
		INNER JOIN context_data.countries c on c.id = p.country_id
		WHERE f.vessel_name NOT IN (SELECT vessel_name FROM ais.vw_non_unique_vessel_names) 	-- Exclude vessels with non-unique names to avoid ambiguous matches
		)
	)
WHERE row_num = 1;

ALTER TABLE data_analysis.fixtures_to_port_stops ADD id SERIAL;

ALTER TABLE data_analysis.fixtures_to_port_stops ADD CONSTRAINT pk_fixtures_to_port_stops_id PRIMARY KEY(id);
