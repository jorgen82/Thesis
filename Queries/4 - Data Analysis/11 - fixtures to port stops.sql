SELECT iteration_number as time_window, count_result as count
	,cast(((count_result - LAG(count_result) OVER (ORDER BY iteration_number)) / ((count_result + LAG(count_result) OVER (ORDER BY iteration_number))/.2) ) * 100 as decimal(5,2)) as pct_change
FROM data_analysis.fn_fixtures_to_port_stops_laycan_interval()

--DROP TABLE data_analysis.fixtures_to_port_stops;

--CREATE TABLE data_analysis.fixtures_to_port_stops AS
SELECT fixture_id, vessel_id, port_id, port_stops_grouped_id, vessel_name, laycan_from, laycan_to, first_ts_begin, last_ts_end, fixture_port_load, fixture_country, port_stop_port_name, port_stop_country,
	total_duration_h, onTime, arrival_after_laycan_from_days, arrival_after_laycan_to_days,
	CASE WHEN arrival_after_laycan_from_days < 0 THEN arrival_after_laycan_from_days END AS early_arrival_days,
	CASE WHEN arrival_after_laycan_to_days > 0 THEN arrival_after_laycan_to_days END AS late_laycan_days,
	CASE WHEN onTime = 1 AND arrival_after_laycan_from_days > 0 THEN arrival_after_laycan_from_days END AS ontime_arrival_after_laycan_from,
	CASE WHEN onTime = 1 AND arrival_after_laycan_to_days < 0 THEN ABS(arrival_after_laycan_to_days) END AS ontime_arrival_before_laycan_to
FROM
	(
	SELECT *
		--,ROW_NUMBER() OVER (PARTITION BY fixture_id ORDER BY distance_from_laycan_days) as row_num
		,ROW_NUMBER() OVER (PARTITION BY fixture_id ORDER BY priority, closest_difference_in_days) as row_num
	FROM 
		(
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
			--CASE WHEN ps.first_ts_begin <= f.laycan_to AND ps.last_ts_end >= f.laycan_from THEN 1 ELSE 0 END AS onTime,
			CASE WHEN ps.first_ts_begin >= f.laycan_from AND ps.first_ts_begin <= f.laycan_to THEN 1 ELSE 0 END AS onTime,
			CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_from)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_from_days,--diff_laycanfrom_arrivalbegin,
			--CAST(EXTRACT(EPOCH FROM (f.laycan_from - ps.last_ts_end)) / 86400 AS DECIMAL(5,2)) AS diff_laycanfrom_arrivalend,--diff_laycanfrom_arrivalend,
			CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_to)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_to_days, --diff_arrivalbegin_laycanto,
			CASE 
	            WHEN first_ts_begin <= laycan_to AND last_ts_end >= laycan_from THEN 1 -- Within laycan
	            WHEN first_ts_begin > laycan_to THEN 2 -- After laycan
	            WHEN last_ts_end < laycan_from THEN 3 -- Before laycan
	            ELSE 4 -- Catch-all, unlikely needed
	        END AS priority,
			LEAST(
				GREATEST(
		            0, 
		            EXTRACT(EPOCH FROM LEAST(laycan_to, last_ts_end) - GREATEST(laycan_from, first_ts_begin))
		        ) * -1,
				LEAST(
			        ABS(EXTRACT(EPOCH FROM (first_ts_begin - laycan_from)) / 86400),
			        ABS(EXTRACT(EPOCH FROM (first_ts_begin - laycan_to)) / 86400),
			        ABS(EXTRACT(EPOCH FROM (last_ts_end - laycan_from)) / 86400),
			        ABS(EXTRACT(EPOCH FROM (last_ts_end - laycan_to)) / 86400)
		    	)
			) as closest_difference_in_days
		FROM fixtures.fixtures_data f
		INNER JOIN (SELECT v.vessel_name, ps.* FROM data_analysis.port_stops_grouped ps INNER JOIN ais.vessel v on v.id = ps.vessel_id) ps
			ON ps.vessel_name = f.vessel_name --pd.vessel_id = f.vessel_id
			AND ps.first_ts_begin <= (f.laycan_to + INTERVAL '4 days')
			AND ps.last_ts_end >= (f.laycan_from - INTERVAL '4 days')
		INNER JOIN context_data.ports p on p.id = ps.port_id
		INNER JOIN context_data.countries c on c.id = p.country_id
		WHERE f.vessel_name NOT IN (SELECT vessel_name FROM ais.vw_non_unique_vessel_names)
		)
	)
WHERE row_num = 1;

ALTER TABLE data_analysis.fixtures_to_port_stops ADD id SERIAL;

ALTER TABLE data_analysis.fixtures_to_port_stops ADD CONSTRAINT pk_fixtures_to_port_stops_id PRIMARY KEY(id);
