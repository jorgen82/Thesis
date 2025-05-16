/* This function will return the number of fixtures to port stops matches, for different number of +/- laycan days */

CREATE OR REPLACE FUNCTION data_analysis.fn_fixtures_to_port_stops_laycan_interval()
RETURNS TABLE (iteration_number INT, count_result BIGINT) AS 
$$
BEGIN
    RETURN QUERY 
    WITH RECURSIVE iter(i) AS (
        SELECT 1 
        UNION ALL
        SELECT i + 1 FROM iter WHERE i < 8
    )
    SELECT iter.i AS iteration_number, (
        SELECT COUNT(*)
        FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (PARTITION BY fixture_id ORDER BY priority, closest_difference_in_days) AS row_num
            FROM (
                SELECT 
                    f.id AS fixture_id,
                    f.vessel_id,
                    p.id AS port_id,
                    ps.id AS port_stops_grouped_id,
                    f.vessel_name,
                    f.laycan_from,
                    f.laycan_to,
                    ps.first_ts_begin, 
                    ps.last_ts_end,  
                    f.port_load AS fixture_port_load,
                    f.country AS fixture_country,
                    ps.port_name AS port_stop_port_name,
                    c.country AS port_stop_country,
                    ps.total_duration_h,
                    CASE WHEN ps.first_ts_begin >= f.laycan_from AND ps.first_ts_begin <= f.laycan_to THEN 1 ELSE 0 END AS onTime,
                    CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_from)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_from_days,
                    CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_to)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_to_days,
                    CASE 
                        WHEN first_ts_begin <= laycan_to AND last_ts_end >= laycan_from THEN 1 
                        WHEN first_ts_begin > laycan_to THEN 2 
                        WHEN last_ts_end < laycan_from THEN 3 
                        ELSE 4 
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
                    ) AS closest_difference_in_days
                FROM fixtures.fixtures_data f
                INNER JOIN (
                    SELECT v.vessel_name, ps.* 
                    FROM data_analysis.port_stops_grouped ps 
                    INNER JOIN ais.vessel v ON v.id = ps.vessel_id
                ) ps
                    ON ps.vessel_name = f.vessel_name
                    AND ps.first_ts_begin <= (f.laycan_to + (iter.i || ' days')::INTERVAL)
                    AND ps.last_ts_end >= (f.laycan_from - (iter.i || ' days')::INTERVAL)
                INNER JOIN context_data.ports p ON p.id = ps.port_id
                INNER JOIN context_data.countries c ON c.id = p.country_id
                WHERE f.vessel_name NOT IN (SELECT vessel_name FROM ais.vw_non_unique_vessel_names)
            ) subquery
        ) subquery_filtered
        WHERE row_num = 1
    ) AS count_result
    FROM iter;
END;
$$ LANGUAGE plpgsql;


--SELECT * FROM data_analysis.fn_fixtures_to_port_stops_laycan_interval()
