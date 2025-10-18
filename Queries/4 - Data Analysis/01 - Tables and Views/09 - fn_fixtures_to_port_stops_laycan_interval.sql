/* This function will return the number of fixtures to port stops matches, for different number of +/- laycan days */

CREATE OR REPLACE FUNCTION data_analysis.fn_fixtures_to_port_stops_laycan_interval()
RETURNS TABLE (iteration_number INT, count_result BIGINT) AS 
$$
BEGIN
    RETURN QUERY 
    -- Recursive CTE to generate test iterations from 1 to 8 days
    -- Each iteration tests a different buffer size around laycan dates
    WITH RECURSIVE iter(i) AS (
        SELECT 1 
        UNION ALL
        SELECT i + 1 FROM iter WHERE i < 8
    )
    SELECT iter.i AS iteration_number, (
        -- Subquery that counts successful matches for current buffer size
        SELECT COUNT(*)
        FROM (
            SELECT *, 
                    -- Rank potential matches to select the best one per fixture
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
                    ps.first_ts_begin, -- Actual arrival time
                    ps.last_ts_end,  -- Actual departure time
                    f.port_load AS fixture_port_load,
                    f.country AS fixture_country,
                    ps.port_name AS port_stop_port_name,
                    c.country AS port_stop_country,
                    ps.total_duration_h,  -- Hours spent in port
                    -- On-time performance indicator
                    CASE WHEN ps.first_ts_begin >= f.laycan_from AND ps.first_ts_begin <= f.laycan_to THEN 1 ELSE 0 END AS onTime,
                    -- Timing differentials in days
                    CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_from)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_from_days,
                    CAST(EXTRACT(EPOCH FROM (ps.first_ts_begin - f.laycan_to)) / 86400 AS DECIMAL(5,2)) AS arrival_after_laycan_to_days,
                    -- Match priority system:
                        -- 1 = Best: Port stop overlaps with laycan window
                        -- 2 = Acceptable: Stop occurs after laycan end
                        -- 3 = Acceptable: Stop completes before laycan start  
                        -- 4 = Catch-all for edge cases
                    CASE 
                        WHEN first_ts_begin <= laycan_to AND last_ts_end >= laycan_from THEN 1 
                        WHEN first_ts_begin > laycan_to THEN 2 
                        WHEN last_ts_end < laycan_from THEN 3 
                        ELSE 4 
                    END AS priority,
                    -- Sophisticated tie-breaking calculation:
                    -- Finds the closest temporal proximity between laycan and port stop boundaries
                    LEAST(
                        -- Negative overlap duration (penalizes poor overlaps)
                        GREATEST(
                            0, 
                            EXTRACT(EPOCH FROM LEAST(laycan_to, last_ts_end) - GREATEST(laycan_from, first_ts_begin))
                        ) * -1,
                        -- Smallest absolute time difference between any boundary pairs
                        LEAST(
                            ABS(EXTRACT(EPOCH FROM (first_ts_begin - laycan_from)) / 86400),    -- Arrival vs laycan start
                            ABS(EXTRACT(EPOCH FROM (first_ts_begin - laycan_to)) / 86400),      -- Arrival vs laycan end 
                            ABS(EXTRACT(EPOCH FROM (last_ts_end - laycan_from)) / 86400),       -- Departure vs laycan start
                            ABS(EXTRACT(EPOCH FROM (last_ts_end - laycan_to)) / 86400)          -- Departure vs laycan end
                        )
                    ) AS closest_difference_in_days
                FROM fixtures.fixtures_data f
                -- Join with port stops (enriched with vessel names for matching)
                INNER JOIN (
                    SELECT v.vessel_name, ps.* 
                    FROM data_analysis.port_stops_grouped ps 
                    INNER JOIN ais.vessel v ON v.id = ps.vessel_id
                ) ps
                    ON ps.vessel_name = f.vessel_name
                    -- Dynamic temporal matching with current iteration's buffer. The buffer expands/shrinks based on iter.i (1-8 days)
                    AND ps.first_ts_begin <= (f.laycan_to + (iter.i || ' days')::INTERVAL) -- Stop starts before laycan end + buffer
                    AND ps.last_ts_end >= (f.laycan_from - (iter.i || ' days')::INTERVAL)  -- Stop ends after laycan start - buffer
                INNER JOIN context_data.ports p ON p.id = ps.port_id
                INNER JOIN context_data.countries c ON c.id = p.country_id
                WHERE f.vessel_name NOT IN (SELECT vessel_name FROM ais.vw_non_unique_vessel_names)  -- Data quality filter: exclude vessels with ambiguous names
            ) subquery
        ) subquery_filtered
        WHERE row_num = 1  -- Keep only the best match for each fixture (top-ranked by priority system)
    ) AS count_result
    FROM iter;
END;
$$ LANGUAGE plpgsql;


--SELECT * FROM data_analysis.fn_fixtures_to_port_stops_laycan_interval()
