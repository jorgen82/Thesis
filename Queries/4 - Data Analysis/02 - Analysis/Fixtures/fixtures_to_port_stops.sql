/*********************************************************************************************************/
/******************************************* First Analysis **********************************************/
/*********************************************************************************************************/
/** This block is used in Section 5.1.1. It generates summary metrics describing the quality, 
    completeness, and timing performance of fixtures matched to AIS-derived port stops. 
    Results help evaluate how well the commercial data aligns with operational vessel behaviour. */
/*********************************************************************************************************/


-- Ensure the pg_trgm extension exists (required for similarity() function used in country matching checks)
DO $$  
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE EXTENSION pg_trgm;
    END IF;
END $$;


-- Generate a series of fixture-related metrics using UNION to stack results as rows.
SELECT *
FROM (
	/* ---------------------------------------------------------------------------------------------- */
	-- 1. Count all fixtures available in the raw dataset.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 1 as "#", 'Total fixtures data' as metric, count(*) as value   
	FROM fixtures.fixtures_data
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 2. Count fixtures successfully matched to port stops (± 4 days from laycan window).
	--    This reflects how many commercial contracts can be linked to observed vessel movements.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 2, 'Total fixtures matched to port stop data (+/- 4 days from laycan)' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 3. Identify mismatches between the fixture's country and the country inferred from AIS port stop.
	--    similarity() < 0.6 flags cases where names differ significantly (likely incorrect or ambiguous match).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 3, 'Port Calls Matched to Different Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE fixture_country != 'Unknown'
		AND fixture_country != port_stop_country
		AND similarity(fixture_country, port_stop_country) < 0.6
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 4. Count fixtures that were "on time" (arrival falls within laycan window).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 4, 'On Time Port Calls' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 5. On-time arrivals where fixture country and port-stop country disagree.
	--    Indicates potential fixture inconsistencies despite operational alignment.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 5, 'On Time Port Calls - Different Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
		AND fixture_country != 'Unknown'
		AND fixture_country != port_stop_country
		AND similarity(fixture_country, port_stop_country) < 0.6
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 6. Average days after laycan_from for arrivals classified as on time.
	--    Positive value → arrived after laycan_from but still within window.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 6, 'On Time Port Calls - Days After Laycan From' as metric, CAST(AVG(ontime_arrival_after_laycan_from) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 7. Same as above, but only for fixtures with short laycan periods (< 8 days).
	--    These contracts tend to be more sensitive to timing variability.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 7, 'On Time Port Calls - Days After Laycan From (fixtures with less than 8 days laycan)' as metric, CAST(AVG(ontime_arrival_after_laycan_from) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
		AND extract(days from laycan_to - laycan_from) + 1 < 8
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 8. Average days before laycan_to for on-time arrivals.
	--    Higher values mean ships arrived well before the upper bound of their laycan window.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 8, 'On Time Port Calls - Days Before Laycan To' as metric, CAST(AVG(ontime_arrival_before_laycan_to) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 9. Same as above but limited to shorter laycan windows (< 8 days).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 9, 'On Time Port Calls - Days Before Laycan To (fixtures with less than 8 days laycan)' as metric, CAST(AVG(ontime_arrival_before_laycan_to) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
		AND extract(days from laycan_to - laycan_from) + 1 < 8
	UNION	
	/* ---------------------------------------------------------------------------------------------- */
	-- 10. Count fixtures that arrived outside their laycan window (late or early).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 10, 'Not On Time Port Calls' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 0
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 11. Not-on-time fixtures where fixture country does not match port-stop country.
	--     Helps identify possible noise or inconsistent records.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 11, 'Not On Time Port Calls - Different Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 0
		AND fixture_country != 'Unknown'
		AND fixture_country != port_stop_country
		AND similarity(fixture_country, port_stop_country) < 0.6
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 12. Fixtures with unknown country (low-quality or incomplete records).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 12, 'Port calls with Unknown Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE fixture_country = 'Unknown'
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 13. Count fixtures with early arrivals (arrival happens before laycan_from).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 13, 'Fixtures with Early Arrival' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE early_arrival_days IS NOT NULL
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 14. Average number of days vessels arrived early.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 14, 'Fixtures with Early Arrival - Average Days' as metric, CAST(AVG(ABS(early_arrival_days)) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 15. Count fixtures where vessels missed the laycan period entirely (late arrival).
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 15, 'Fixtures with Missed Laycan' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE late_laycan_days IS NOT NULL
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 16. Average number of days vessels arrived after the laycan_to date.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 16, 'Fixtures with Missed Laycan - Average Days' as metric, CAST(AVG(late_laycan_days) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 17. Average laycan duration (days) for all fixtures.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 17 as "#", 'Average Laycan Period Days' as metric, CAST(AVG(EXTRACT(epoch from laycan_to - laycan_from) / 86400) as decimal(6,3)) as value
	FROM fixtures.fixtures_data
	UNION
	/* ---------------------------------------------------------------------------------------------- */
	-- 18–21. Laycan statistics calculated only for fixtures successfully matched to port stops.
	--       Includes average, minimum, maximum, and standard deviation.
	/* ---------------------------------------------------------------------------------------------- */
	SELECT 18 as "#", 'Average Laycan Period Days on Port Stop Matched Data' as metric, CAST(AVG(EXTRACT(epoch from laycan_to - laycan_from) / 86400) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	SELECT 19 as "#", 'Min Laycan Period Days on Port Stop Matched Data' as metric, CAST(MIN(EXTRACT(epoch from laycan_to - laycan_from) / 86400) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	SELECT 20 as "#", 'Max Laycan Period Days on Port Stop Matched Data' as metric, CAST(MAX(EXTRACT(epoch from laycan_to - laycan_from) / 86400) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	SELECT 21 as "#", 'StDev Laycan Period Days on Port Stop Matched Data' as metric, CAST(stddev(EXTRACT(epoch from laycan_to - laycan_from) / 86400) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
)
ORDER BY 1
