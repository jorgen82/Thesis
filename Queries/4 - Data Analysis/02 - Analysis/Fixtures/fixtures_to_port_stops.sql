/*********************************************************************************************************/
/******************************************* First Analysis **********************************************/
/*********************************************************************************************************/
/** Used in Section 5.1.1 ********************************************************************************/
/*********************************************************************************************************/

DO $$  -- This is used to install the pg_trgm if not exists. We will use it for the similarity function
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE EXTENSION pg_trgm;
    END IF;
END $$;


SELECT *
FROM (
	SELECT 1 as "#", 'Total fixtures data' as metric, count(*) as value
	FROM fixtures.fixtures_data
	UNION
	SELECT 2, 'Total fixtures matched to port stop data (+/- 4 days from laycan)' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	SELECT 3, 'Port Calls Matched to Different Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE fixture_country != 'Unknown'
		AND fixture_country != port_stop_country
		AND similarity(fixture_country, port_stop_country) < 0.6
	UNION
	SELECT 4, 'On Time Port Calls' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
	UNION
	SELECT 5, 'On Time Port Calls - Different Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
		AND fixture_country != 'Unknown'
		AND fixture_country != port_stop_country
		AND similarity(fixture_country, port_stop_country) < 0.6
	UNION
	SELECT 6, 'On Time Port Calls - Days After Laycan From' as metric, CAST(AVG(ontime_arrival_after_laycan_from) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
	UNION
	SELECT 7, 'On Time Port Calls - Days After Laycan From (fixtures with less than 8 days laycan)' as metric, CAST(AVG(ontime_arrival_after_laycan_from) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
		AND extract(days from laycan_to - laycan_from) + 1 < 8
	UNION
	SELECT 8, 'On Time Port Calls - Days Before Laycan To' as metric, CAST(AVG(ontime_arrival_before_laycan_to) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
	UNION
	SELECT 9, 'On Time Port Calls - Days Before Laycan To (fixtures with less than 8 days laycan)' as metric, CAST(AVG(ontime_arrival_before_laycan_to) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 1
		AND extract(days from laycan_to - laycan_from) + 1 < 8
	UNION	
	SELECT 10, 'Not On Time Port Calls' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 0
	UNION
	SELECT 11, 'Not On Time Port Calls - Different Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE ontime = 0
		AND fixture_country != 'Unknown'
		AND fixture_country != port_stop_country
		AND similarity(fixture_country, port_stop_country) < 0.6
	UNION
	SELECT 12, 'Port calls with Unknown Country' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE fixture_country = 'Unknown'
	UNION
	SELECT 13, 'Fixtures with Early Arrival' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE early_arrival_days IS NOT NULL
	UNION
	SELECT 14, 'Fixtures with Early Arrival - Average Days' as metric, CAST(AVG(ABS(early_arrival_days)) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	SELECT 15, 'Fixtures with Missed Laycan' as metric, count(*) as value
	FROM data_analysis.fixtures_to_port_stops
	WHERE late_laycan_days IS NOT NULL
	UNION
	SELECT 16, 'Fixtures with Missed Laycan - Average Days' as metric, CAST(AVG(late_laycan_days) as decimal(6,3)) as value
	FROM data_analysis.fixtures_to_port_stops
	UNION
	SELECT 17 as "#", 'Average Laycan Period Days' as metric, CAST(AVG(EXTRACT(epoch from laycan_to - laycan_from) / 86400) as decimal(6,3)) as value
	FROM fixtures.fixtures_data
	UNION
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
