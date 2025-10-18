/*
	We will see some details and measures on the Tracks
*/

/*********************************************************************************************************/
/*******************************************Based on Country *********************************************/
/*********************************************************************************************************/

SELECT from_country, to_country, COUNT(*) as count
FROM data_analysis.vw_track_port_to_port
GROUP BY from_country, to_country
ORDER BY 3 DESC

/* Traffic between BS or MX and US per Vessel Type. Used in Section 5.2.2 */

--Tracks from BS to US
SELECT from_country, to_country--, COUNT(*) as traffic
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_port_to_port
WHERE from_country = 'BS'
	AND to_country = 'US'
GROUP BY from_country, to_country
UNION
--Tracks from US to BS
SELECT from_country, to_country--, COUNT(*) as traffic
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_port_to_port
WHERE from_country = 'US'
	AND to_country = 'BS'
GROUP BY from_country, to_country
UNION
--Tracks from MX to US
SELECT from_country, to_country--, COUNT(*) as traffic
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_port_to_port
WHERE from_country = 'MX'
	AND to_country = 'US'
GROUP BY from_country, to_country
UNION
--Tracks from US to MX
SELECT from_country, to_country--, COUNT(*) as traffic
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_port_to_port
WHERE from_country = 'US'
	AND to_country = 'MX'
GROUP BY from_country, to_country




/*********************************************************************************************************/
/******************************** Tracks Starting and Endind to Port *************************************/
/*********************************************************************************************************/

/* Tracks based in Vessel Type. Used in Section 5.2.3  */
SELECT 1, vessel_type, count(*) as count
FROM data_analysis.vw_track_port_to_port
WHERE vessel_type IS NOT NULL
GROUP BY vessel_type
UNION
SELECT 2, 'No Vessel Type Info', count(*) as count
FROM data_analysis.vw_track_port_to_port
WHERE vessel_type IS NULL
GROUP BY vessel_type
UNION
SELECT 3, 'All Tracks', count(*) 
FROM data_analysis.tracks
ORDER BY 1,3 DESC


/* Port Connectivity */
SELECT from_country || ' - ' || from_port_name as "From", to_country || ' - ' || to_port_name as "To", COUNT(*) as count
FROM data_analysis.vw_track_port_to_port
GROUP BY from_country || ' - ' || from_port_name, to_country || ' - ' || to_port_name
ORDER BY 3 DESC


/* Origin Port Track Count */
SELECT from_country || ' - ' || from_port_name as "From", COUNT(*) as count
FROM data_analysis.vw_track_port_to_port 
GROUP BY from_country || ' - ' || from_port_name 
ORDER BY 2 DESC


/* Destination Port Track Count */
SELECT to_country || ' - ' || to_port_name as "To", COUNT(*) as count
FROM data_analysis.vw_track_port_to_port
GROUP BY to_country || ' - ' || to_port_name
ORDER BY 2 DESC


/* Most Visited Ports based on Tracks Starting and Ending to a Port */
SELECT COALESCE(fr.port, t.port) as port, COALESCE(count_from,0) + COALESCE(count_to,0) as count
FROM (  -- Tracks starting from a port
	SELECT from_country || ' - ' || from_port_name as port, COUNT(*) as count_from
	FROM data_analysis.vw_track_port_to_port 
	GROUP BY from_country || ' - ' || from_port_name
	) fr
FULL JOIN (  -- Tracks ending to a port
	SELECT to_country || ' - ' || to_port_name as port, COUNT(*) as count_to
	FROM data_analysis.vw_track_port_to_port
	GROUP BY to_country || ' - ' || to_port_name
	) t on fr.port = t.port
ORDER BY 2 DESC


/* Most Visited Ports based on All Tracks */
SELECT COALESCE(fr.port, t.port) as port, COALESCE(count_from,0) + COALESCE(count_to,0) as count
FROM ( -- Tracks starting from a port
	SELECT p.country || ' - ' || p.port_name as port, COUNT(*) as count_from
	FROM data_analysis.tracks t
	INNER JOIN data_analysis.port_stops_grouped g on g.id = t.from_port_stops_grouped_id
	INNER JOIN context_data.ports p on p.id = g.port_id
	WHERE from_port_stops_grouped_id != to_port_stops_grouped_id
	GROUP BY p.country || ' - ' || p.port_name
	) fr
FULL JOIN ( -- Tracks ending to a port
	SELECT p.country || ' - ' || p.port_name as port, COUNT(*) as count_to
	FROM data_analysis.tracks t
	INNER JOIN data_analysis.port_stops_grouped g on g.id = t.to_port_stops_grouped_id
	INNER JOIN context_data.ports p on p.id = g.port_id
	WHERE from_port_stops_grouped_id != to_port_stops_grouped_id
	GROUP BY p.country || ' - ' || p.port_name
	) t on fr.port = t.port
ORDER BY 2 DESC


/* Most Visited Ports by Year (based on All Tracks) */
WITH base_data AS (
    SELECT COALESCE(fr.port, t.port) AS port, 
           COALESCE(fr.year, t.year) AS year, 
           COALESCE(count_from, 0) + COALESCE(count_to, 0) AS count
    FROM ( -- Tracks starting from a port
        SELECT p.country || ' - ' || p.port_name AS port, 
               EXTRACT(YEAR FROM ts_start) AS year, 
               COUNT(*) AS count_from
        FROM data_analysis.tracks t
        INNER JOIN data_analysis.port_stops_grouped g 
            ON g.id = t.from_port_stops_grouped_id
        INNER JOIN context_data.ports p 
            ON p.id = g.port_id
        WHERE from_port_stops_grouped_id != to_port_stops_grouped_id
        GROUP BY p.country || ' - ' || p.port_name, EXTRACT(YEAR FROM ts_start)
    ) fr
    FULL JOIN ( -- Tracks ending to a port
        SELECT p.country || ' - ' || p.port_name AS port, 
               EXTRACT(YEAR FROM ts_end) AS year, 
               COUNT(*) AS count_to
        FROM data_analysis.tracks t
        INNER JOIN data_analysis.port_stops_grouped g 
            ON g.id = t.to_port_stops_grouped_id
        INNER JOIN context_data.ports p 
            ON p.id = g.port_id
        WHERE from_port_stops_grouped_id != to_port_stops_grouped_id
        GROUP BY p.country || ' - ' || p.port_name, EXTRACT(YEAR FROM ts_end)
    ) t 
    ON fr.port = t.port AND fr.year = t.year
),
pivot_data AS (
    SELECT 
        port,
		SUM(CASE WHEN year = 2019 THEN count ELSE 0 END) AS "2023",
        SUM(CASE WHEN year = 2020 THEN count ELSE 0 END) AS "2020",
        SUM(CASE WHEN year = 2021 THEN count ELSE 0 END) AS "2021",
        SUM(CASE WHEN year = 2022 THEN count ELSE 0 END) AS "2022",
        SUM(count) AS total -- Adding total column
    FROM base_data
    GROUP BY port
)
SELECT * FROM pivot_data
ORDER BY total DESC; -- Sort by total column



/* Comparison of the above - Used in Section 5.2.3 */
SELECT COALESCE(p.port, ptp.port) as port, p.count as port_traffic, ptp.count as port_to_port_traffic, CAST((ptp.count::numeric / p.count) as decimal(5,2)) as diff
FROM (  -- All tracks
	SELECT COALESCE(fr.port, t.port) as port, COALESCE(count_from,0) + COALESCE(count_to,0) as count
	FROM (  -- Oring Port
		SELECT p.country || ' - ' || p.port_name as port, COUNT(*) as count_from
		FROM data_analysis.tracks t
		INNER JOIN data_analysis.port_stops_grouped g on g.id = t.from_port_stops_grouped_id
		INNER JOIN context_data.ports p on p.id = g.port_id
		WHERE from_port_stops_grouped_id != to_port_stops_grouped_id
		GROUP BY p.country || ' - ' || p.port_name
		) fr
	FULL JOIN (  -- Destination Port
		SELECT p.country || ' - ' || p.port_name as port, COUNT(*) as count_to
		FROM data_analysis.tracks t
		INNER JOIN data_analysis.port_stops_grouped g on g.id = t.to_port_stops_grouped_id
		INNER JOIN context_data.ports p on p.id = g.port_id
		WHERE from_port_stops_grouped_id != to_port_stops_grouped_id
		GROUP BY p.country || ' - ' || p.port_name
		) t on fr.port = t.port
	) p
FULL JOIN  -- Tracks starting and ending to a port
	(
	SELECT COALESCE(fr.port, t.port) as port, COALESCE(count_from,0) + COALESCE(count_to,0) as count
	FROM ( -- Oring Port
		SELECT from_country || ' - ' || from_port_name as port, COUNT(*) as count_from
		FROM data_analysis.vw_track_port_to_port 
		GROUP BY from_country || ' - ' || from_port_name
		) fr
	FULL JOIN ( -- Destination Port
		SELECT to_country || ' - ' || to_port_name as port, COUNT(*) as count_to
		FROM data_analysis.vw_track_port_to_port
		GROUP BY to_country || ' - ' || to_port_name
		) t on fr.port = t.port
	) ptp ON p.port = ptp.port
ORDER BY 2 DESC


/* Destination Port Per Vessel Type */
SELECT to_country || ' - ' || to_port_name AS "To"
	,COUNT(*) AS Fleet
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_port_to_port
GROUP BY to_country || ' - ' || to_port_name
ORDER BY 2 DESC


/* Oring Port By Vessel Type */
SELECT from_country || ' - ' || from_port_name AS "To"
	,COUNT(*) AS Fleet
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_from_port
GROUP BY from_country || ' - ' || from_port_name
ORDER BY 2 DESC


/* Incoming and Outgoing Port Calls by Vessel Type based on Tracks having both oring and destination a port */
SELECT incoming_port as port, incoming_traffic + outgoing_traffic as traffic, incoming_traffic, outgoing_traffic
	,"Incoming - Aframax Tankers", "Incoming - Handysize Tankers", "Incoming - Panamax Tankers", "Incoming - Small Tanker (5-10K dwt)", "Incoming - Suezmax Tankers", "Incoming - ULCC-VLCC Tankers"
	,"Outgoing - Aframax Tankers", "Outgoing - Handysize Tankers", "Outgoing - Panamax Tankers", "Outgoing - Small Tanker (5-10K dwt)", "Outgoing - Suezmax Tankers", "Outgoing - ULCC-VLCC Tankers"
FROM (  -- Incoming Tracks
	SELECT to_country || ' - ' || to_port_name AS "incoming_port", COUNT(*) as incoming_traffic
		,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Incoming - Aframax Tankers"
		,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Incoming - Handysize Tankers" 
		,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Incoming - Panamax Tankers"
		,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Incoming - Small Tanker (5-10K dwt)"
		,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Incoming - Suezmax Tankers"
		,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "Incoming - ULCC-VLCC Tankers"
	FROM data_analysis.vw_track_port_to_port
	GROUP BY to_country || ' - ' || to_port_name
	) incoming
FULL JOIN (  -- Outgoing Tracks
	SELECT from_country || ' - ' || from_port_name AS "outgoing_port", COUNT(*) as outgoing_traffic
		,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Outgoing - Aframax Tankers"
		,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Outgoing - Handysize Tankers" 
		,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Outgoing - Panamax Tankers"
		,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Outgoing - Small Tanker (5-10K dwt)"
		,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Outgoing - Suezmax Tankers"
		,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "Outgoing - ULCC-VLCC Tankers"
	FROM data_analysis.vw_track_port_to_port
	GROUP BY from_country || ' - ' || from_port_name
	) outgoing ON outgoing.outgoing_port = incoming.incoming_port
WHERE outgoing.outgoing_port IS NOT NULL
	AND incoming.incoming_port IS NOT NULL
ORDER BY traffic DESC




/*********************************************************************************************************/
/************************* Tracks the Ends to a Port but with a non-port origin **************************/
/*********************************************************************************************************/

/* Some basic info - Used in Section 5.2.4 */
SELECT 1, vessel_type, count(*) as count
FROM data_analysis.vw_track_notport_to_port
WHERE vessel_type IS NOT NULL
GROUP BY vessel_type
UNION
SELECT 2, 'No Vessel Type Info', count(*) as count
FROM data_analysis.vw_track_notport_to_port
WHERE vessel_type IS NULL
GROUP BY vessel_type
UNION
SELECT 3, 'All Tracks', count(*) 
FROM data_analysis.tracks
ORDER BY 1,3 DESC


-- Tracks To Port
SELECT to_country || ' - ' || to_port_name as "To", COUNT(*) as count
FROM data_analysis.vw_track_port_to_port
GROUP BY to_country || ' - ' || to_port_name
ORDER BY 2 DESC

/* Destination Per Vessel Type */
SELECT to_country || ' - ' || to_port_name AS "To", COUNT(*) as traffic
	,COUNT(CASE WHEN vessel_type = 'Aframax Tankers' THEN 1 END) AS "Aframax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Handysize Tankers' THEN 1 END) AS "Handysize Tankers" 
	,COUNT(CASE WHEN vessel_type = 'Panamax Tankers' THEN 1 END) AS "Panamax Tankers"
	,COUNT(CASE WHEN vessel_type = 'Small Tanker (5-10K dwt)' THEN 1 END) AS "Small Tanker (5-10K dwt)"
	,COUNT(CASE WHEN vessel_type = 'Suezmax Tankers' THEN 1 END) AS "Suezmax Tankers"
	,COUNT(CASE WHEN vessel_type = 'ULCC-VLCC Tankers' THEN 1 END) AS "ULCC-VLCC Tankers"
FROM data_analysis.vw_track_notport_to_port
GROUP BY to_country || ' - ' || to_port_name
ORDER BY traffic DESC
