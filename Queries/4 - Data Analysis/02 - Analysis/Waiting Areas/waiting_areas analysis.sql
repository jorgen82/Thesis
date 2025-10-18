
/* We investigate which areas are utilized the most, based on Areas stops and Waiting Hours */
WITH base AS (  -- Fetch basic stats for the traffic on waiting areas belonging to a specific port (in this case Galveston, you may change it to investigate another area)
    SELECT p.port_name,
        t.waiting_areas_cluster_id,
        CAST(SUM(t.waiting_minutes)/60 AS DECIMAL(8,2)) AS total_waiting_hours,
        CAST((SUM(t.waiting_minutes)/60) / COUNT(t.vessel_id) AS DECIMAL(9,3)) AS avg_waiting_hours,
        SUM(t.nb_points) AS total_nb_points,
        COUNT(t.vessel_id) AS total_stops
    FROM data_analysis.waiting_areas_traffic t
    INNER JOIN context_data.ports p ON p.id = t.to_port_id
    WHERE p.port_name = 'GALVESTON'   -- Change the port to the one you would like to investigate
    GROUP BY p.port_name, t.waiting_areas_cluster_id
),
pct AS ( -- total stops and total hours of each waiting area, but the totals of the specific port (pct of an area over the port where it belongs to)
    SELECT *,
        CAST(total_stops * 1.0 / SUM(total_stops) OVER (PARTITION BY port_name) AS DECIMAL(5,4)) AS total_stops_pct, 
        CAST(total_waiting_hours * 1.0 / SUM(total_waiting_hours) OVER (PARTITION BY port_name) AS DECIMAL(5,4)) AS total_waiting_hours_pct
    FROM base
),
cumulative AS ( -- commulative total stops and waiting areas, in descenting order. This will make the investigation easier (eg we want to see which areas are used to server the 80% of the traffic)
    SELECT *,
        SUM(total_stops_pct) OVER (PARTITION BY port_name ORDER BY total_stops DESC) AS stops_cumulative_pct,
        SUM(total_waiting_hours_pct) OVER (PARTITION BY port_name ORDER BY total_waiting_hours DESC) AS waiting_hours_cumulative_pct
    FROM pct
),
cumulative_compare AS (  -- a comparison to see how much this cummulative number is changing. This will help in cases you would like to investigate areas which will make small difference (eg less than 10% of change in port stops)
    SELECT *,
        CAST((stops_cumulative_pct - LAG(stops_cumulative_pct) OVER (PARTITION BY port_name ORDER BY total_stops DESC)) / LAG(stops_cumulative_pct) OVER (PARTITION BY port_name ORDER BY total_stops DESC)  as DECIMAL(5,2)) as stops_cumulative_pct_increase,
        CAST((waiting_hours_cumulative_pct - LAG(waiting_hours_cumulative_pct) OVER (PARTITION BY port_name ORDER BY total_waiting_hours DESC)) / LAG(waiting_hours_cumulative_pct) OVER (PARTITION BY port_name ORDER BY total_waiting_hours DESC)  as DECIMAL(5,2)) as waiting_hours_cumulative_pct_increase
    FROM cumulative
)
SELECT *
FROM cumulative_compare
--WHERE stops_cumulative_pct_increase >= 0.15
ORDER BY total_stops DESC;


-- Concave Hulls of Waiting Areas in specific region (filtered by ports)
SELECT t.waiting_areas_cluster_id, CAST((SUM(t.waiting_minutes) / count(t.vessel_id)) / 60 as decimal(9,3)) as avg_waiting_hours, count(*) as total_tracks, wa.concave_hull
FROM data_analysis.waiting_areas_traffic t
INNER JOIN data_analysis.waiting_areas wa on wa.cid_dbscan = t.waiting_areas_cluster_id
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('GALVESTON', 'HOUSTON', 'DEER PARK', 'PASADENA', 'SINCO', 'NORSWORTHY', 'BAYTOWN', 'TEXAS CITY')
GROUP BY t.waiting_areas_cluster_id, wa.concave_hull


-- Metrics for vessel heading to Galveston Area. Used in Section 5.3.5)
with galveston_areas as (SELECT wa.cid_dbscan
    FROM data_analysis.waiting_areas wa
    INNER JOIN data_analysis.ports_voronoi pv ON ST_Within(wa.centr, pv.voronoi_zone)
    INNER JOIN context_data.ports p ON p.id = pv.port_id
    WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
)
,track_totals as (
    SELECT DISTINCT track_id, waiting_areas_total
    FROM data_analysis.waiting_areas_traffic
    WHERE waiting_areas_total !=1
)
SELECT 'Total Port to Port Tracks' as "Metric", count(distinct t.track_id) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
UNION ALL
SELECT 'Tracks with only 1 utilized waiting area', count(distinct t.track_id) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_total = 1
UNION ALL
SELECT 'Tracks with more than 1 utilized waiting area', count(distinct t.track_id) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_total != 1
UNION ALL
SELECT 'Tracks with only 1 utilized waiting area which is in Galveston', count(distinct t.track_id) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_total =1
    AND waiting_areas_cluster_id in (select cid_dbscan from galveston_areas)
UNION ALL
SELECT 'Tracks with only 1 utilized waiting area which is NOT in Galveston', count(distinct t.track_id) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_total =1
    AND waiting_areas_cluster_id not in (select cid_dbscan from galveston_areas)
UNION ALL
SELECT 'Tracks with more than 1 utilized waiting area All only in Galveston', count(distinct X.track_id) as "Value"
FROM (
    SELECT t.track_id, count(*) as cnt
    FROM data_analysis.waiting_areas_traffic t
    INNER JOIN context_data.ports p on p.id = t.to_port_id
    WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
        AND t.from_port_stops_grouped_id IS NOT NULL
        AND waiting_areas_total !=1
        AND waiting_areas_cluster_id in (select cid_dbscan from galveston_areas)
    GROUP BY t.track_id
) X
INNER JOIN track_totals on track_totals.track_id = X.track_id and track_totals.waiting_areas_total = X.cnt
UNION ALL
SELECT 'Tracks with more than 1 utilized waiting area inlcuding NOT Galveston areas', count(distinct t.track_id) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_total !=1
    AND waiting_areas_cluster_id not in (select cid_dbscan from galveston_areas)
UNION ALL
SELECT 'Avg Waiting Hours for ALL tracks', CAST(avg(waiting_minutes) / 60 as decimal (6,2)) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
UNION ALL
SELECT 'Avg Waiting Hours for Galveston Areas', CAST(avg(waiting_minutes) / 60 as decimal (6,2)) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_cluster_id in (select cid_dbscan from galveston_areas)
UNION ALL
SELECT 'Avg Waiting Hours for NON Galveston Areas', CAST(avg(waiting_minutes) / 60 as decimal (6,2)) as "Value"
FROM data_analysis.waiting_areas_traffic t
INNER JOIN context_data.ports p on p.id = t.to_port_id
WHERE p.port_name in ('CORPUS CHRISTI', 'PORT INGLESIDE', 'PORT ARANSAS', 'ROCKPORT')
    AND t.from_port_stops_grouped_id IS NOT NULL
    AND waiting_areas_cluster_id not in (select cid_dbscan from galveston_areas)
