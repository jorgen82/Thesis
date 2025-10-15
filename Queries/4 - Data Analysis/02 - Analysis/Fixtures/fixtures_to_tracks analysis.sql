
-- Basic Fixtures to Track Matching Information
SELECT 1 as "#", 'Fixtures Matched to Port Stop' as metric, COUNT(*) as value
FROM data_analysis.fixtures_to_port_stops
UNION
SELECT 2, 'Fixtures Matched to Track', COUNT(*)
FROM data_analysis.fixtures_to_tracks
UNION
SELECT 3, 'Fixtures Matched to Track - No Origin Port Info', COUNT(*)
FROM data_analysis.fixtures_to_tracks
WHERE track_origin_port_id is null
UNION
SELECT 4, 'Fixtures Matched to Track - No Destination Port Info', COUNT(*)
FROM data_analysis.fixtures_to_tracks
WHERE track_destination_port_id is null
UNION
SELECT 5, 'Fixtures Matched to Track - Port to Port', COUNT(*)
FROM data_analysis.fixtures_to_tracks
WHERE track_destination_port_id is not null
	AND track_destination_port_id is not null
ORDER BY 1


-- Calcualte some basic measures for All Tracks
SELECT v.vessel_type as "Vessel Type", count(*) as "Total Records"
	,AVG(distance_from_origin_port_nmi) as "Dist from Origin Port (nmi)", AVG(distance_to_destination_port_nmi) as "Dist to Destination Port (nmi)"
	,AVG(travel_speed_from_origin_port_kn) as "Travel Speed from Origin Port (kn)", AVG(travel_speed_to_destination_port_kn) as "Travel Speed to Destination Port (kn)"
FROM data_analysis.fixtures_to_tracks t
INNER JOIN ais.vessel v on v.id = t.vessel_id
GROUP BY v.vessel_type


-- Calcualte some basic measures for Port to Port Tracks
SELECT v.vessel_type, count(distinct t.track_id) as total_records
	,AVG(distance_from_origin_port_nmi) as distance_from_origin_port_nmi, AVG(distance_to_destination_port_nmi) as distance_to_destination_port_nmi
	,AVG(travel_speed_from_origin_port_kn) as travel_speed_from_origin_port_kn, AVG(travel_speed_to_destination_port_kn) as travel_speed_to_destination_port_kn
    ,SUM(CASE WHEN speed_kn >= 0.1 THEN duration_sec END) / 86400 as total_sailing_days
	,SUM(CASE WHEN speed_kn < 0.1 THEN duration_sec END) / 86400 as total_waiting_days
FROM data_analysis.fixtures_to_tracks t
INNER JOIN ais.vessel v on v.id = t.vessel_id
INNER JOIN ais.ais a on a.vessel_id = t.vessel_id and a.ts >= t.track_start and a.ts <= t.track_end
WHERE t.track_origin_port_id IS NOT NULL 
	AND t.track_destination_port_id IS NOT NULL
GROUP BY v.vessel_type;


