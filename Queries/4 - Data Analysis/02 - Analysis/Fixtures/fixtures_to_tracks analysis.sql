/* 
    This section provides summary statistics about how many fixtures could be matched to reconstructed vessel tracks. 
    It helps evaluate the strength of the link between commercial data (fixtures) and operational behaviour (tracks).
*/

-- Basic matching information for fixture-to-track linkage
SELECT 1 as "#", 'Fixtures Matched to Port Stop' as metric, COUNT(*) as value
FROM data_analysis.fixtures_to_port_stops
UNION
-- Count fixtures successfully matched to a vessel track
SELECT 2, 'Fixtures Matched to Track', COUNT(*)
FROM data_analysis.fixtures_to_tracks
UNION
-- Fixtures linked to a track but lacking origin port information (track_start ∉ port)
-- Indicates tracks that begin offshore, at anchorage, or where port-stop inference failed
SELECT 3, 'Fixtures Matched to Track - No Origin Port Info', COUNT(*)
FROM data_analysis.fixtures_to_tracks
WHERE track_origin_port_id is null
UNION
-- Fixtures linked to tracks missing a destination port-stop association
-- Often occurs when a vessel stops offshore or the track does not end in a port
SELECT 4, 'Fixtures Matched to Track - No Destination Port Info', COUNT(*)
FROM data_analysis.fixtures_to_tracks
WHERE track_destination_port_id is null
UNION
-- Fixtures linked to tracks where both origin and destination ports are known
-- Represents clean, interpretable port-to-port commercial voyages
SELECT 5, 'Fixtures Matched to Track - Port to Port', COUNT(*)
FROM data_analysis.fixtures_to_tracks
WHERE track_destination_port_id is not null
	AND track_destination_port_id is not null
ORDER BY 1


/*
    Calculate basic operational measures for ALL tracks linked to fixtures, grouped by vessel type.
    Measures include:
      - distance from origin port
      - distance to destination port
      - average travel speed near origin/destination
    These help characterise how vessels behave operationally around fixture periods.
*/
SELECT v.vessel_type as "Vessel Type", count(*) as "Total Records"
	,AVG(distance_from_origin_port_nmi) as "Dist from Origin Port (nmi)", AVG(distance_to_destination_port_nmi) as "Dist to Destination Port (nmi)"
	,AVG(travel_speed_from_origin_port_kn) as "Travel Speed from Origin Port (kn)", AVG(travel_speed_to_destination_port_kn) as "Travel Speed to Destination Port (kn)"
FROM data_analysis.fixtures_to_tracks t
INNER JOIN ais.vessel v ON v.id = t.vessel_id
GROUP BY v.vessel_type


/*
    This query calculates operational characteristics for CLEAN port-to-port tracks (i.e., tracks where both origin and destination ports are known). 
    Additional metrics here:
      - total sailing days   (speed >= 0.1 kn)
      - total waiting days   (speed <  0.1 kn)
    
    These allow comparison of travel vs waiting behaviour among vessel types.
*/
SELECT v.vessel_type, count(distinct t.track_id) as total_records
	,AVG(distance_from_origin_port_nmi) as distance_from_origin_port_nmi, AVG(distance_to_destination_port_nmi) as distance_to_destination_port_nmi  -- Distance metrics (precomputed in fixtures_to_tracks)
	,AVG(travel_speed_from_origin_port_kn) as travel_speed_from_origin_port_kn, AVG(travel_speed_to_destination_port_kn) as travel_speed_to_destination_port_kn  -- Speed metrics
    ,SUM(CASE WHEN speed_kn >= 0.1 THEN duration_sec END) / 86400 as total_sailing_days  -- Total sailing time (converted from seconds to days)
	,SUM(CASE WHEN speed_kn < 0.1 THEN duration_sec END) / 86400 as total_waiting_days   -- Total waiting/idle time (speed < 0.1 kn → drifting or anchored)
FROM data_analysis.fixtures_to_tracks t
INNER JOIN ais.vessel v ON v.id = t.vessel_id      -- Join vessel metadata
INNER JOIN ais.ais a ON a.vessel_id = t.vessel_id  -- Join AIS stream to compute sailing/waiting time within track duration
	AND a.ts >= t.track_start 
	AND a.ts <= t.track_end
WHERE t.track_origin_port_id IS NOT NULL   -- Restrict to valid port-to-port tracks only
	AND t.track_destination_port_id IS NOT NULL
GROUP BY v.vessel_type;


