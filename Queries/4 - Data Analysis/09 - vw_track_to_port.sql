/* Tracks with a Port Destination */

CREATE VIEW data_analysis.vw_track_to_port AS
SELECT t.id, t.vessel_id, v.vessel_name, v.vessel_type
	,p_from.port_name as from_port_name, p_from.country as from_country, p_to.port_name as to_port_name, p_to.country as to_country
	,CASE WHEN p_from.country = p_to.country THEN 1 ELSE 0 END as is_intra_country_track
	,group_id, from_port_stops_grouped_id, to_port_stops_grouped_id, ts_start, ts_end
	,duration_h, avg_speed_kn, distance_nmi, direction
	--,track
FROM data_analysis.tracks t
INNER JOIN ais.vessel v on v.id = t.vessel_id
INNER JOIN data_analysis.port_stops_grouped ps_from on ps_from.id = t.from_port_stops_grouped_id
INNER JOIN data_analysis.port_stops_grouped ps_to on ps_to.id = t.to_port_stops_grouped_id
INNER JOIN context_data.ports p_from on p_from.id = ps_from.port_id
INNER JOIN context_data.ports p_to on p_to.id = ps_to.port_id
WHERE to_port_stops_grouped_id IS NOT NULL
	AND p_from.id != p_to.id
