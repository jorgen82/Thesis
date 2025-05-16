/* Tracks with Port Destination but NOT a Port Origin */

CREATE VIEW data_analysis.vw_track_notport_to_port AS
SELECT t.id, t.vessel_id, v.vessel_name, v.vessel_type
	,p_to.port_name as to_port_name, p_to.country as to_country
	,group_id, to_port_stops_grouped_id, ts_start, ts_end
	,duration_h, avg_speed_kn, distance_nmi, direction
	,track
FROM data_analysis.tracks t
INNER JOIN ais.vessel v on v.id = t.vessel_id
INNER JOIN data_analysis.port_stops_grouped ps_to on ps_to.id = t.to_port_stops_grouped_id
INNER JOIN context_data.ports p_to on p_to.id = ps_to.port_id
WHERE from_port_stops_grouped_id IS NULL
	AND to_port_stops_grouped_id IS NOT NULL
