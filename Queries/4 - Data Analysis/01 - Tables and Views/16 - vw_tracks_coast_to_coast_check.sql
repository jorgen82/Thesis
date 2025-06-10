/*
    This view is used to identify potential continuation of tracks, for furhter analysis and investigation.
    We are grouping a track with its previous one if the current track is not starting from a port.
    Also the current and previous tracks should belong to different regions (one east and the other west).
    Finally we return the tracks where the days between those two are <= 80
*/

CREATE VIEW data_analysis.vw_tracks_coast_to_coast_check AS
WITH tracks AS(
	SELECT tracks.vessel_id, v.vessel_type, tracks.id as track_id, group_id, from_port_stops_grouped_id, to_port_stops_grouped_id, points, ts_start, ts_end, duration_h, duration_h / 24 as duration_days, avg_speed_m_s, avg_speed_kn, distance_m, distance_nmi, direction
		,previous_id as previous_track_id, us_region, days_from_previous_track
        ,LAG(us_region) OVER (PARTITION BY tracks.vessel_id ORDER BY ts_start) as previous_us_region
		,CASE WHEN from_port_stops_grouped_id is null 
			AND days_from_previous_track <= 80 
			AND us_region != LAG(us_region) OVER (PARTITION BY tracks.vessel_id ORDER BY ts_start)
		THEN 1 ELSE 0 END as to_check
	FROM data_analysis.tracks
	LEFT JOIN ais.vessel v on v.id = tracks.vessel_id
)

SELECT t_cur.vessel_id, '{' || CAST(t_pre.track_id as varchar) || ', ' || CAST(t_cur.track_id as varchar)  || '}'as tracks_id, '{' || CAST(t_pre.group_id as varchar) || ', ' || CAST(t_cur.group_id as varchar) || '}' as groups_id
    ,t_pre.from_port_stops_grouped_id, t_cur.to_port_stops_grouped_id, t_pre.points + t_cur.points as points
    ,t_pre.ts_start, t_cur.ts_end, t_pre.duration_h + t_cur.duration_h as duration_h, t_pre.duration_days + t_cur.duration_days as duration_days, ((t_pre.points * t_pre.avg_speed_m_s) + (t_cur.points * t_cur.avg_speed_m_s)) / (t_pre.points + t_cur.points) as avg_speed_m_s
    ,((t_pre.points * t_pre.avg_speed_kn) + (t_cur.points * t_cur.avg_speed_kn)) / (t_pre.points + t_cur.points) as avg_speed_kn
    ,t_pre.distance_m + t_cur.distance_m as distance_m, t_pre.distance_nmi + t_cur.distance_nmi as distance_nmi
    ,t_cur.track_id, t_cur.previous_track_id, t_pre.us_region || ' to ' || t_cur.us_region as us_region
    --,CAST((EXTRACT (epoch from t_cur.ts_start) - EXTRACT (epoch from t_pre.ts_end)) / 86400 as decimal(5,2)) as days_between_tracks
    ,t_cur.days_from_previous_track as days_between_tracks
FROM tracks t_cur
LEFT JOIN tracks t_pre ON t_pre.track_id = t_cur.previous_track_id 
WHERE t_cur.to_check = 1;

