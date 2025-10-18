/*
    This view is used to identify potential continuation of tracks, for furhter analysis and investigation.
    We are grouping a track with its previous one if the current track is not starting from a port.
    Also the current and previous tracks should belong to different regions (one east and the other west).
    Finally we return the tracks where the days between those two are <= 80
*/


CREATE VIEW data_analysis.vw_tracks_coast_to_coast_check AS
--CTE to prepare track data with region analysis
WITH tracks AS(
    SELECT 
        --Basic track and vessel information
        tracks.vessel_id, 
        v.vessel_type, 
        tracks.id as track_id, 
        group_id, 
        from_port_stops_grouped_id, 
        to_port_stops_grouped_id, 
        points, 
        ts_start, 
        ts_end, 
        duration_h, 
        duration_h / 24 as duration_days, 
        avg_speed_m_s, 
        avg_speed_kn, 
        distance_m, 
        distance_nmi, 
        direction,
        previous_id as previous_track_id, 
        us_region, 
        days_from_previous_track,
        --Get the US region from the previous track using window function
        LAG(us_region) OVER (PARTITION BY tracks.vessel_id ORDER BY ts_start) as previous_us_region,
        --Flag tracks that need manual checking for potential coast-to-coast voyages
        CASE WHEN from_port_stops_grouped_id is null           --Missing origin port information
            AND days_from_previous_track <= 80                 --Reasonable time gap between tracks (<= 80 days)
            AND us_region != LAG(us_region) OVER (PARTITION BY tracks.vessel_id ORDER BY ts_start)  --Region changed from previous track
        THEN 1 ELSE 0 END as to_check                         --Flag as 1 if all conditions met
    FROM data_analysis.tracks
    LEFT JOIN ais.vessel v on v.id = tracks.vessel_id  -- Get vessel type information
)

-- Main SELECT that combines consecutive tracks for analysis
SELECT 
    t_cur.vessel_id, 
    ('{' || CAST(t_pre.track_id as varchar) || ', ' || CAST(t_cur.track_id as varchar)  || '}')::integer[] as tracks_id, --Create array of combined track IDs for reference
    ('{' || CAST(t_pre.group_id as varchar) || ', ' || CAST(t_cur.group_id as varchar) || '}')::integer[] as groups_id, --Create array of combined group IDs
    --Port information from the combined tracks
    t_pre.from_port_stops_grouped_id, 
    t_cur.to_port_stops_grouped_id, 
    -- Combined statistics
    t_pre.points + t_cur.points as points,                    	--Total AIS points in both tracks
    t_pre.ts_start,                                           	--Start time from first track
    t_cur.ts_end,                                             	--End time from second track
    t_pre.duration_h + t_cur.duration_h as duration_h,        	--Combined duration in hours
    t_pre.duration_days + t_cur.duration_days as duration_days, --Combined duration in days
    --Weighted average speed calculation based on number of points
    ((t_pre.points * t_pre.avg_speed_m_s) + (t_cur.points * t_cur.avg_speed_m_s)) / (t_pre.points + t_cur.points) as avg_speed_m_s,
    ((t_pre.points * t_pre.avg_speed_kn) + (t_cur.points * t_cur.avg_speed_kn)) / (t_pre.points + t_cur.points) as avg_speed_kn,
    t_pre.distance_m + t_cur.distance_m as distance_m,        --Total distance in meters
    t_pre.distance_nmi + t_cur.distance_nmi as distance_nmi,  --Total distance in nautical miles
    t_cur.track_id, 
    t_cur.previous_track_id, 
    t_pre.us_region || ' to ' || t_cur.us_region as us_region,  --Region transition description
    t_cur.days_from_previous_track as days_between_tracks,  	--Time gap between tracks (using pre-calculated value)
    --Categorize the time gap between tracks into groups for analysis
    CASE WHEN t_cur.days_from_previous_track >= 71 THEN '71-80'
        WHEN t_cur.days_from_previous_track >= 61 THEN '61-70'
        WHEN t_cur.days_from_previous_track >= 51 THEN '51-60'
        WHEN t_cur.days_from_previous_track >= 41 THEN '41-50'
        WHEN t_cur.days_from_previous_track >= 36 THEN '36-40'
        WHEN t_cur.days_from_previous_track >= 31 THEN '31-35'
        WHEN t_cur.days_from_previous_track >= 26 THEN '26-30'
        WHEN t_cur.days_from_previous_track >= 21 THEN '21-25'
        WHEN t_cur.days_from_previous_track >= 16 THEN '16-20'
        WHEN t_cur.days_from_previous_track >= 11 THEN '11-15'
        WHEN t_cur.days_from_previous_track >= 6 THEN '6-10'
        ELSE '0-5' 
    END as days_between_tracks_grouped
FROM tracks t_cur  --Current track (the one flagged for checking)
--Join with previous track to combine them for analysis
LEFT JOIN tracks t_pre ON t_pre.track_id = t_cur.previous_track_id 
WHERE t_cur.to_check = 1;  --Only include tracks that were flagged as needing verification

