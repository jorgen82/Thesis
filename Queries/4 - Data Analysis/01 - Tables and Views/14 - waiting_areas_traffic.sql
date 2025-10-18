/*
    Combine tracks with waiting areas
    This can provides us info like how many tracks stops at a waiting area and for how long
*/

CREATE TABLE data_analysis.waiting_areas_traffic AS
WITH waiting_points_grouped as (
    SELECT t.vessel_id, t.id as track_id, wa.cid_dbscan as waiting_areas_cluster_id, t.from_port_stops_grouped_id, t.to_port_stops_grouped_id, from_port.port_id as from_port_id, to_port.port_id as to_port_id,
        MIN(a.ts) as min_ts, MAX(a.ts) as max_ts, --First and last timestamp in waiting area
        CAST(EXTRACT(epoch from MAX(a.ts) - MIN(a.ts)) / 60 as decimal(10,2)) as waiting_minutes, --Total waiting time in minutes
        count(*) as nb_points  --Count of AIS points in this waiting period
    FROM data_analysis.tracks t
    --Join with AIS data to get vessel positions during the track
    INNER JOIN ais.ais a ON a.vessel_id = t.vessel_id
        AND a.ts >= t.ts_start  --Only consider points during the track period
        AND a.ts < t.ts_end
    --Filter for points that fall within predefined waiting areas using spatial containment
    INNER JOIN data_analysis.waiting_areas wa ON ST_Within(a.geom, wa.concave_hull)
    --Get port information for the track's origin and destination (optional joins)
    LEFT JOIN data_analysis.port_stops_grouped from_port ON from_port.id = t.from_port_stops_grouped_id
    LEFT JOIN data_analysis.port_stops_grouped to_port ON to_port.id = t.to_port_stops_grouped_id
    WHERE a.speed_kn <= 0.1  --Only consider points where vessel is essentially stationary (speed <= 0.1 knots)
    GROUP BY t.vessel_id, t.id, wa.cid_dbscan, t.from_port_stops_grouped_id, t.to_port_stops_grouped_id, from_port.port_id, to_port.port_id
)

SELECT *
    ,row_number() OVER (PARTITION BY track_id ORDER BY min_ts) as waiting_areas_num
    ,count(*) OVER (PARTITION BY track_id) as waiting_areas_total
FROM waiting_points_grouped;


