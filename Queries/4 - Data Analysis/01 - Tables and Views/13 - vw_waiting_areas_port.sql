/*
    We create 2 views
    In the 1st we map the Waiting Areas to a Port, baed on the Area centroid and the Voronoi Tesselation of the Port
    In the 2nd we will enxand the previous view, adding temporal information (year, quarter, month) 
*/

/*
    Match waiting areas to port, based on the area centroid.
    This will match with one port, even if the area might be big, and potentially serving more that one ports.
*/
CREATE VIEW data_analysis.vw_waiting_areas_port AS
SELECT pv.port_id, pv.port_name, pv.country,
    MIN(river_coastal) AS river_coastal, 
    COUNT(cid_dbscan) AS total_areas,
    SUM(nb_stops) AS vessels,
    MIN(min_duration_hours) AS min_duration_hours,
    MAX(max_duration_hours) AS max_duration_hours,
    AVG(avg_duration_hours) AS avg_duration_hours,
    SUM(area_km2) AS area_km2,
    AVG(area_km2) AS avg_area_km2,
    MIN(area_km2) AS min_area_km2,
    MAX(area_km2) AS max_area_km2,
    SUM(CASE WHEN area_km2 < 0.5 THEN 1 ELSE 0 END) AS areas_less_halfkm,
    SUM(nb_stops) / NULLIF(SUM(area_km2), 0) AS vessel_density,
    SUM(total_vessel_hours_waiting) AS total_vessel_hours_waiting,
    SUM(utilization_rate) AS utilization_rate,
    AVG(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1000
    ) AS port_distance_km,
    MIN(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1000
    ) AS min_port_distance_km,
    MAX(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1000
    ) AS max_port_distance_km,
    AVG(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1852
    ) AS port_distance_nm,
    SUM(
        (avg_duration_hours * nb_stops) / NULLIF(
            ST_Distance(
                geography(ST_SetSRID(wa.centr, 4326)),
                geography(ST_SetSRID(pv.geom, 4326))
            ) / 1000,
        0)
    ) AS port_congestion_score
FROM data_analysis.waiting_areas wa
INNER JOIN data_analysis.ports_voronoi pv ON ST_Within(wa.centr, pv.voronoi_zone)
INNER JOIN context_data.ports p ON p.id = pv.port_id
GROUP BY pv.port_id, pv.port_name, pv.country;




/*
    Match waiting areas seasonal to port, based on the area centroid.
    This will match with one port, even if the area might be big, and potentially serving more that one ports.
*/

CREATE VIEW data_analysis.vw_waiting_areas_port_seasonal AS
SELECT pv.port_id, pv.port_name, pv.country,
    wa.temporal_cluster, wa."Year", wa.month_quarter, wa.year_month_quarter,
    MIN(river_coastal) AS river_coastal, 
    COUNT(cid_dbscan) AS total_areas,
    SUM(nb_stops) AS vessels,
    MIN(min_duration_hours) AS min_duration_hours,
    MAX(max_duration_hours) AS max_duration_hours,
    AVG(avg_duration_hours) AS avg_duration_hours,
    SUM(area_km2) AS area_km2,
    AVG(area_km2) AS avg_area_km2,
    MIN(area_km2) AS min_area_km2,
    MAX(area_km2) AS max_area_km2,
    SUM(CASE WHEN area_km2 < 0.5 THEN 1 ELSE 0 END) AS areas_less_halfkm,
    SUM(nb_stops) / NULLIF(SUM(area_km2), 0) AS vessel_density,
    SUM(total_vessel_hours_waiting) AS total_vessel_hours_waiting,
    SUM(utilization_rate) AS utilization_rate,
    AVG(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1000
    ) AS port_distance_km,
    MIN(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1000
    ) AS min_port_distance_km,
    MAX(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1000
    ) AS max_port_distance_km,
    AVG(
        ST_Distance(
            geography(ST_SetSRID(wa.centr, 4326)),
            geography(ST_SetSRID(pv.geom, 4326))
        ) / 1852
    ) AS port_distance_nm,
    SUM(
        (avg_duration_hours * nb_stops) / NULLIF(
            ST_Distance(
                geography(ST_SetSRID(wa.centr, 4326)),
                geography(ST_SetSRID(pv.geom, 4326))
            ) / 1000,
        0)
    ) AS port_congestion_score
FROM data_analysis.waiting_areas_seasonal wa
INNER JOIN data_analysis.ports_voronoi pv ON ST_Within(wa.centr, pv.voronoi_zone)
INNER JOIN context_data.ports p ON p.id = pv.port_id
GROUP BY pv.port_id, pv.port_name, pv.country, wa.temporal_cluster, wa."Year", wa.month_quarter, wa.year_month_quarter;




/* Archive Queries

/*
    Match waiting areas to port, based on the area geography
    This will match with more than one ports
*/

SELECT 
    agg.port_ids,
    ,agg.port_names,
    ,cid_dbscan, river_coastal, convex_hull, concave_hull, bounding_circle, centr, nb_stops, nb_vessels_distinct, min_duration_seconds, max_duration_seconds, avg_duration_seconds, min_duration_minutes
    ,max_duration_minutes, avg_duration_minutes,	min_duration_hours, max_duration_hours,avg_duration_hours, area_km2, vessel_density, total_vessel_hours_waiting, utilization_rate
    ,CAST(ST_Distance(geography(ST_SetSRID(wa.centr, 4326)), geography(ST_SetSRID(pv.geom, 4326))) / 1000 as decimal(10,4)) as port_distance_km
    ,CAST(ST_Distance(geography(ST_SetSRID(wa.centr, 4326)), geography(ST_SetSRID(pv.geom, 4326))) / 1852 as decimal(10,4)) as port_distance_nm
    --,(avg_duration_hours * vessel_density) / (ST_Distance(geography(ST_SetSRID(wa.centr, 4326)), geography(ST_SetSRID(pv.geom, 4326))) / 1000)  -- This metric will need to be calculated on port level, therefore it will provide as much lines as the matchin ports.
FROM data_analysis.waiting_areas wa
INNER JOIN data_analysis.ports_voronoi pv on ST_Intersects(wa.concave_hull, pv.voronoi_zone)
--INNER JOIN context_data.ports p on p.id = pv.port_id
INNER JOIN LATERAL (
    SELECT DISTINCT
        ARRAY_AGG(DISTINCT p.id ORDER BY p.id) AS port_ids,
        STRING_AGG(DISTINCT p.port_name, ' / ' ORDER BY p.port_name) AS port_names
    FROM data_analysis.ports_voronoi pv  
        INNER JOIN context_data.ports p on p.id = pv.port_id
    WHERE 
        ST_Intersects(wa.concave_hull, pv.voronoi_zone)
) agg ON TRUE



*/
