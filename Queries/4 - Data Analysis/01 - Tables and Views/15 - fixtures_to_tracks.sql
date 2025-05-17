/* 
  We will match the Fixtures which have a respective port stop, with the tracks.
  We will also do a "separation" of the track at the point of fixture, in order to compute some measures before and after the vessel got fixed.
*/

CREATE TABLE data_analysis.fixtures_to_tracks AS 
	SELECT fps.id as fixtures_to_port_stops_id, t.id as track_id, fps.fixture_id, fps.vessel_id, f.fixture_date
		,fps.first_ts_begin as port_stop_start, fps.last_ts_end as port_stop_end, t.ts_start as track_start, t.ts_end as track_end
		,fps.port_id as fixtures_to_port_stops_port_id, ps_from.port_id as track_origin_port_id, ps_to.port_id as track_destination_port_id
		,calcs_origin_port.distance_from_origin_port_nmi, calcs_origin_port.travel_speed_from_origin_port_kn, calcs_destination_port.distance_to_destination_port_nmi, calcs_destination_port.travel_speed_to_destination_port_kn 
		,ais.geom, t.track
	FROM data_analysis.fixtures_to_port_stops fps
	INNER JOIN fixtures.fixtures_data f on f.id = fps.fixture_id
	INNER JOIN LATERAL (
		SELECT a.geom
		FROM ais.ais a
		WHERE a.vessel_id = fps.vessel_id
			AND ts::date = f.fixture_date
		ORDER BY a.ts
		LIMIT 1
	) ais ON (true)
	LEFT JOIN data_analysis.tracks t ON t.vessel_id = fps.vessel_id
		AND f.fixture_date >= t.ts_start
		AND f.fixture_date <= t.ts_end
	LEFT JOIN data_analysis.port_stops_grouped ps_from ON ps_from.id = t.from_port_stops_grouped_id
	LEFT JOIN data_analysis.port_stops_grouped ps_to ON ps_to.id = t.to_port_stops_grouped_id
	LEFT JOIN LATERAL (
		SELECT SUM(distance_nmi) as distance_from_origin_port_nmi --, AVG(CASE WHEN speed_kn > 1 THEN speed_kn END) as travel_speed_from_origin_port_kn
			,SUM(CASE WHEN row != 1 AND speed_kn > 0.1 THEN speed_kn END * duration_sec) / SUM(CASE WHEN row != 1 AND speed_kn > 0.1 THEN duration_sec END) as travel_speed_from_origin_port_kn
		FROM (
			SELECT CASE WHEN ps_from.id IS NOT NULL 
					THEN ST_DistanceSphere(geom, LEAD(geom) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)) / 1852
					ELSE null
				END as distance_nmi,
				CASE WHEN ps_from.id IS NOT NULL AND (LEAD(ts) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)) IS NOT NULL
					THEN (ST_DistanceSphere(geom, LEAD(geom) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)) / 1852) 
        				/ (EXTRACT(EPOCH FROM (LEAD(ts) OVER (PARTITION BY a.vessel_id ORDER BY a.ts) - ts)) / 3600) 
					ELSE null
				END AS speed_kn,
				duration_sec,
				ROW_NUMBER() OVER (PARTITION BY vessel_id ORDER BY ts) as row
			FROM ais.ais a
			WHERE a.vessel_id = fps.vessel_id
				AND a.ts >= t.ts_start
				AND a.ts <= f.fixture_date
			)
	) calcs_origin_port on (true)
	LEFT JOIN LATERAL (
		SELECT SUM(distance_nmi) as distance_to_destination_port_nmi --, AVG(CASE WHEN speed_kn > 1 THEN speed_kn END) as travel_speed_to_destination_port_kn
			,SUM(CASE WHEN speed_kn > 0.1 THEN speed_kn END * duration_sec) / SUM(CASE WHEN speed_kn > 0.1 THEN duration_sec END) as travel_speed_to_destination_port_kn
		FROM (
			SELECT CASE WHEN ps_to.id IS NOT NULL 
					THEN ST_DistanceSphere(geom, LEAD(geom) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)) / 1852
					ELSE null
				END as distance_nmi,
				CASE WHEN ps_to.id IS NOT NULL AND (LEAD(ts) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)) IS NOT NULL
					THEN (ST_DistanceSphere(geom, LEAD(geom) OVER (PARTITION BY a.vessel_id ORDER BY a.ts)) / 1852) 
        				/ (EXTRACT(EPOCH FROM (LEAD(ts) OVER (PARTITION BY a.vessel_id ORDER BY a.ts) - ts)) / 3600) 
					ELSE null
				END AS speed_kn,
				duration_sec
			FROM ais.ais a
			WHERE a.vessel_id = fps.vessel_id
				AND a.ts >= f.fixture_date 
				AND a.ts <= t.ts_end
			)
	) calcs_destination_port on (true);




ALTER TABLE data_analysis.fixtures_to_tracks ADD COLUMN id bigserial;
ALTER TABLE data_analysis.fixtures_to_tracks ADD CONSTRAINT pk_fixtures_to_tracks_id PRIMARY KEY(id);
CREATE INDEX idx_fixtures_to_tracks_geom ON data_analysis.fixtures_to_tracks USING GIST (geom);
CREATE INDEX idx_fixtures_to_tracks_track ON data_analysis.fixtures_to_tracks USING GIST (track);
