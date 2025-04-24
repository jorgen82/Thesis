/*
	Here we identify the waiting areas.
	At first we find the stops that do not exist in the port stops.
	Then we perform a DBSCAN using as minPoints = 10 and epd = 3000.
*/

CREATE TABLE data_analysis.waiting_areas AS
SELECT *
FROM (
	SELECT S.vessel_id, S.ts_begin, S.ts_end, S.duration_s, S.nb_pos, S.centr, S.avg_dist_centroid, S.max_dist_centroid, S.id as stop_id
		,ST_ClusterDBSCAN(ST_Transform(S.centr, 3857) ,eps := 3000, minpoints := 10) OVER() + 1  AS cid_dbscan
	FROM data_analysis.stops S
	LEFT JOIN data_analysis.port_stops PS ON PS.vessel_id = S.vessel_id
		AND PS.ts_begin = S.ts_begin
		AND PS.ts_end = S.ts_end 
	WHERE PS.id is NULL
)x
WHERE cid_dbscan IS NOT NULL;  

ALTER TABLE data_analysis.waiting_areas ADD ID bigserial;

CREATE INDEX idx_waiting_areas_stop_id ON data_analysis.waiting_areas(stop_id);
CREATE INDEX idx_waiting_areas_cid_dbscan ON data_analysis.waiting_areas(cid_dbscan);
CREATE INDEX idx_waiting_areas_centr ON data_analysis.waiting_areas USING gist (centr);


