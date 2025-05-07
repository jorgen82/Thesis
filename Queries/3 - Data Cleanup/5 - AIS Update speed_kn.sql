/* Create the speed_kn as a calculation based on the timestamp and coordinates, since speed_over_ground cannot be trusted  */

ALTER TABLE ais.ais
ADD COLUMN speed_kn decimal(10,2);

CREATE INDEX idx_ais_id ON ais.ais(id);


WITH speed AS (
	SELECT * 
		,CASE
			WHEN ts_cur = ts_pre AND speed_cur < 20 THEN speed_cur 
			WHEN ts_pre is null AND speed_cur < 20 then speed_cur
			ELSE (ST_DistanceSphere(p_pre, p_cur) / 1852) / (extract(epoch FROM (ts_cur - ts_pre)) / 3600) 
		END as speed_kn
	FROM (
		SELECT id
			,vessel_id
			,speed_over_ground AS speed_cur
			,ts AS ts_cur
			,LAG(ts) OVER (PARTITION BY vessel_id ORDER BY ts) AS ts_pre
			,geom AS p_cur
			,LAG(geom) OVER (PARTITION BY vessel_id ORDER BY ts) AS p_pre
		FROM ais.ais
		)
	)

UPDATE ais.ais A
SET speed_kn = S.speed_kn
FROM speed S
WHERE A.id = S.id;
