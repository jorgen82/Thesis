/* We will add the duration_sec column in ais.ais table. This is a calculation of the seconds between 2 consecutive points, the current and the previous one. */

/*********** Create duration_sec column */
ALTER TABLE ais.ais
ADD COLUMN duration_sec integer;

/*********** Update duration_sec all at once - Different approach bellow*/
WITH computed_durations AS (
    SELECT 
		id,
        vessel_id,
        ts,
        extract(epoch from ts) - extract(epoch from LAG(ts) OVER (PARTITION BY vessel_id ORDER BY ts)) AS duration_sec
    FROM ais.ais
)
UPDATE ais.ais AS a
SET duration_sec = c.duration_sec
FROM computed_durations AS c
WHERE a.id = c.id;



/* 	The below is a different approach to achive the same thing. 
	It will update vessel by vessel using transactions.
	This is helpful for slower computers, where the update process might fail

/*********** Update duration_sec vessel_id by vessel_id */

DO $$ 
DECLARE 
    v_id BIGINT;  -- Assuming vessel_id is a BIGINT, adjust accordingly
BEGIN
    FOR v_id IN (SELECT DISTINCT vessel_id FROM ais.ais ORDER BY vessel_id) 
    LOOP
        -- Update only for the current vessel_id
        UPDATE ais.ais AS a
        SET duration_sec = subquery.duration_sec
        FROM (
            SELECT 
				id,
                ts,
                extract(epoch from ts) - extract(epoch from LAG(ts) OVER (PARTITION BY vessel_id ORDER BY ts)) AS duration_sec
            FROM ais.ais
            WHERE vessel_id = v_id
        ) AS subquery
        WHERE a.vessel_id = v_id AND a.id = subquery.id;

        -- Commit after updating each vessel
        COMMIT;
    END LOOP;
END $$;

*/
