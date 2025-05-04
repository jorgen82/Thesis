/*
With the below procedures we will Remove Duplicates and Outliers
We will use a table named test_ais in order to perform those procedures, avoiding messing with the base table
When this is done and if the valuation is good, you may replace the main ais table with the test_ais (this is not included in the code)

PLEASE DO NOT RUN THIS SCRIPT AT ONCE, BUT EACH STEP SEQUENTIALLY IN ORDER TO BE ABLE TO FOLLOW UP THE WHOLE PROCESS 
*/

/*********************************************************/
/**************** Create Table and Indexes ***************/
/*********************************************************/

-- We will use another table (test table) where we will copy the AIS data, sicne we do not want to do the cleanup in the AIS table directly
CREATE TABLE test_ais AS
	SELECT * FROM ais.ais;
CREATE INDEX IF NOT EXISTS idx_test_ais_vessel_id_ts ON test_ais(vessel_id, ts);
ALTER TABLE test_ais ADD PRIMARY KEY (id);


/*********************************************************/
/************** Duplicates Removal Procedure *************/
/*********************************************************/

-- Create a table where we will store all the duplicates records
CREATE TABLE test_ais_duplicates AS 
SELECT *
FROM ais.ais
WHERE false;


-- Cleanup procedure
-- The test table will be checked, the duplicates will be removed and moved to another table

DO $$
DECLARE del_count BIGINT := 0;
BEGIN
	TRUNCATE TABLE test_ais_duplicates;
	
	WITH duplicates AS (
	    -- Identify duplicates by vessel_id and ts
	    SELECT 
	        curr.id, 
	        curr.vessel_id, 
	        curr.ts, 
	        curr.geom, 
			prev.ts AS prev_ts,
	        nxt.ts AS next_ts,
	        prev.geom AS prev_geom,
	        nxt.geom AS next_geom
	    FROM test_ais curr
		INNER JOIN LATERAL (
			SELECT prev.id, prev.ts, prev.geom
			FROM test_ais prev
			WHERE prev.vessel_id = curr.vessel_id
				AND prev.ts < curr.ts
			ORDER BY prev.ts DESC
			LIMIT 1
			) prev ON true
		INNER JOIN LATERAL (
			SELECT nxt.id, nxt.ts, nxt.geom
			FROM test_ais nxt
			WHERE nxt.vessel_id = curr.vessel_id
				AND nxt.ts > curr.ts
			ORDER BY nxt.ts 
			LIMIT 1
			) nxt ON true
	    WHERE (curr.vessel_id, curr.ts) IN (
	        SELECT vessel_id, ts 
	        FROM test_ais
	        GROUP BY vessel_id, ts
	        HAVING COUNT(*) > 1
	    )
	),
	distance_calculation AS (
	    -- Calculate distances to previous and next points
	    SELECT 
	        id, 
	        vessel_id, 
	        ts, 
	        geom, 
	        ST_DistanceSphere(geom, prev_geom) AS prev_dist,
	        ST_DistanceSphere(geom, next_geom) AS next_dist,
	        COALESCE(ST_DistanceSphere(geom, prev_geom), 0) + 
	        COALESCE(ST_DistanceSphere(geom, next_geom), 0) AS total_distance,
			(ST_DistanceSphere(geom, prev_geom) / 1852) / (EXTRACT(EPOCH FROM (ts - prev_ts)) / 3600) AS speed_kn
	    FROM duplicates
	),
	ranked_duplicates AS (
	    -- Rank duplicates by the smallest total distance
	    SELECT 
	        id, 
	        ROW_NUMBER() OVER (PARTITION BY vessel_id, ts ORDER BY total_distance ASC) AS rank
	    FROM distance_calculation
	) 

	INSERT INTO test_ais_duplicates
	SELECT *
	FROM ais.ais 
	WHERE id IN (SELECT id FROM ranked_duplicates WHERE rank > 1);

	DELETE FROM test_ais
	WHERE id IN (SELECT id FROM test_ais_duplicates);

	SELECT COUNT(*) INTO del_count FROM test_ais_duplicates;
	RAISE NOTICE 'Total removed duplicates: %' ,del_count;
	
END $$;



/*********************************************************/
/**************** Outlier Cleanup process ****************/
/*********************************************************/

DO $$
DECLARE row_count BIGINT;
DECLARE del_count BIGINT;
DECLARE vessels_not_rechecked BIGINT := 0;
DECLARE cur_ts varchar;
BEGIN
	SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
	RAISE NOTICE '%: Script Begins', cur_ts;

	-- Step 0: Initialize temporary table to store previous timestamps per vessel
	IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'test_ais_deleted_records') THEN
		DROP TABLE test_ais_deleted_records;
		raise notice '%: Table test_ais_deleted_records dropped', cur_ts;
	END IF;

	CREATE TABLE test_ais_deleted_records (
	    id BIGINT PRIMARY KEY,
	    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	raise notice '%: Table test_ais_deleted_records created', cur_ts;

	IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_type = 'LOCAL TEMPORARY' AND table_name = 'temp_ais_to_be_deleted') THEN
		DROP TABLE temp_ais_to_be_deleted;
		raise notice '%: Temp Table temp_ais_to_be_deleted dropped', cur_ts;
	END IF;
	
	CREATE TEMP TABLE temp_ais_to_be_deleted (
    	id bigint,
	    vessel_id integer,
	    latitude numeric(8,6),
	    longitude numeric(9,6),
	    geom geometry(Point,4326),
	    speed_over_ground numeric(4,1),
	    course_over_ground numeric(4,1),
	    heading smallint,
	    status smallint,
	    draft numeric(3,1),
	    ts timestamp without time zone
	);
	raise notice '%: Table temp_ais_to_be_deleted created', cur_ts;

	IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_type = 'LOCAL TEMPORARY' AND table_name = 'temp_vessel_last_ts') THEN
		DROP TABLE temp_vessel_last_ts;
		raise notice '%: Temp Table temp_vessel_last_ts dropped', cur_ts;
	END IF;
	
	CREATE TEMP TABLE temp_vessel_last_ts AS
    SELECT vessel_id, MIN(ts) AS last_ts
    FROM test_ais
    GROUP BY vessel_id;
	
    LOOP
		BEGIN
	        -- Step 1: Identify records to be deleted in one pass
	        WITH speed_check_lateral AS (
	            SELECT curr.id AS curr_id, curr.vessel_id, curr.ts as curr_ts,
	                (ST_DistanceSphere(curr.geom, prev.geom) / 1852) / 
	                	(EXTRACT(EPOCH FROM (curr.ts - prev.ts)) / 3600) AS speed_kn
	            FROM test_ais curr
				INNER JOIN temp_vessel_last_ts vlt
	                ON curr.vessel_id = vlt.vessel_id AND curr.ts >= vlt.last_ts
	            INNER JOIN LATERAL (
	                SELECT prev.id, prev.ts, prev.geom
	                FROM test_ais prev
	                WHERE prev.vessel_id = curr.vessel_id
	                    AND prev.ts < curr.ts
	                ORDER BY prev.ts DESC
	                LIMIT 1
	            ) prev ON true
	        ),
			speed_check_lag AS (
	            SELECT id as curr_id, vessel_id, curr_ts
	                ,CASE 
	                    WHEN curr_ts = pre_ts THEN cur_speed 
	                    ELSE (ST_DistanceSphere(pre_geom, cur_geom) / 1852) / (extract(epoch FROM (curr_ts - pre_ts)) / 3600) 
	                END AS speed_kn
	            FROM (
	                SELECT id
	                    ,t.vessel_id
	                    ,speed_over_ground AS cur_speed
	                    ,ts AS curr_ts
	                    ,LAG(ts) OVER (PARTITION BY t.vessel_id ORDER BY ts) AS pre_ts
	                    ,geom AS cur_geom
	                    ,LAG(geom) OVER (PARTITION BY t.vessel_id ORDER BY ts) AS pre_geom
	                FROM test_ais t
	                INNER JOIN temp_vessel_last_ts vlt ON t.vessel_id = vlt.vessel_id AND t.ts >= vlt.last_ts
	            ) subquery
	        ),
	        ranked_deletions AS (
	            SELECT curr_id AS id,
	                   ROW_NUMBER() OVER (PARTITION BY vessel_id ORDER BY curr_ts ASC) AS rank
	            FROM speed_check_lag   -- We will use the lag check since its a lot faster than join lateral. The lag would not work if we had no removed the duplicates earlier
	            WHERE speed_kn > 20
	        ),
	        to_delete AS (
	            SELECT id
	            FROM ranked_deletions
	            WHERE rank = 1 -- Only take the first record per vessel
	        )
			
	        INSERT INTO temp_ais_to_be_deleted
	        SELECT id
	        FROM to_delete;
		
	        -- Step 2: Count the records flagged for deletion
	        SELECT COUNT(*) INTO del_count FROM temp_ais_to_be_deleted;
			SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
	        RAISE NOTICE '%: Records for deletion: %', cur_ts, del_count;
	
	        -- Step 3: Exit if no records are marked for deletion
	        IF del_count = 0 THEN
				SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
	            RAISE NOTICE '%: No records to delete. Exit...', cur_ts;
	            EXIT;
	        END IF;
	
	        -- Step 4: Log the deleted IDs
	        INSERT INTO test_ais_deleted_records (id, vessel_id, latitude, longitude, geom, speed_over_ground, course_over_ground, heading, status, draft, ts)
	        SELECT a.id, vessel_id, latitude, longitude, geom, speed_over_ground, course_over_ground, heading, status, draft, ts
	        FROM temp_ais_to_be_deleted tmp
			INNER JOIN temp_ais a on a.id = tmp.id;
			
			SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
			RAISE NOTICE '%: Log the deleted IDs', cur_ts;

			-- Step 5: Update the 'temp_vessel_last_ts' to reflect the new starting point (previous timestamp)
	        WITH deleted_records AS (
	            SELECT id, vessel_id, ts
	            FROM test_ais
	            WHERE id IN (SELECT id FROM temp_ais_to_be_deleted)
	        ),
			updated_last_ts AS (
			    SELECT v.vessel_id, MAX(t.ts) AS new_last_ts
			    FROM temp_vessel_last_ts v
			    LEFT JOIN test_ais t
			        ON v.vessel_id = t.vessel_id
			        AND t.ts < (SELECT MIN(ts) FROM deleted_records WHERE deleted_records.vessel_id = v.vessel_id)
			    GROUP BY v.vessel_id
			)
			
			UPDATE temp_vessel_last_ts
			SET last_ts = updated_last_ts.new_last_ts
			FROM updated_last_ts
			WHERE temp_vessel_last_ts.vessel_id = updated_last_ts.vessel_id;
	     
			vessels_not_rechecked := vessels_not_rechecked + 
				(SELECT COALESCE(COUNT(*), 0) FROM temp_vessel_last_ts WHERE last_ts IS NULL);
			SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
			RAISE NOTICE '%: Vessels not re-checked = %', cur_ts, vessels_not_rechecked;
			
			DELETE
			FROM temp_vessel_last_ts
			WHERE last_ts IS NULL;
			
			SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
			RAISE NOTICE '%: Update the temp_vessel_last_ts', cur_ts;
			
	        -- Step 6: Delete flagged records from `test_ais`
	        DELETE FROM test_ais
	        WHERE id IN (SELECT id FROM temp_ais_to_be_deleted);
	
	        SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
			RAISE NOTICE '%: Records deleted from test_ais.', cur_ts;
			
			RAISE NOTICE '-----------------------------------------------------------------';
			
	        -- Step 7: Clear the temporary deletion table
	        TRUNCATE TABLE temp_ais_to_be_deleted;
		
		EXCEPTION
            WHEN OTHERS THEN
                SELECT TO_CHAR(clock_timestamp(), 'YYYY/MM/DD HH24:MI:SS') INTO cur_ts;
                RAISE NOTICE '%: ERROR: %', cur_ts, SQLERRM;
                EXIT;
        END;
    END LOOP;


	SELECT COUNT(*) INTO row_count FROM test_ais_deleted_records;
	RAISE NOTICE 'Total deleted records: %' ,row_count;

END $$;



/*********************************************************/
/************** Check if outliers removed ****************/
/*********************************************************/

-- Below there are the 2 methods for checking. One is the join lateral and the seconf the lag (faster)
WITH speed_check AS (
	SELECT curr.id AS curr_id, prev.id as prev_id, curr.ts as curr_ts, prev.ts as prev_ts, curr.geom as curr_geom, prev.geom as prev_geom,
		(ST_DistanceSphere(curr.geom, prev.geom) / 1852) / 
		(EXTRACT(EPOCH FROM (curr.ts - prev.ts)) / 3600) AS speed_kn
	FROM test_ais curr
	INNER JOIN LATERAL (
		SELECT prev.id, prev.ts, prev.geom
	    FROM test_ais prev
	    WHERE prev.vessel_id = curr.vessel_id
			AND prev.ts < curr.ts
	    ORDER BY prev.ts DESC
		LIMIT 1
	) prev ON true
)
SELECT *
FROM speed_check
WHERE speed_kn> 20;


SELECT *
FROM (
	SELECT id, vessel_id, ts, geom, ts_pre, geom_pre,id_pre
		,CASE WHEN ts != ts_pre THEN
			(ST_DistanceSphere(geom, geom_pre) / 1852) / (extract(epoch FROM (ts - ts_pre)) / 3600) 
		END AS speed_kn
	FROM (
		select *
			,LAG(ts) OVER (PARTITION BY vessel_id ORDER BY ts) as ts_pre
			,LAG(geom) OVER (PARTITION BY vessel_id ORDER BY ts) as geom_pre
			,LAG(id) OVER (PARTITION BY vessel_id ORDER BY ts) as id_pre
		from test_ais
	)
) x
WHERE speed_kn > 20



/*********************************************************/
/************ Remove Outliers from AIS table *************/
/*********************************************************/

--DELETE FROM ais.ais 
--WHERE id in (
--	SELECT id FROM test_ais_deleted_records
--	)


