/* 
    This query calculates the number of days between consecutive identified port calls for each vessel. 
    It helps measure how frequently vessels return to the same port and provides insight into operational patterns such as shuttle trades, recurring port rotations, or long-haul voyages.
*/

WITH time_diff AS (
	SELECT *
		,CAST(COALESCE(EXTRACT(EPOCH FROM (ts_begin - LAG(ts_begin) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin)))/86400, 0) as int) as time_diff_days  --Calculate the days between the start of the current port stop and the start of the previous port stop for the SAME vessel AND port.
		,COALESCE(EXTRACT(EPOCH FROM (ts_begin - LAG(ts_end) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin)))/3600, 0) AS non_stop_hours  -- Calculate non-stop hours between consecutive calls: time between the start of this port call and the END of the previous one.
		,LAG(port_id) OVER (PARTITION BY vessel_id ORDER BY ts_begin) AS pre_port
	   FROM data_analysis.port_stops
)

SELECT time_diff_days as "Time Difference (Days)", count, "pct over total"
FROM (
	select time_diff_days, count(*) as count
		,CAST(100 * (count(*)/ sum(count(*)) over ()) as decimal(5,2)) as "pct over total"  --Percentage contribution to total records. Shows how common each time-gap category is.
	FROM time_diff
	WHERE port_id != pre_port  -- Exclude consecutive records belonging to the SAME port, because these often represent split port calls rather than real multiple visits
	GROUP BY time_diff_days
) x
WHERE time_diff_days > 0  --Exclude zero-day differences (same-day split events)
ORDER BY time_diff_days
