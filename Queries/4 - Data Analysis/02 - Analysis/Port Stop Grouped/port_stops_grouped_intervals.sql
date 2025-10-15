/* Calculate how many are the days between consecutive identified port calls. */

WITH time_diff AS (
	SELECT *
		,CAST(COALESCE(EXTRACT(EPOCH FROM (ts_begin - LAG(ts_begin) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin)))/86400, 0) as int) as time_diff_days
		,COALESCE(EXTRACT(EPOCH FROM (ts_begin - LAG(ts_end) OVER (PARTITION BY vessel_id, port_id ORDER BY ts_begin)))/3600, 0) AS non_stop_hours
	   FROM data_analysis.port_stops
)

SELECT time_diff_days as "Time Difference (Days)", count, "pct over total"
FROM (
	select time_diff_days, count(*) as count, CAST(100 * (count(*)/ sum(count(*)) over ()) as decimal(5,2)) as "pct over total"
	FROM time_diff
	GROUP BY time_diff_days
) x
WHERE time_diff_days > 0
ORDER BY time_diff_days
