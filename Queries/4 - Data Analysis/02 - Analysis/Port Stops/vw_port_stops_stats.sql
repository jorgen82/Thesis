/* Create a view with basic statistics for each Port Stop */

CREATE VIEW data_analysis.vw_port_stops_stats
AS

WITH port_stop_stats AS (
	SELECT port_id,
		p.port_name,
		p.country,
		count(*) AS count_port_stops,
		min(min_port_dist) AS min_port_dist,
		max(min_port_dist) AS max_port_dist,
		avg(min_port_dist) AS avg_port_dist,
		percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY min_port_dist) AS q1_port_dist,
		percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY min_port_dist) AS q2_port_dist,
		percentile_cont(0.75::double precision) WITHIN GROUP (ORDER BY min_port_dist) AS q3_port_dist,
		min(total_duration_h) AS min_duration_h,
		max(total_duration_h) AS max_duration_h,
		avg(total_duration_h) AS avg_duration_h,
		percentile_cont(0.25::double precision) WITHIN GROUP (ORDER BY (total_duration_h)) AS q1_duration_h,
		percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (total_duration_h)) AS q2_duration_h,
		percentile_cont(0.75::double precision) WITHIN GROUP (ORDER BY (total_duration_h)) AS q3_duration_h
	FROM data_analysis.port_stops_grouped psg
	INNER JOIN context_data.ports p on p.id = psg.port_id
	GROUP BY port_id, p.port_name, p.country
)

SELECT port_id,
	port_name,
	country,
	count_port_stops,
	min_port_dist,
	max_port_dist,
	avg_port_dist,
	q1_port_dist,
	q2_port_dist,
	q3_port_dist,
	q3_port_dist - q1_port_dist AS port_dist_iqr,
	q1_port_dist - 1.5 * (q3_port_dist - q1_port_dist) AS port_dist_lower_bound,
	q1_port_dist + 1.5 * (q3_port_dist - q1_port_dist) AS port_dist_upper_bound,	
	min_duration_h,
	max_duration_h,
	avg_duration_h,
	q1_duration_h,
	q2_duration_h,
	q3_duration_h,
	q3_duration_h - q1_duration_h AS duration_h_iqr,
	q1_duration_h - 1.5 * (q3_duration_h - q1_duration_h) AS duration_h_lower_bound,
	q1_duration_h + 1.5 * (q3_duration_h - q1_duration_h) AS duration_h_upper_bound
FROM port_stop_stats;
