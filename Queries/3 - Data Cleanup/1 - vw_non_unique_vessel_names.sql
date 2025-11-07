/*  This view is to be used to find vessels that have same vessel name and mssi
    We might want to filter out those vessels on our analysis, since out goal is to match the fixtures with the vessels / trajectories etc
    and the match can only be done using the vessel names
*/

CREATE OR REPLACE VIEW ais.vw_non_unique_vessel_names AS
SELECT vessel_name 
FROM (
    SELECT vessel_name, imo
    FROM ais.vessel
    GROUP BY vessel_name, imo
    ) 
GROUP BY vessel_name 
HAVING count(*)>1;
