/* 
	This script will insert some extra vessel data we might need on the ais.vessel table.
*/

ALTER TABLE ais.vessel
ADD COLUMN vessel_type varchar(50), 
	ADD COLUMN vessel_type_detailed varchar(50), 
	ADD COLUMN dwt integer, 
	ADD COLUMN gt integer, 
	ADD COLUMN calculated_design_draught decimal(5,2), 
	ADD COLUMN flag varchar(50), 
	ADD COLUMN year_bult integer,
	ADD COLUMN builder varchar(50), 
	ADD COLUMN company varchar(50), 
	ADD COLUMN group_company varchar(50);


UPDATE ais.vessel AS V
SET vessel_type = REPLACE(IV.filename, '.xlsx', '') 
	,vessel_type_detailed = IV."Type"  
	,dwt = IV."Dwt"
	,gt = IV."GT"
	,calculated_design_draught = CAST(
		((0.2 * dwt) + dwt) / (
		CASE vessel_type
			WHEN 'ULCC-VLCC Tankers' THEN 0.84
			WHEN 'Suezmax Tankers' THEN 0.85
			WHEN'Shuttle Tankers' THEN 0.825
			WHEN 'Aframax Tankers' THEN 0.8
			WHEN 'Panamax Tankers' THEN 0.78
			WHEN'Asphalt & Bitumen Carrier' THEN 0.775
			WHEN 'Handysize Tankers' THEN 0.77
			WHEN 'Small Tanker (5-10K dwt)' THEN 0.725
			ELSE 0.85 END * length * width * 1.025) as decimal(5,2))
	,flag = IV."Flag" 
	,year_bult = IV."Built" 
	,builder = IV."Builder"
	,company = IV."Company"
	,group_company = IV."Group Company"
FROM import.imported_vessel_data IV
WHERE UPPER(IV."Name") = UPPER(V.vessel_name)
	AND V.vessel_name NOT IN (SELECT "Name" FROM import.imported_vessel_data GROUP BY "Name" HAVING COUNT(*) > 1);

