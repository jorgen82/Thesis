/* Remove vessels marked as dry that do not exist in fixtures  */
DELETE
FROM ais.vessel
WHERE (vessel_category = 'Dry' AND vessel_name not in (SELECT DISTINCT vessel_name FROM fixtures.fixtures_data))


/* Set calculated_design_draught to null where the thresholds are not met. The thresholds created after research. */
/* THIS IS NOT A COLUMN THAT WE USED ON OUR RESEARCH AFTER ALL, SO THIS STEP CAN BE SKIPPED                       */
	
UPDATE ais.vessel
SET calculated_design_draught = null
WHERE 
	(vessel_type = 'ULCC-VLCC Tankers' and length < 320)
	OR (vessel_type = 'ULCC-VLCC Tankers' and width < 58)
	OR (vessel_type = 'Handysize Tankers' and length < 150)
	OR (vessel_type = 'Handysize Tankers' and length > 200)
	OR (vessel_type = 'Handysize Tankers' and width < 20)
	OR (vessel_type = 'Handysize Tankers' and width > 35)
	OR (vessel_type = 'Asphalt & Bitumen Carrier')
	OR (vessel_type = 'Suezmax Tankers' and length < 260)
	OR (vessel_type = 'Suezmax Tankers' and width < 45)
	OR (vessel_type = 'Aframax Tankers' and length < 225)
	OR (vessel_type = 'Aframax Tankers' and length > 260)
	OR (vessel_type = 'Aframax Tankers' and width > 45)
	OR (vessel_type = 'Panamax Tankers' and dwt < 65000)
	OR (vessel_type = 'Panamax Tankers' and length < 215)
	OR (vessel_type = 'Panamax Tankers' and length > 254)
	OR (vessel_type = 'Panamax Tankers' and width < 32)
	OR (vessel_type = 'Panamax Tankers' and width > 38)
	OR (vessel_type = 'Small Tanker (5-10K dwt)' and length > 150)
	OR (vessel_type = 'Small Tanker (5-10K dwt)' and width > 25)
