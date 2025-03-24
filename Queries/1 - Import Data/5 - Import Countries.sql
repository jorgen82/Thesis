CREATE TABLE import.imported_countries_data (
    id bigserial,
    country varchar(100),
    alpha2_code char(2),
    alpha3_code char(3),
    numeric_code integer,
    latitude decimal(8,6),
    longitude decimal(9,6),
    CONSTRAINT imported_countries_data_PKEY PRIMARY KEY (id)
);


COPY import.imported_countries_data (country, alpha2_code, alpha3_code, numeric_code, latitude, longitude)
FROM 'J:/OneDrive/Documents/Personal Files/Academic/Demokritos/Thesis/Complex Event Recognition/Data/countries/countries_codes_and_coordinates.csv'
DELIMITER ',' CSV HEADER QUOTE '"';


UPDATE context_data.countries SET country = 'The Bahamas' where country = 'Bahamas';
