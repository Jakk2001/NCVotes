INSERT INTO registration.voter_registration (county, party, race, ethnicity, age, gender, registration_date, total)
VALUES 
('Wake', 'DEM', 'White', 'Not Hispanic', 25, 'M', '2024-10-10', 265000),
('Wake', 'REP', 'White', 'Not Hispanic', 45, 'F', '2024-10-10', 225000),
('Durham', 'DEM', 'Black', 'Not Hispanic', 33, 'F', '2024-10-10', 180000),
('Durham', 'REP', 'White', 'Not Hispanic', 60, 'M', '2024-10-10', 35000);


SELECT * FROM registration.voter_registration LIMIT 5;