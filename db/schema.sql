-- registration schema
CREATE SCHEMA IF NOT EXISTS registration;

CREATE TABLE registration.voter_registration (
    id SERIAL PRIMARY KEY,
    county TEXT,
    party TEXT,
    race TEXT,
    ethnicity TEXT,
    age INTEGER,
    gender TEXT,
    registration_date DATE,
    total INTEGER
);

-- elections schema
CREATE SCHEMA IF NOT EXISTS elections;

CREATE TABLE elections.election_results (
    id SERIAL PRIMARY KEY,
    election_date DATE,
    county TEXT,
    precinct TEXT,
    office TEXT,
    district TEXT,
    candidate TEXT,
    party TEXT,
    votes INTEGER
);

-- shared reference
CREATE TABLE public.counties (
    id SERIAL PRIMARY KEY,
    fips_code TEXT,
    county_name TEXT
);