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

CREATE TABLE registration.raw_voters (
    county_id INT,
    county_desc VARCHAR(15),
    voter_reg_num CHAR(12),
    ncid CHAR(12),
    last_name VARCHAR(25),
    first_name VARCHAR(20),
    middle_name VARCHAR(20),
    name_suffix_lbl CHAR(3),
    status_cd CHAR(2),
    voter_status_desc VARCHAR(25),
    reason_cd CHAR(2),
    voter_status_reason_desc VARCHAR(60),
    res_street_address VARCHAR(65),
    res_city_desc VARCHAR(60),
    state_cd CHAR(2),
    zip_code CHAR(9),
    mail_addr1 VARCHAR(40),
    mail_addr2 VARCHAR(40),
    mail_addr3 VARCHAR(40),
    mail_addr4 VARCHAR(40),
    mail_city VARCHAR(30),
    mail_state CHAR(2),
    mail_zipcode CHAR(9),
    full_phone_number VARCHAR(12),
    confidential_ind CHAR(1),
    registr_dt DATE,
    race_code CHAR(3),
    ethnic_code CHAR(3),
    party_cd CHAR(3),
    gender_code CHAR(1),
    birth_year CHAR(4),
    age_at_year_end CHAR(3),
    birth_state VARCHAR(2),
    drivers_lic CHAR(1),
    precinct_abbrv VARCHAR(6),
    precinct_desc VARCHAR(60),
    municipality_abbrv VARCHAR(6),
    municipality_desc VARCHAR(60),
    ward_abbrv VARCHAR(6),
    ward_desc VARCHAR(60),
    cong_dist_abbrv VARCHAR(6),
    super_court_abbrv VARCHAR(6),
    judic_dist_abbrv VARCHAR(6),
    nc_senate_abbrv VARCHAR(6),
    nc_house_abbrv VARCHAR(6),
    county_commiss_abbrv VARCHAR(6),
    county_commiss_desc VARCHAR(60),
    township_abbrv VARCHAR(6),
    township_desc VARCHAR(60),
    school_dist_abbrv VARCHAR(6),
    school_dist_desc VARCHAR(60),
    fire_dist_abbrv VARCHAR(6),
    fire_dist_desc VARCHAR(60),
    water_dist_abbrv VARCHAR(6),
    water_dist_desc VARCHAR(60),
    sewer_dist_abbrv VARCHAR(6),
    sewer_dist_desc VARCHAR(60),
    sanit_dist_abbrv VARCHAR(6),
    sanit_dist_desc VARCHAR(60),
    rescue_dist_abbrv VARCHAR(6),
    rescue_dist_desc VARCHAR(60),
    munic_dist_abbrv VARCHAR(6),
    munic_dist_desc VARCHAR(60),
    dist_1_abbrv VARCHAR(6),
    dist_1_desc VARCHAR(60),
    vtd_abbrv VARCHAR(6),
    vtd_desc VARCHAR(60)
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

CREATE SCHEMA IF NOT EXISTS registration;

CREATE TABLE IF NOT EXISTS registration.raw_voters (
    county_id INT,
    county_desc VARCHAR(15),
    voter_reg_num CHAR(12),
    ncid CHAR(12),
    last_name VARCHAR(25),
    first_name VARCHAR(20),
    middle_name VARCHAR(20),
    name_suffix_lbl CHAR(3),
    status_cd CHAR(2),
    voter_status_desc VARCHAR(25),
    reason_cd CHAR(2),
    voter_status_reason_desc VARCHAR(60),
    res_street_address VARCHAR(65),
    res_city_desc VARCHAR(60),
    state_cd CHAR(2),
    zip_code CHAR(9),
    mail_addr1 VARCHAR(40),
    mail_addr2 VARCHAR(40),
    mail_addr3 VARCHAR(40),
    mail_addr4 VARCHAR(40),
    mail_city VARCHAR(30),
    mail_state CHAR(2),
    mail_zipcode CHAR(9),
    full_phone_number VARCHAR(12),
    confidential_ind CHAR(1),
    registr_dt DATE,
    race_code CHAR(3),
    ethnic_code CHAR(3),
    party_cd CHAR(3),
    gender_code CHAR(1),
    birth_year CHAR(4),
    age_at_year_end CHAR(3),
    birth_state VARCHAR(2),
    drivers_lic CHAR(1),
    precinct_abbrv VARCHAR(6),
    precinct_desc VARCHAR(60),
    municipality_abbrv VARCHAR(6),
    municipality_desc VARCHAR(60),
    ward_abbrv VARCHAR(6),
    ward_desc VARCHAR(60),
    cong_dist_abbrv VARCHAR(6),
    super_court_abbrv VARCHAR(6),
    judic_dist_abbrv VARCHAR(6),
    nc_senate_abbrv VARCHAR(6),
    nc_house_abbrv VARCHAR(6),
    county_commiss_abbrv VARCHAR(6),
    county_commiss_desc VARCHAR(60),
    township_abbrv VARCHAR(6),
    township_desc VARCHAR(60),
    school_dist_abbrv VARCHAR(6),
    school_dist_desc VARCHAR(60),
    fire_dist_abbrv VARCHAR(6),
    fire_dist_desc VARCHAR(60),
    water_dist_abbrv VARCHAR(6),
    water_dist_desc VARCHAR(60),
    sewer_dist_abbrv VARCHAR(6),
    sewer_dist_desc VARCHAR(60),
    sanit_dist_abbrv VARCHAR(6),
    sanit_dist_desc VARCHAR(60),
    rescue_dist_abbrv VARCHAR(6),
    rescue_dist_desc VARCHAR(60),
    munic_dist_abbrv VARCHAR(6),
    munic_dist_desc VARCHAR(60),
    dist_1_abbrv VARCHAR(6),
    dist_1_desc VARCHAR(60),
    vtd_abbrv VARCHAR(6),
    vtd_desc VARCHAR(60)
);

CREATE INDEX idx_registration_date ON registration.voter_registration (registration_date);
CREATE INDEX idx_election_date ON elections.election_results (election_date);
CREATE INDEX idx_candidate_party ON elections.election_results (candidate, party);

CREATE INDEX idx_raw_voters_county ON registration.raw_voters (county_id);
CREATE INDEX idx_raw_voters_reg_date ON registration.raw_voters (registr_dt);
CREATE INDEX idx_raw_voters_party ON registration.raw_voters (party_cd);
CREATE INDEX idx_raw_voters_ncid ON registration.raw_voters (ncid);

CREATE INDEX idx_election_date ON elections.election_results (election_date);
CREATE INDEX idx_election_county ON elections.election_results (county);
CREATE INDEX idx_election_candidate ON elections.election_results (candidate);
CREATE INDEX idx_election_party ON elections.election_results (party);
CREATE INDEX idx_election_office_district ON elections.election_results (office, district);

CREATE UNIQUE INDEX idx_counties_fips ON public.counties (fips_code);
CREATE UNIQUE INDEX idx_counties_name ON public.counties (county_name);


CREATE SCHEMA IF NOT EXISTS raw;
-- then move raw_voters to raw schema
ALTER TABLE registration.raw_voters SET SCHEMA raw;