-- NCVotes Database Schema
-- Fixed version with no duplicates and consistent naming

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS registration;
CREATE SCHEMA IF NOT EXISTS elections;
CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================================================
-- REGISTRATION TABLES
-- ============================================================================

-- Aggregated voter registration data (main table for analysis)
CREATE TABLE IF NOT EXISTS registration.voter_registration (
    id SERIAL PRIMARY KEY,
    registration_date DATE NOT NULL,
    county_desc TEXT NOT NULL,
    party TEXT,
    race TEXT,
    ethnicity TEXT,
    gender TEXT,
    age INTEGER,
    total INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- RAW DATA TABLES
-- ============================================================================

-- Raw individual voter records (for detailed analysis)
CREATE TABLE IF NOT EXISTS raw.raw_voters (
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
    ssn CHAR(1),
    no_dl_ssn_chkbx CHAR(1),
    hava_id_req CHAR(1),
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
    vtd_desc VARCHAR(60),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ELECTIONS TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS elections.election_results (
    id SERIAL PRIMARY KEY,
    election_date DATE NOT NULL,
    county TEXT NOT NULL,
    precinct TEXT,
    contest TEXT,
    district TEXT,
    candidate TEXT NOT NULL,
    party TEXT,
    votes INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- REFERENCE TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.counties (
    id SERIAL PRIMARY KEY,
    fips_code TEXT UNIQUE NOT NULL,
    county_name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Registration indexes
CREATE INDEX IF NOT EXISTS idx_voter_reg_date 
    ON registration.voter_registration (registration_date);
CREATE INDEX IF NOT EXISTS idx_voter_reg_county 
    ON registration.voter_registration (county_desc);
CREATE INDEX IF NOT EXISTS idx_voter_reg_party 
    ON registration.voter_registration (party);
CREATE INDEX IF NOT EXISTS idx_voter_reg_date_party 
    ON registration.voter_registration (registration_date, party);

-- Raw voters indexes
CREATE INDEX IF NOT EXISTS idx_raw_voters_county 
    ON raw.raw_voters (county_id);
CREATE INDEX IF NOT EXISTS idx_raw_voters_reg_date 
    ON raw.raw_voters (registr_dt);
CREATE INDEX IF NOT EXISTS idx_raw_voters_party 
    ON raw.raw_voters (party_cd);
CREATE INDEX IF NOT EXISTS idx_raw_voters_ncid 
    ON raw.raw_voters (ncid);

-- Election results indexes
CREATE INDEX IF NOT EXISTS idx_election_date 
    ON elections.election_results (election_date);
CREATE INDEX IF NOT EXISTS idx_election_county 
    ON elections.election_results (county);
CREATE INDEX IF NOT EXISTS idx_election_candidate 
    ON elections.election_results (candidate);
CREATE INDEX IF NOT EXISTS idx_election_party 
    ON elections.election_results (party);
CREATE INDEX IF NOT EXISTS idx_election_contest 
    ON elections.election_results (contest, district);

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON SCHEMA registration IS 'Voter registration data and aggregations';
COMMENT ON SCHEMA elections IS 'Election results data';
COMMENT ON SCHEMA raw IS 'Raw data files loaded without transformation';

COMMENT ON TABLE registration.voter_registration IS 'Aggregated voter registration counts by demographics';
COMMENT ON TABLE raw.raw_voters IS 'Individual voter records from NC State Board of Elections';
COMMENT ON TABLE elections.election_results IS 'Election results by precinct and candidate';
COMMENT ON TABLE public.counties IS 'NC county reference data with FIPS codes';

-- Add column
ALTER TABLE raw.raw_voters ADD COLUMN age_group VARCHAR(10);

-- Populate with current ages
UPDATE raw.raw_voters
SET age_group = 
    CASE 
        WHEN 2026 - CAST(birth_year AS INTEGER) BETWEEN 18 AND 25 THEN '18-25'
        WHEN 2026 - CAST(birth_year AS INTEGER) BETWEEN 26 AND 35 THEN '26-35'
        WHEN 2026 - CAST(birth_year AS INTEGER) BETWEEN 36 AND 50 THEN '36-50'
        WHEN 2026 - CAST(birth_year AS INTEGER) BETWEEN 51 AND 65 THEN '51-65'
        WHEN 2026 - CAST(birth_year AS INTEGER) > 65 THEN '65+'
        ELSE 'Unknown'
    END
WHERE birth_year IS NOT NULL AND birth_year ~ '^[0-9]+$';

-- Add index for performance
CREATE INDEX idx_raw_voters_age_group ON raw.raw_voters (age_group);