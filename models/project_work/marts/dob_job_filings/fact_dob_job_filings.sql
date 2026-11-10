-- Fact table: DOB NOW job filing applications
-- Grain: one row per job filing (job_filing_number, controlled in staging model)
--
-- Dimension keys are looked up Kimball-style: hash the staging attributes the same way the
-- dim does, then join on hash = dim surrogate key. That's a plain equality join (fast),
-- and NULL handling is built into the hash, so no IS NOT DISTINCT FROM is needed.
-- No match (all-NULL attributes are excluded from the dim) -> '-1' unknown member.

-- Make sure you have access to staging and its grain
WITH stg_dob AS (
    SELECT * FROM {{ ref('stg_dob_jobs_data') }}
),

-- All the dimensions in their own CTEs for easy reference (only the columns we join on / return)
dim_date AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

dim_location AS (
    SELECT location_key FROM {{ ref('dim_location') }}
),

dim_filing_representative AS (
    SELECT filing_representative_key FROM {{ ref('dim_filing_representative') }}
),

dim_applicant AS (
    SELECT applicant_key FROM {{ ref('dim_applicant') }}
),

dim_owner AS (
    SELECT owner_key FROM {{ ref('dim_owner') }}
),

dim_property AS (
    SELECT property_key FROM {{ ref('dim_property') }}
),

dim_job_type AS (
    SELECT job_type_key FROM {{ ref('dim_job_type') }}
),

-- Compute each dimension's hash once per row, using the SAME column list and order as the dim
hashed AS (

    SELECT
        s.*,

        -- Property location uses postcode (renamed to zip in staging)
        {{ dbt_utils.generate_surrogate_key([
            'borough', 'zip'
        ]) }} AS location_hash,

        {{ dbt_utils.generate_surrogate_key([
            'job_type', 'filing_review_type'
        ]) }} AS job_type_hash,

        {{ dbt_utils.generate_surrogate_key([
            'filing_representative_first_name', 'filing_representative_middle_initial',
            'filing_representative_last_name', 'filing_representative_business_name',
            'filing_representative_street_name', 'filing_representative_city',
            'filing_representative_state', 'filing_representative_zip'
        ]) }} AS filing_representative_hash,

        {{ dbt_utils.generate_surrogate_key([
            'applicant_license', 'applicant_professional_title', 'applicant_first_name',
            'applicants_middle_initial', 'applicant_last_name', 'applicant_business_name'
        ]) }} AS applicant_hash,

        {{ dbt_utils.generate_surrogate_key([
            'owner_first_name', 'owner_last_name', 'owner_s_business_name', 'owner_type'
        ]) }} AS owner_hash,

        {{ dbt_utils.generate_surrogate_key([
            'bin', 'bbl', 'block', 'lot'
        ]) }} AS property_hash

    FROM stg_dob s

),

-- Build initial CTE for fact table
fact AS (

    SELECT
        -- Fact's own surrogate key, from the grain (this is deduped in staging)
        {{ dbt_utils.generate_surrogate_key(['s.job_filing_number']) }} AS job_filing_key,
        -- Degenerate dimension (business identifier)
        s.job_filing_number,

        -- Dates: staging guarantees filing_date, so a NULL key here is a bug
        filing_d.date_key AS filing_date_key,
        -- The rest legitimately may not exist yet -> unknown member
        CASE WHEN s.current_status_date IS NULL THEN '-1' ELSE status_d.date_key   END AS current_status_date_key,
        CASE WHEN s.approved_date       IS NULL THEN '-1' ELSE approved_d.date_key END AS approved_date_key,
        CASE WHEN s.first_permit_date   IS NULL THEN '-1' ELSE permit_d.date_key   END AS first_permit_date_key,
        CASE WHEN s.signoff_date        IS NULL THEN '-1' ELSE signoff_d.date_key  END AS signoff_date_key,

        -- Location: always present for a filing
        l.location_key,
        -- Job type: always present for a filing (a '-1' here is worth investigating)
        COALESCE(jt.job_type_key, '-1')                       AS job_type_key,

        -- Optional dimensions: no match -> unknown member
        COALESCE(fr.filing_representative_key, '-1')          AS filing_representative_key,
        COALESCE(a.applicant_key, '-1')                       AS applicant_key,
        COALESCE(o.owner_key, '-1')                           AS owner_key,
        COALESCE(p.property_key, '-1')                        AS property_key,

        -- Plus the data we're passing on right from staging -> fact table
        s.filing_status,
        s.job_description,
        s.latitude,
        s.longitude,

        -- As-filed address and point-in-time geography (these vary between filings
        -- for the same building, so they live here rather than in dim_property)
        s.house_no,
        s.street_name,
        s.council_district,
        s.census_tract,
        s.nta,

        -- Measures (assumes staging casts these from text to numeric types)
        s.initial_cost,
        s.total_construction_floor_area,
        s.existing_stories,
        s.existing_height,
        s.existing_dwelling_units,
        s.proposed_no_of_stories,
        s.proposed_height,
        s.proposed_dwelling_units,

        -- Calculated fields
        TIMESTAMP_DIFF(s.approved_date,     s.filing_date, DAY) AS days_to_approval,
        TIMESTAMP_DIFF(s.first_permit_date, s.filing_date, DAY) AS days_to_first_permit,
        TIMESTAMP_DIFF(s.signoff_date,      s.filing_date, DAY) AS days_to_signoff

        -- TODO: work-type flags (sprinkler_work_type, plumbing_work_type, etc.) could be
        -- passed through here as-is, or moved to a junk dimension (dim_work_types) later

    FROM hashed s

    -- Left joins because you want all the rows from staging in the fact rep'd, with key information from dimensions
    LEFT JOIN dim_date filing_d
        ON CAST(s.filing_date AS DATE) = filing_d.full_date
    LEFT JOIN dim_date status_d
        ON CAST(s.current_status_date AS DATE) = status_d.full_date
    LEFT JOIN dim_date approved_d
        ON CAST(s.approved_date AS DATE) = approved_d.full_date
    LEFT JOIN dim_date permit_d
        ON CAST(s.first_permit_date AS DATE) = permit_d.full_date
    LEFT JOIN dim_date signoff_d
        ON CAST(s.signoff_date AS DATE) = signoff_d.full_date

    -- Hash lookups: plain equality on the dimension's surrogate key
    LEFT JOIN dim_location l
        ON s.location_hash = l.location_key
    LEFT JOIN dim_job_type jt
        ON s.job_type_hash = jt.job_type_key
    LEFT JOIN dim_filing_representative fr
        ON s.filing_representative_hash = fr.filing_representative_key
    LEFT JOIN dim_applicant a
        ON s.applicant_hash = a.applicant_key
    LEFT JOIN dim_owner o
        ON s.owner_hash = o.owner_key
    LEFT JOIN dim_property p
        ON s.property_hash = p.property_key
)

SELECT * FROM fact