-- Fact table: DOB NOW job filing applications
-- Grain: one row per job filing (job_filing_number, controlled in staging model)

-- Make sure you have access to staging and its grain
WITH stg_dob AS (
    SELECT * FROM {{ ref('stg_dob_jobs_data') }}
),

-- All the dimensions in their own CTEs for easy reference
dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_location AS (
    SELECT * FROM {{ ref('dim_location') }}
),

dim_filing_representative AS (
    SELECT * FROM {{ ref('dim_filing_representative') }}
),

-- Proposed new dimensions
dim_applicant AS (
    SELECT * FROM {{ ref('dim_applicant') }}
),

dim_owner AS (
    SELECT * FROM {{ ref('dim_owner') }}
),

dim_property AS (
    SELECT * FROM {{ ref('dim_property') }}
),

dim_job_type AS (
    SELECT * FROM {{ ref('dim_job_type') }}
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
        -- Job type: always present for a filing
        jt.job_type_key,

        -- Optional dimensions: no match (all-NULL attributes are excluded from the dim) -> unknown member
        COALESCE(fr.filing_representative_key, '-1') AS filing_representative_key,
        COALESCE(a.applicant_key, '-1')              AS applicant_key,
        COALESCE(o.owner_key, '-1')                  AS owner_key,
        COALESCE(p.property_key, '-1')               AS property_key,

        -- Plus the data we're passing on right from staging -> fact table
        s.filing_status,
        s.job_description,
        s.latitude,
        s.longitude,

        -- As-filed address and point-in-time geography (these vary between filings
        -- for the same building, so they live here rather than in dim_property)
        s.house_no,
        s.street_name,
        s.community_board,
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

    FROM stg_dob s

    -- Left joins because you want all the rows from staging in the fact rep'd, with key information from dimensions
    -- JOIN ON the items that make up the surrogate key in the dimension
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

    -- IS NOT DISTINCT FROM so a NULL attribute matches the dim row
    -- Property location uses postcode (not the applicant's zip)
    LEFT JOIN dim_location l
        ON  s.borough   IS NOT DISTINCT FROM l.borough
        AND s.postcode  IS NOT DISTINCT FROM l.zip

    LEFT JOIN dim_job_type jt
        ON  s.job_type          IS NOT DISTINCT FROM jt.job_type
        AND s.filing_review_type IS NOT DISTINCT FROM jt.filing_review_type

    LEFT JOIN dim_filing_representative fr
        ON  s.filing_representative_first_name     IS NOT DISTINCT FROM fr.first_name
        AND s.filing_representative_middle_initial IS NOT DISTINCT FROM fr.middle_initial
        AND s.filing_representative_last_name      IS NOT DISTINCT FROM fr.last_name
        AND s.filing_representative_business_name  IS NOT DISTINCT FROM fr.business_name
        AND s.filing_representative_street_name    IS NOT DISTINCT FROM fr.street_name
        AND s.filing_representative_city           IS NOT DISTINCT FROM fr.city
        AND s.filing_representative_state          IS NOT DISTINCT FROM fr.state
        AND s.filing_representative_zip            IS NOT DISTINCT FROM fr.zip

    LEFT JOIN dim_applicant a
        ON  s.applicant_license           IS NOT DISTINCT FROM a.license_number
        AND s.applicant_professional_title IS NOT DISTINCT FROM a.professional_title
        AND s.applicant_first_name        IS NOT DISTINCT FROM a.first_name
        AND s.applicant_middle_initial    IS NOT DISTINCT FROM a.middle_initial
        AND s.applicant_last_name         IS NOT DISTINCT FROM a.last_name
        AND s.applicant_business_name     IS NOT DISTINCT FROM a.business_name

    LEFT JOIN dim_owner o
        ON  s.owner_first_name        IS NOT DISTINCT FROM o.first_name
        AND s.owner_last_name         IS NOT DISTINCT FROM o.last_name
        AND s.owner_business_name     IS NOT DISTINCT FROM o.business_name
        AND s.owner_type              IS NOT DISTINCT FROM o.owner_type

    LEFT JOIN dim_property p
        ON  s.bin   IS NOT DISTINCT FROM p.bin
        AND s.bbl   IS NOT DISTINCT FROM p.bbl
        AND s.block IS NOT DISTINCT FROM p.block
        AND s.lot   IS NOT DISTINCT FROM p.lot
)

SELECT * FROM fact