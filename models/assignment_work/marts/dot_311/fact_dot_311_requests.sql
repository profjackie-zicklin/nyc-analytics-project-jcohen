-- Fact table: DOT 311 service requests
-- Grain: one row per service request (request_id, controlled in staging model)

-- Make sure you have access to staging and its grain
WITH stg_311 AS (
    SELECT * FROM {{ ref('stg_nyc_311_dot') }}
),

-- All the dimensions in their own CTEs for easy reference
dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_location AS (
    SELECT * FROM {{ ref('dim_location') }}
),

dim_complaint_type AS (
    SELECT * FROM {{ ref('dim_complaint_type') }}
),

-- Build intiial CTE for fact table
fact AS (

    SELECT
        -- Fact's own surrogate key, from the grain (this is deduped in staging)
        {{ dbt_utils.generate_surrogate_key(['s.request_id']) }} AS service_request_key,

        -- Degenerate dimension (business identifier)
        s.request_id as unique_key,

        -- Staging guarantees created_date: a NULL key here is a bug
        created_d.date_key AS created_date_key,

        -- Open requests legitimately have no closed date -> unknown member
        CASE WHEN s.closed_date IS NULL THEN '-1' ELSE closed_d.date_key END AS closed_date_key,

        -- Staging guarantees borough; zip may be NULL and still matches its dim row
        l.location_key,

        -- Staging guarantees complaint_type
        ct.complaint_type_key,
        rc.request_channel_key,

        s.status,
        s.method_of_submission,
        TIMESTAMP_DIFF(s.closed_date, s.created_date, HOUR) AS hours_to_close,
        TIMESTAMP_DIFF(s.closed_date, s.created_date, DAY)  AS days_to_close,
        s.latitude,
        s.longitude,
        s.street_name,
        s.incident_address,
        s.bbl,
        s.agency

    FROM stg_311 s

    -- Left joins because you want all the rows from staging in the fact rep'd, with key information from dimensions
    -- JOIN ON the items that make up the surrogate key in the dimension
    LEFT JOIN dim_date created_d
        ON CAST(s.created_date AS DATE) = created_d.full_date
    LEFT JOIN dim_date closed_d
        ON CAST(s.closed_date AS DATE) = closed_d.full_date

    -- IS NOT DISTINCT FROM so a NULL zip / council_district matches the dim row
    -- Could leave out the NULL exception and make these =s
    LEFT JOIN dim_location l
        ON  s.borough          IS NOT DISTINCT FROM l.borough
        AND s.council_district IS NOT DISTINCT FROM l.council_district
        AND s.zip              IS NOT DISTINCT FROM l.zip
    
    LEFT JOIN dim_request_channel rc 
        ON s.open_data_channel_type IS NOT DISTINCT FROM rc.open_data_channel_type
        AND s.agency IS NOT DISTINCT FROM rc.agency

    LEFT JOIN dim_complaint_type ct
        ON  s.complaint_type IS NOT DISTINCT FROM ct.complaint_type
        AND s.descriptor     IS NOT DISTINCT FROM ct.descriptor
)

SELECT * FROM fact