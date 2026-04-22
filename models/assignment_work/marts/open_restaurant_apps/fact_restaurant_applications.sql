-- Fact table for NYC Open Restaurant applications                                                                                                                                 
  -- One row per application submission                                                                                                                                              
                                                                                                                                                                                     
  WITH restaurant_apps AS (                                                                                                                                                          
      SELECT * FROM {{ ref('stg_nyc_open_restaurant_apps') }}                                                                                                                        
  ),                                                                                                                                                                                 
                                                                                                                                                                                     
  dim_date AS (
      SELECT * FROM {{ ref('dim_date') }}
  ),

  dim_location AS (
      SELECT * FROM {{ ref('dim_location') }}
  ),

  dim_restaurant AS (
      SELECT * FROM {{ ref('dim_restaurant') }}
  ),

  dim_seating_type AS (
      SELECT * FROM {{ ref('dim_seating_type') }}
  ),

  fact_restaurant_apps AS (
      SELECT
          -- Surrogate key
          {{ dbt_utils.generate_surrogate_key(['r.objectid']) }} AS application_key,

          -- Natural key
          CAST(r.objectid AS STRING) AS objectid,

          -- Event timestamp
          CAST(r.time_of_submission AS TIMESTAMP) AS application_submitted,

          -- Dimension keys
          d.date_key AS submission_date_key,
          l.location_key AS location_key,
          rest.restaurant_key AS restaurant_key,
          st.seating_type_key AS seating_type_key,

          -- Location details
          CAST(r.business_address AS STRING) AS business_address,
          CAST(r.street AS STRING) AS street,
          CAST(r.bulding_number AS STRING) AS building_number,
          CAST(r.latitude AS FLOAT64) AS latitude,
          CAST(r.longitude AS FLOAT64) AS longitude,

          -- Sidewalk measurements
          CAST(r.sidewalk_dimensions_length AS INT) AS sidewalk_length_ft,
          CAST(r.sidewalk_dimensions_width AS INT) AS sidewalk_width_ft,
          CAST(r.sidewalk_dimensions_area AS INT) AS sidewalk_area_sqft,

          -- Compliance flags
          CASE WHEN r.qualify_alcohol  = "yes" THEN True ELSE False END AS qualify_alcohol,
          CASE WHEN r.landmark_district_or_building  = "yes" THEN True ELSE False END AS is_landmark_location,
          CASE WHEN r.healthcompliance_terms = "yes" THEN True ELSE False END AS health_compliance_terms_accepted,

          -- Liquor license
          CAST(r.sla_serial_number AS STRING) AS sla_serial_number,
          CAST(r.sla_license_type AS STRING) AS sla_license_type

      FROM restaurant_apps r

      LEFT JOIN dim_date d
          ON CAST(r.time_of_submission AS DATE) = d.full_date

      LEFT JOIN dim_location l
          ON r.borough = l.borough
          AND r.zip_code = l.zip_code

      LEFT JOIN dim_restaurant rest
          ON r.food_service_establishment = rest.food_service_establishment
          AND r.business_address = rest.business_address

      LEFT JOIN dim_seating_type st
          ON r.seating_interest_sidewalk = st.seating_interest
          AND (CASE WHEN r.approved_for_sidewalk_seating = 'yes' THEN True ELSE False END) = st.approved_for_sidewalk
          AND (CASE WHEN r.approved_for_roadway_seating = 'yes' THEN True ELSE False END) = st.approved_for_roadway
  )

  SELECT * FROM fact_restaurant_apps

