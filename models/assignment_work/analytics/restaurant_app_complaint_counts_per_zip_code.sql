WITH restaurant_counts AS (                                                                                                                                                        
      SELECT                             
          l.zip_code,                                                                                                                                                                
          COUNT(*) AS total_restaurants                                                                                                                                              
      FROM {{ ref('fact_restaurant_applications') }} f                                                                                                                               
      JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
      GROUP BY l.zip_code
  ),

  complaint_counts AS (
      SELECT
          l.zip_code,
          COUNT(*) AS total_complaints
      FROM {{ ref('fact_dot_311_requests') }} f
      JOIN {{ ref('dim_location') }} l ON f.location_key = l.location_key
      JOIN {{ ref('dim_date') }} d ON f.created_date_key = d.date_key
      JOIN {{ ref('dim_complaint_type') }} ct ON f.complaint_type_key = ct.complaint_type_key
      WHERE ct.complaint_category IN ('Sidewalk Issues', 'Street Issues', 'Bike Issues')
        AND d.year BETWEEN 2020 AND 2024
      GROUP BY l.zip_code
  )

  SELECT
      r.zip_code,
      r.total_restaurants,
      c.total_complaints AS total_sidewalk_street_bike_complaints,
      ROUND(c.total_complaints * 1.0 / r.total_restaurants, 2) AS complaints_per_restaurant
  FROM restaurant_counts r
  JOIN complaint_counts c
      ON r.zip_code = c.zip_code
  ORDER BY r.zip_code