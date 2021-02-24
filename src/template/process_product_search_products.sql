WITH advertiser_count AS (
  SELECT advertiser_name, count(*) as n_products
  FROM {{ params.active_products_table }} 
  GROUP BY advertiser_name
)
SELECT 
  ap.*,
  random()*sqrt(ac.n_products)*cbrt(ac.n_products) as default_search_order,
  (n_likes + n_add_to_cart + 1) / n_views as swipe_rate
FROM {{ params.active_products_table }} ap 
INNER JOIN advertiser_count ac
ON ap.advertiser_name=ac.advertiser_name
ORDER BY default_search_order ASC
