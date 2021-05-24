WITH advertiser_count AS (
  SELECT advertiser_name, count(*) as n_products
  FROM {{ params.active_products_table }} 
  GROUP BY advertiser_name
), product_color_options AS (
  SELECT 
    co.product_id,
    collect_set(
      struct(
        color as color, 
        alternate_color_product_id as product_id,
        ap.product_image_url as product_image_url
      )
    ) as product_color_options
  FROM {{ params.color_options_table}} co
  INNER JOIN {{ params.active_products_table }} ap
  ON co.alternate_color_product_id = ap.product_id
  GROUP BY co.product_id
)

SELECT 
  ap.*,
  random()*sqrt(ac.n_products)*cbrt(ac.n_products)*cbrt(sqrt(sqrt(ac.n_products))) as default_search_order,
  TRANSFORM(
    product_details,
    x -> struct( 
      x['size'] as size, 
      x['product_purchase_url'] as product_purchase_url, 
      true as in_stock
    )
  ) as sizes,
  pco.product_color_options,
  (n_likes + n_add_to_cart + 1) / n_views as swipe_rate
FROM {{ params.active_products_table }} ap 
INNER JOIN advertiser_count ac
ON ap.advertiser_name=ac.advertiser_name
LEFT JOIN product_color_options pco
ON ap.product_id = pco.product_id
ORDER BY default_search_order ASC
