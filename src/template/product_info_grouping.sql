SELECT 
  product_id,
  COALESCE(
    collect_set(
      struct(lower(color) as color, size, product_price, product_sale_price, product_purchase_url)
    ),
    ARRAY()
  ) as product_details,
  {{ params.ungrouped_columns}}
FROM {{ params.src }} 
GROUP BY product_id
