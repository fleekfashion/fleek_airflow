SELECT
  DISTINCT(product_id),
  product_image_url as image_url
  FROM `{{params.product_info_table}}`
