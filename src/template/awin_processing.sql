SELECT 
  aw_deep_link as product_purchase_url,
  product_name as product_name,
  rrp_price::float as product_price,
  COALESCE(search_price, rrp_price)::float as product_sale_price,
  merchant_image_url as product_image_url,
  FILTER(
    ARRAY(alternate_image, alternate_image_two, alternate_image_three, alternate_image_four),
    x -> length(x) > 0
  ) as product_additional_images,
  merchant_product_id as external_product_id,
  ma.advertiser_name as advertiser_name,
  description as product_description,
  currency as product_currency,
  brand_name as product_brand,
  colour as color,
  `Fashion:size` as size,
  ARRAY(
    category_name, 
    merchant_category, 
    if( in_stock = '1', 'in_stock', 'out of stock'),
    custom_1
  ) as product_external_labels
FROM {{ params.new_products_table }} np
INNER JOIN {{ params.advertiser_name_map_table }} ma
ON np.merchant_name=ma.merchant_name
