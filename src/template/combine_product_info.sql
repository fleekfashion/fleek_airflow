CREATE OR REPLACE TEMPORARY VIEW pi AS (
  WITH labels AS (
    SELECT 
      product_id,
      collect_set(product_label) as product_labels
    FROM {{ params.labels }}
    GROUP BY product_id
  ),
  secondary_labels AS (
    SELECT 
      product_id,
      collect_set(product_secondary_label) as product_secondary_labels
    FROM {{ params.secondary_labels }}
    GROUP BY product_id
  )
  SELECT 
    l.product_id,
    l.product_labels,
    COALESCE(
      sl.product_secondary_labels,
      ARRAY()
    ) as product_secondary_labels,
    COALESCE(urls.product_image_url, pi.product_image_url) as product_image_url,
    COALESCE(
      more_urls.product_additional_image_urls, 
      pi.product_additional_image_urls
    ) as product_additional_image_urls,
    {{ params.columns }}
  FROM {{params.src}} pi
  INNER JOIN labels l
    ON pi.product_id=l.product_id
  LEFT JOIN secondary_labels sl
    ON pi.product_id=sl.product_id
  LEFT JOIN {{ params.image_url_table }} urls
    ON pi.product_id = urls.product_id
  LEFT JOIN {{ params.additional_image_urls_table }} more_urls
    ON pi.product_id = more_urls.product_id
  WHERE size(l.product_labels) > 0
);

SELECT 
  * 
FROM pi 
WHERE NOT ({{params.drop_args_filter}})
