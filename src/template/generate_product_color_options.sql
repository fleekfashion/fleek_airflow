CREATE OR REPLACE TEMP VIEW parsed_active AS (
  SELECT 
    *,
    split(product_name, ' ') as split_product_name
  FROM {{ params.active_table }} 
);

CREATE OR REPLACE TEMPORARY VIEW similar_product_info AS (
  
  WITH t AS (
    SELECT 
      si.product_id,
      si.similar_product_id,
      a1.split_product_name as root_product_name,
      a2.split_product_name as similar_product_name,
      a1.color as root_color,
      a2.color as similar_color,
      a1.product_image_url as root_url,
      a2.product_image_url as similar_url
    FROM prod_product_catalog.similar_products_v2 si
    INNER JOIN parsed_active a1
      ON si.product_id = a1.product_id
    INNER JOIN parsed_active a2
      ON si.similar_product_id = a2.product_id
    WHERE si.product_id IN (
      SELECT product_id 
      FROM prod_product_catalog.active_products
    ) AND a1.advertiser_name = a2.advertiser_name
    AND size(a1.product_labels) + size(a2.product_labels) = 2*size(array_intersect(a1.product_labels, a2.product_labels))
    AND size(a1.product_secondary_labels) + size(a2.product_secondary_labels) = 2*size(array_intersect(a1.product_secondary_labels, a2.product_secondary_labels))
    AND a1.product_id != a2.product_id
    AND size(a1.split_product_name) = size(a2.split_product_name)
    AND a1.color != a2.color
    AND a1.product_brand = a2.product_brand
  )

  SELECT 
    *,
    AGGREGATE(
      TRANSFORM(
        arrays_zip(root_product_name, similar_product_name), 
        x -> (x['root_product_name'] = x['similar_product_name'])::int
      ),
      0,
      (x, y) -> x + y
    ) as n_matching_words
  FROM t
);

CREATE OR REPLACE TEMPORARY VIEW final_colors AS (
  WITH t1 AS (
    SELECT 
      *
    FROM similar_product_info
    WHERE size(root_product_name) - n_matching_words <= 1
    AND n_matching_words::float/size(root_product_name) >= .8
  ), t2 AS (
    SELECT
      product_id,
      max(n_matching_words) as max_matching_words
    FROM t1
    GROUP BY product_id
  ), t3 AS (
    SELECT
      t1.*
    FROM t1
    INNER JOIN t2
    ON t1.product_id = t2.product_Id
    WHERE n_matching_words = max_matching_words
  ), t4 AS (
    SELECT 
      product_id,
      similar_product_id
    FROM t3
      UNION ALL
    SELECT 
      t3.product_id as product_id,
      tsim.similar_product_id as similar_product_id
    FROM t3
    INNER JOIN t3 tmid
    ON t3.product_id = tmid.similar_product_id
    INNER JOIN t3 tsim
    ON tmid.product_id = tsim.product_id
    WHERE t3.root_color != tsim.similar_color
  )

  SELECT 
    product_id,
    collect_set(similar_product_id) as alternate_color_product_ids
  FROM t4
  WHERE product_id != similar_product_id
  GROUP BY product_id
);

SELECT 
  product_id,
  explode(alternate_color_product_ids) as alternate_color_product_id
FROM final_colors
WHERE size(alternate_color_product_ids) > 0
