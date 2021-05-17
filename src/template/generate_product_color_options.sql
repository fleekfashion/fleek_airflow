CREATE OR REPLACE TEMP VIEW parsed_active AS (
  WITH t AS (
    SELECT
      array_remove(
        split(
          regexp_replace(
            lower(color),
            '[^\\w\\s]',
            ' '
          ),
          ' '
        ),
        ''
      ) as parsed_color,
      array_remove(
        split(
          regexp_replace(
            lower(product_name), 
            '[^\\w\\s]', 
            ' '
          ), 
          ' '
        ),
        ''
      ) as parsed_product_name,
      *
    FROM {{ params.active_table }}
  )
  
  SELECT
    AGGREGATE(
      parsed_color,
      parsed_product_name,
      (acc, x) -> array_remove(acc, x)
    ) as split_product_name,
    *
  FROM t
);

CREATE OR REPLACE TEMPORARY VIEW similar_product_info AS (
  WITH t AS (
    SELECT 
      a1.product_id,
      a2.product_id as similar_product_id,
      a1.split_product_name as root_product_name,
      a2.split_product_name as similar_product_name,
      a1.color as root_color,
      a2.color as similar_color,
      a1.product_image_url as root_url,
      a2.product_image_url as similar_url
    FROM parsed_active a1
    INNER JOIN parsed_active a2
      ON a1.advertiser_name = a2.advertiser_name
      AND a1.product_brand = a2.product_brand
      AND a1.product_id != a2.product_id
      AND a1.color != a2.color
      AND size(a1.split_product_name) = size(a2.split_product_name)
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
    WHERE size(root_product_name) = n_matching_words
  ), t2 AS (
    SELECT 
      product_id,
      similar_product_id
    FROM t1
      UNION ALL
    SELECT 
      t1.product_id as product_id,
      tsim.similar_product_id as similar_product_id
    FROM t1
    INNER JOIN t1 tmid
    ON t1.product_id = tmid.similar_product_id
    INNER JOIN t1 tsim
    ON tmid.product_id = tsim.product_id
    WHERE t1.root_color != tsim.similar_color
  )


  SELECT 
    product_id,
    similar_product_id as alternate_color_product_id
  FROM t2
  WHERE product_id != similar_product_id
);

SELECT 
  *
FROM final_colors
