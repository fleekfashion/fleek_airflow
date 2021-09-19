CREATE OR REPLACE TEMPORARY VIEW smart_tag_advertiser AS (
  WITH distinct_product_smart_tag AS (
    SELECT DISTINCT
      product_id,
      suggestion_hash as smart_tag_id
      FROM {{ params.product_smart_tag_table }}
  )
  SELECT 
    dpst.product_id,
    dpst.smart_tag_id,
    ap.advertiser_name
  FROM {{ params.active_products_table }} ap
  INNER JOIN distinct_product_smart_tag dpst
    ON ap.product_id = dpst.product_id
);

CREATE OR REPLACE TEMPORARY VIEW advertiser_counts AS ( SELECT 
    COUNT(*) as advertiser_count,
    advertiser_name
  FROM {{ params.active_products_table }} ap
  GROUP BY advertiser_name
);

CREATE OR REPLACE TEMPORARY VIEW brand_compositions AS (
  WITH t AS (
    SELECT 
      count(*) as c,
      smart_tag_id,
      advertiser_name
    FROM smart_tag_advertiser
    GROUP BY advertiser_name, smart_tag_id
  )
  SELECT 
    t.*,
    ac.advertiser_count,
    t.c/ac.advertiser_count as brand_pct,
    avg(t.c/ac.advertiser_count) OVER (PARTITION BY smart_tag_id) as global_pct
  FROM t
  INNER JOIN advertiser_counts ac
  ON ac.advertiser_name = t.advertiser_name
);

CREATE OR REPLACE TEMP VIEW ranked_attributes AS (
  WITH t AS (
    SELECT 
      bc.*,
      bc.brand_pct/bc.global_pct AS pct_ratio,
      log(c)*bc.brand_pct/bc.global_pct as score
    FROM brand_compositions bc
    wHERE brand_pct > {{ params.min_brand_pct }}
      AND c > {{ params.min_c }} 

  ), t2 AS (
    SELECT
      t.*,
      st.suggestion,
      st.product_label,
      ROW_NUMBER() OVER (
        PARTITION BY advertiser_name 
        ORDER BY t.score DESC
      ) as rank,
      ROW_NUMBER() OVER (
        PARTITION BY advertiser_name, product_label 
        ORDER BY t.score DESC
      ) as plrank
    FROM t
    INNER JOIN {{ params.smart_tag_table }} st
      ON t.smart_tag_id = st.smart_tag_id
    WHERE char_length(product_label) > 1
  )

  SELECT
    *
  FROM t2
  WHERE rank < 1000
);

SELECT
  *
FROM ranked_attributes


