CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT DISTINCT
    product_id,
    product_name,
    color,
    size
  FROM {{params.product_info_table}} 
);

SELECT
  product_id,
  regexp_replace(
    regexp_replace(
      regexp_replace(
        product_name,
        size,
        ''
      ),
      color,
      ''
    )
    ' - .*', 
    ''
  ) as product_name 
FROM pinfo
