CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT DISTINCT
    product_id,
    product_name,
    color,
    size
  FROM {{params.product_info_table}} 
);

WITH t AS (
  SELECT
    product_id,
    regexp_replace(
      regexp_replace(
        product_name,
        COALESCE(size, 'absurd_impossible_string_tthat_means_nothing'), -- b/c size can be null
        ''
      ),
      ' - .*', 
      ''
    ) as product_name 
  FROM pinfo
)

SELECT *
FROM t
