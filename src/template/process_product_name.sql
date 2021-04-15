CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT DISTINCT
    product_id,
    aggregate(
      array(
        'MANGO -'
      ),
      product_name,
      (acc, x) -> trim(regexp_replace(acc, x, ''))
    ) as product_name,
    regexp_replace(color, '[^0-9a-zA-Z:, ]+', '') as color, -- remove all special regex character from color (else errors)
    COALESCE(size, 'absurd_impossible_string_tthat_means_nothing') as size -- b/c size can be null
  FROM {{params.product_info_table}} 
);

WITH t AS (
  SELECT
    product_id,
    aggregate(
      array(
        ' - .*',
        '\\..*',
        '(?i)\\bextra\\b', 
        '(?i)\\bx\\b', 
        '(?i)\\bsmall\\b', 
        '(?i)\\bmedium\\b', 
        '(?i)\\blarge\\b', 
        array_join(ARRAY('(?i)\\b', size, '\\b'), ''),
        '(?i)\\bsize$',
        array_join(ARRAY('(?i)\\bin ', color, '.*'), ''),
        array_join(ARRAY('(?i)-.', color, '.*'), '')
      ),
      product_name,
      (acc, x) -> trim(regexp_replace(acc, x, ''))
    ) as product_name
  FROM pinfo
)

SELECT *
FROM t
