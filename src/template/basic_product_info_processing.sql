CREATE OR REPLACE TEMPORARY VIEW basic_parsed_products AS (
  WITH t as (
    SELECT
      DISTINCT *
      FROM {{ params.src }}
  )
  SELECT
    {{ params.unchanged_columns }},
    DATE('{{ ds }}') as execution_date,
    CAST({{ execution_date.int_timestamp }} as bigint) as execution_timestamp,
    abs(xxhash64(advertiser_name, product_image_url)) as product_id,
    CAST( ARRAY() as array<string> ) as product_tags,
    CAST( ARRAY() as array<string> ) as product_labels,
    CAST( ARRAY() as array<string> ) as product_secondary_labels,
    CAST( ARRAY() as array<string> ) as product_external_labels,
    COALESCE(product_sale_price, product_price) as product_sale_price
  FROM t
);

SELECT *
FROM basic_parsed_products
WHERE {{ params.required_fields_filter }}

