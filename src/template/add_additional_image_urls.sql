CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT * FROM {{product_info_table}} 
  WHERE execution_date='{{ ds }}'
);

CREATE OR REPLACE TEMPORARY VIEW processed_urls AS (
  -- Free People
  SELECT 
    product_id,
    TRANSFORM( array('a', 'b', 'c', 'd', 'e', 'f'), x -> regexp_replace(product_image_url, '_.\\?', format_string('_%s?', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE advertiser_name='Free People'

    UNION ALL

  -- Revolve
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(product_image_url, 'V.\.jpg?', format_string('V%d.jpg?', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE advertiser_name='REVOLVE'


    UNION ALL

  -- Asos
  SELECT 
    product_id,
    TRANSFORM( sequence(2, 4), x -> regexp_replace(element_at(product_additional_image_urls, 1), '\\-.\\?', format_string('-%d?', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE 
    advertiser_name='ASOS' 
    AND size(product_additional_image_urls) > 0

    UNION ALL

  -- boohoo
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(product_image_url, '\\.jpg', format_string('_%d.jpg', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE 
    advertiser_name='boohoo.com' 


    UNION ALL

  -- madewell
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 2), x -> regexp_replace(product_image_url, '_m\\?', format_string('_d%d?', x)   )  )  as additional_image_urls
  FROM test.product_info
  WHERE 
    advertiser_name='Madewell US' 
    AND size(product_additional_image_urls) > 0

    UNION ALL

  -- nastygal
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(product_image_url, '\\.jpg', format_string('_%d.jpg', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE 
    advertiser_name='NastyGal' 

    UNION ALL

  -- topshop
  SELECT 
    product_id,
    TRANSFORM( sequence(2, 4), x -> regexp_replace(product_image_url, '_.\\.jpg', format_string('_%d.jpg', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE 
    advertiser_name='Topshop' 

    UNION ALL

  -- pacsun
  SELECT 
    product_id,
    TRANSFORM( sequence(2, 4), x -> regexp_replace(product_image_url, '_00_', format_string('_0%d_', x)   )  ) as additional_image_urls
  FROM test.product_info
  WHERE 
    advertiser_name='PacSun' 
);

MERGE INTO {{product_info_table}} AS TARGET
USING processed_urls AS SRC
ON TARGET.execution_date={{ds}} AND SRC.product_id = TARGET.product_id
WHEN MATCHED THEN UPDATE SET
  TARGET.product_additional_image_urls=array_remove(
    array_union(
      TARGET.product_additional_image_urls,
      SRC.additional_image_urls
    ),
    product_image_url
  )

