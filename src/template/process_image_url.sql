CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT DISTINCT
    product_id,
    product_image_url,
    advertiser_name
  FROM {{params.product_info_table}} 
);

CREATE OR REPLACE TEMPORARY VIEW processed_urls AS (

  -- American Eagle 
  SELECT 
    product_id,
    regexp_replace(
      regexp_replace(
        product_image_url, 
        '\\?.*', '\\?\\$\pdp-m-opt\\$\&fmt=webp'
      ),
      '_f\\?',
      '_of\\?'
    ) as product_image_url
  FROM pinfo
  WHERE 
    advertiser_name = 'American Eagle'

    UNION ALL

  -- forever 21 
  SELECT 
    product_id,
    regexp_replace(product_image_url, 'default_330', 'default_750') as product_image_url
  FROM pinfo
  WHERE 
    advertiser_name = 'Forever 21' 

    UNION ALL

  -- princess polly 
  SELECT 
    product_id,
    regexp_replace(product_image_url, '_large\\.jpg', '_750x\\.jpg') as product_image_url
  FROM pinfo
  WHERE 
    advertiser_name = 'Princess Polly' 

    UNION ALL

  -- ROMWE and SHEIN
  SELECT 
    product_id,
  regexp_replace(
    product_image_url, 
    'thumbnail_.*', 
    'thumbnail_600x\\.jpg'
  ) as product_image_url
  FROM pinfo 
  WHERE 
    advertiser_name = 'ROMWE' or advertiser_name = 'SHEIN'

    UNION ALL

  -- Urban 
  SELECT 
    product_id,
  regexp_replace(
    product_image_url, 
    '\\?.*', 
    '\\?\\$\xlarge\\$'
  ) as product_image_url
  FROM pinfo 
  WHERE 
    advertiser_name = 'Urban Outfitters'

    UNION ALL

  -- Warp + Weft
  SELECT 
    product_id,
  regexp_replace(
    product_image_url, 
    'small', 
    '1200x'
  ) as product_image_url
  FROM pinfo 
  WHERE 
    advertiser_name = 'Warp + Weft'
);

SELECT *
FROM processed_urls
