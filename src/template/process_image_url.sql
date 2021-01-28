CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT DISTINCT
    product_id,
    product_image_url,
    advertiser_name
  FROM {{params.product_info_table}} 
);

CREATE OR REPLACE TEMPORARY VIEW processed_urls AS (

  -- forever 21 
  SELECT 
    product_id,
    regexp_replace(product_image_url, 'default_330', 'default_750') as product_image_url
  FROM pinfo
  WHERE 
    advertiser_name='Forever 21' 

    UNION ALL

  -- princess polly 
  SELECT 
    product_id,
    regexp_replace(product_image_url, '_large\\.jpg', '_750x\\.jpg') as product_image_url
  FROM pinfo
  WHERE 
    advertiser_name='Princess Polly' 

);

SELECT *
FROM processed_urls
