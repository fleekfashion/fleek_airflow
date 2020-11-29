
CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT * FROM {{params.product_info_table}} 
  WHERE execution_date='{{ ds }}'
);

CREATE OR REPLACE TEMPORARY VIEW processed_urls AS (

  -- forever 21 
  SELECT 
    product_id,
    regexp_replace(product_image_url, 'default_330', 'default_750') as image_url
  FROM pinfo
  WHERE 
    advertiser_name='Forever 21' 

    UNION ALL

  -- princess polly 
  SELECT 
    product_id,
    regexp_replace(product_image_url, '_large\\.jpg', '_750x\\.jpg') as image_url
  FROM pinfo
  WHERE 
    advertiser_name='Princess Polly' 

);

MERGE INTO {{params.product_info_table}} AS TARGET
USING processed_urls AS SRC
ON TARGET.execution_date='{{ds}}' AND SRC.product_id = TARGET.product_id
WHEN MATCHED THEN UPDATE SET
  TARGET.product_image_url=SRC.image_url
