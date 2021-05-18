CREATE OR REPLACE TEMPORARY VIEW pi AS (
  SELECT 
    *
  FROM {{ params.image_download_table }} im
  WHERE im.image_hash NOT IN (
    SELECT image_hash
    FROM {{ params.invalid_images_table }}
  )
);

SELECT *
FROM pi
