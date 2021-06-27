CREATE OR REPLACE TEMPORARY VIEW pinfo AS (
  SELECT 
    pi.product_id,
    first(pi.advertiser_name) as advertiser_name,
    first(
      COALESCE(urls.product_image_url, pi.product_image_url) 
    ) as product_image_url,
    first(pi.product_additional_image_urls) as product_additional_image_urls
  FROM {{params.product_info_table}} pi
  LEFT JOIN {{ params.image_urls_table }} urls
    ON pi.product_id = urls.product_id
  GROUP BY pi.product_id
);

CREATE OR REPLACE TEMPORARY VIEW processed_urls AS (
  -- Revolve
  SELECT 
    product_id,
    TRANSFORM( sequence(2, 4), x -> regexp_replace(product_image_url, 'V.\.jpg?', format_string('V%d.jpg?', x)   )  ) as product_additional_image_urls
  FROM pinfo
  WHERE advertiser_name='REVOLVE'

  UNION ALL

  -- Asos
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(element_at(product_additional_image_urls, 1), '\\-.\\?', format_string('-%d?', x)   )  ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='ASOS' 
    AND size(product_additional_image_urls) > 0

  UNION ALL

  -- american eagle 
  SELECT 
    product_id,
    TRANSFORM(
      array('_ob\\?', '_os\\?', '_f\\?', '_b\\?', '_s\\?'),
      x -> regexp_replace(
        product_image_url,
        '_of\\?',
        x
      )
    ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name = 'American Eagle'

  UNION ALL

  -- forever 21 
  SELECT 
    product_id,
    TRANSFORM(
      array('2_side_', '3_back_', '4_full_'),
      x -> regexp_replace(
        regexp_replace(product_image_url, 'default_', x),
        '1_front_',
        x
      )
    ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='Forever 21' 

  UNION ALL

  -- topshop
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(product_image_url, '_.\\.jpg', format_string('_%d.jpg', x)   )  ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='Topshop' 

  UNION ALL

  -- pacsun
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(product_image_url, '_00_', format_string('_0%d_', x)   )  ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='PacSun' 

  UNION ALL

  -- Free People increase to f later
  SELECT 
    product_id,
    TRANSFORM( array('a', 'b', 'c'), x -> regexp_replace(product_image_url, '_.\\?', format_string('_%s?', x)   )  ) as product_additional_image_urls
  FROM pinfo
  WHERE advertiser_name='Free People'

  UNION ALL

  -- nastygal
  SELECT 
    product_id,
    TRANSFORM(
      sequence(1, 4),
      x -> regexp_replace(product_image_url, '\\.jpg', format_string('_%d.jpg', x))
    ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='NastyGal' 

  UNION ALL

  -- boohoo 
  SELECT 
    product_id,
    TRANSFORM( sequence(1, 4), x -> regexp_replace(product_image_url, '\\.jpg', format_string('_%d.jpg', x)   )  ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='boohoo.com' 

  UNION ALL

  -- ROMWE and SHEIN
  SELECT 
    product_id,
    TRANSFORM( 
      product_additional_image_urls, 
      x -> regexp_replace(
        x,
        'thumbnail_.*',
        'thumbnail_600x\\.jpg'
      )
    ) as product_additional_image_urls
  FROM pinfo 
  WHERE 
    advertiser_name = 'ROMWE' or advertiser_name = 'SHEIN'

  UNION ALL

  -- Urban 
  SELECT 
    product_id,
    TRANSFORM( 
      if(
        product_image_url rlike '_[ab]\\?',
        ARRAY('d', 'e', 'f'),
        ARRAY('e', 'f', 'g', 'h')
      ),
      x -> regexp_replace(
        product_image_url,
        '_[abcd]\\?',
        format_string('_%s?', x)
      )
    ) as product_additional_image_urls
  FROM pinfo 
  WHERE 
    advertiser_name = 'Urban Outfitters'

  UNION ALL

  -- Champion 
  SELECT 
    product_id,
    TRANSFORM( 
      if(
        product_image_url rlike '(_Front|_Coed)',
        ARRAY('Front', 'Back', 'Side'),
        ARRAY('back', 'side')
      ),
      x -> regexp_replace(
        product_image_url,
        '(?i)(_Front|_Coed)',
        format_string('_%s', x)
      )
    ) as product_additional_image_urls
  FROM pinfo 
  WHERE 
    advertiser_name = 'Champion'

  UNION ALL

  -- madewell
  SELECT 
    product_id,
    array_union(
      TRANSFORM(
        sequence(1, 2),
        x -> regexp_replace(product_image_url, '_m\\?', format_string('_d%d?', x))
      ),
      product_additional_image_urls
    ) as product_additional_image_urls
  FROM pinfo
  WHERE 
    advertiser_name='Madewell US' 
    AND size(product_additional_image_urls) > 0
);

SELECT 
  urls.product_id,
  array_remove(
    array_distinct(urls.product_additional_image_urls), 
    pi.product_image_url
  ) as product_additional_image_urls
FROM processed_urls urls
INNER JOIN pinfo pi
  ON pi.product_id = urls.product_id
