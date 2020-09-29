MERGE INTO {{ params.active_table }} AS TARGET
  USING (
    SELECT {{ params.columns }} FROM (
      SELECT 
        pi.*,
        ml.product_image_embedding,
        0 as n_views,
        0 as n_likes,
        0 as n_add_to_cart,
        0 as n_conversions
      FROM {{ params.product_info_table }} pi
      INNER JOIN {{ params.prod_ml_features_table }} ml
        ON pi.product_id = ml.product_id
      WHERE pi.execution_date = '{{ ds }}'
    )
  ) AS SRC
ON TARGET.product_id=SRC.product_id
WHEN NOT MATCHED THEN
  INSERT ( {{ params.columns }} )
  VALUES ( {{ params.columns }} )
