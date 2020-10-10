MERGE INTO {{params.historic_table}} as TARGET
USING (
  SELECT * 
  FROM {{params.active_table}} active_table
  WHERE product_id NOT IN (
    SELECT product_id 
    FROM {{params.product_info_table}}
    WHERE execution_date="{{ds}}"

  ) 
) as SOURCE
  ON TARGET.product_id = SOURCE.product_id
WHEN MATCHED THEN 
  UPDATE SET
    TARGET.n_views = SOURCE.n_views + TARGET.n_views,
    TARGET.n_likes = SOURCE.n_likes + TARGET.n_likes,
    TARGET.n_add_to_cart = SOURCE.n_add_to_cart + TARGET.n_add_to_cart,
    TARGET.n_conversions = SOURCE.n_conversions + TARGET.n_conversions
WHEN NOT MATCHED THEN
  INSERT ( {{ params.columns }} )
  VALUES ( {{ params.columns }} )
