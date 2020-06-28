MERGE `{{params.historic_table}}` as TARGET
USING (
  SELECT * 
  FROM `{{params.active_table}}` active_table
  WHERE active_table.execution_date < DATE_ADD(DATE("{{ ds }}"), INTERVAL -2 DAY) 
) as SOURCE
  ON TARGET.product_id = SOURCE.product_id
  WHEN MATCHED THEN 
    UPDATE SET
      TARGET.n_views = SOURCE.n_views + TARGET.n_views,
      TARGET.n_likes = SOURCE.n_likes + TARGET.n_likes,
      TARGET.n_add_to_cart = SOURCE.n_add_to_cart + TARGET.n_add_to_cart,
      TARGET.n_conversions = SOURCE.n_conversions + TARGET.n_conversions
  WHEN NOT MATCHED BY TARGET THEN
    INSERT ( {{ params.columns }} )
    VALUES ( {{ params.columns }} )
