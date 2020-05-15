SELECT 
  cj.*,
  active_table.product_embedding,
  active_table.n_views,
  active_table.n_likes,
  active_table.n_add_to_cart,
  active_table.n_conversions
FROM `{{params.cj_table}}` cj
INNER JOIN `{{params.active_table}}` active_table
  ON cj.product_id = active_table.product_id
