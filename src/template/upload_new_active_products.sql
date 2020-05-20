WITH emb_data AS (
  SELECT
  product_id,
  [
    {% for i in range(params.n_embs - 1) %} emb_{{i}}, {% endfor %}
    emb_{{params.n_embs - 1 }}
  ] as product_embedding
  FROM `{{ params.new_emb_table }}`
)

SELECT 
  i.*,
  e.product_embedding,
  1 as n_likes,
  1 as n_views,
  1 as n_add_to_cart,
  1 as n_conversions
FROM emb_data e
INNER JOIN `{{params.new_info_table}}` i
ON e.product_id = i.product_id
