{% for batch_params in params.batch_params[:-1] %}
SELECT 
  user_id,
  {{ batch_params.batch }} as batch{% for i, ind in batch_params.inds %}, top_product_ids_{{ind}} as top_products_{{i}}{% endfor %}
FROM
  `{{ params.rec_table}}`
UNION ALL
{% endfor %}

SELECT 
  user_id,
  {{ params.batch_params[-1].batch }} as batch{% for i, ind in params.batch_params[-1].inds %}, top_product_ids_{{ind}} as top_products_{{i}}{% endfor %}
FROM
  `{{ params.rec_table}}`
