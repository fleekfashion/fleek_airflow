UPDATE {{ params.active_table }}
SET product_tags=array_union(product_tags, array('{{ params.tag }}'))
WHERE execution_date > DATE_SUB('{{ ds }}', {{ params.n_days }})
