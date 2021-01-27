{% for kv in params.updates %}
CREATE OR REPLACE TEMPORARY VIEW temp_{{kv[0]}}_view AS (
  SELECT
    product_id,
    '{{kv[0]}}' as product_label
  FROM {{ params.src}}
  WHERE {{kv[1]}}
);
CACHE TABLE temp_{{kv[0]}}_view;
{% endfor %}

{% for kv in params.updates[:-1] %}
  SELECT *
  FROM temp_{{kv[0]}}_view
UNION ALL
{% endfor %} 
  SELECT * 
  FROM temp_{{ params.updates[-1][0]}}_view
  
