{% for kv in params.label_filters %}
CREATE OR REPLACE TEMPORARY VIEW temp_{{ kv[0].replace("-", "_").replace(" ", "_") }}_view AS (
  SELECT
    product_id,
    '{{kv[0]}}' as {{ params.field_name }} 
  FROM {{ params.src}}
  WHERE {{kv[1]}}
);
CACHE TABLE temp_{{ kv[0].replace("-", "_").replace(" ", "_") }}_view;
{% endfor %}

{% for kv in params.label_filters[:-1] %}
  SELECT *
  FROM temp_{{ kv[0].replace("-", "_").replace(" ", "_") }}_view
UNION ALL
{% endfor %} 
  SELECT * 
  FROM temp_{{ params.label_filters[-1][0].replace("-", "_").replace(" ", "_") }}_view
  
