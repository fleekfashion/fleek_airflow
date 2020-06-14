DELETE FROM `{{ params.table }}` t
WHERE NOT EXISTS (
  SELECT * 
  FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY product_id) as rn 
      FROM `{{ params.table }}` 
  ) d
  WHERE d.rn = 1 
  {% for col in params.columns %}
  AND t.{{col}} = d.{{col}}
  {% endfor %}
 )
