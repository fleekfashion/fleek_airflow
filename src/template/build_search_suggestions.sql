CREATE OR REPLACE TEMP VIEW parsedSubsets AS (
  SELECT *
  FROM {{ params.product_smart_tag_table }}
);

CREATE OR REPLACE TEMP VIEW hash_counts AS (
  WITH t AS (
    SELECT DISTINCT
      product_id,
      suggestion
    FROM parsedSubsets
  )
  SELECT
    COUNT(*) as c,
    suggestion
  FROM t
  GROUP BY suggestion
);

CREATE OR REPLACE TEMPORARY VIEW suggestions AS (
  WITH distinctSuggestions AS (
    SELECT 
      suggestion,
      max(product_label) as product_label,
      first(suggestion_hash) as suggestion_hash,
      first(secondary_subset) as secondary_subset,
      first(internal_color) as internal_color
    FROM parsedSubsets
    GROUP BY suggestion
  
  )
  SELECT 
    s.suggestion,
    c.c as n_hits,
    s.suggestion_hash,
    s.product_label,
    s.suggestion = s.product_label as is_base_label,
    c.c > {{ params.min_strong }} as is_strong_suggestion,
    secondary_subset as secondary_labels,
    internal_color
  FROM hash_counts c
  INNER JOIN distinctSuggestions s
  ON c.suggestion = s.suggestion
  WHERE char_length(s.suggestion) > 0 AND c.c > {{ params.min_include }}
  ORDER BY c DESC
);

SELECT 
  *
FROM suggestions
