CREATE OR REPLACE TEMP VIEW parsedSubsets AS (
  SELECT *
  FROM {{ params.product_smart_tag_table }}
);

CREATE OR REPLACE TEMP VIEW hash_counts_pl AS (
  WITH t AS (
    SELECT DISTINCT
      product_id,
      suggestion,
      product_label
    FROM parsedSubsets
  )
  SELECT
    COUNT(*) as c,
    suggestion,
    product_label
  FROM t
  GROUP BY suggestion, product_label
);

CREATE OR REPLACE TEMP VIEW filtered_hash_counts AS (
  WITH t AS (
    SELECT
      suggestion,
      product_label,
      max(c) OVER (
        PARTITION BY suggestion 
        ORDER BY char_length(product_label) > 0 DESC, c DESC
      ) as max_c,
      c
    FROM hash_counts_pl
  )
  SELECT
    suggestion,
    max(product_label) as product_label,
    c
  FROM t
  WHERE c = max_c
  GROUP BY suggestion, c
);

CREATE OR REPLACE TEMPORARY VIEW suggestions AS (
  WITH distinctSuggestions AS (
    SELECT 
      suggestion,
      product_label,
      first(suggestion_hash) as suggestion_hash,
      first(secondary_subset) as secondary_subset,
      first(internal_color) as internal_color
    FROM parsedSubsets
    GROUP BY suggestion, product_label
  
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
  FROM filtered_hash_counts c
  INNER JOIN distinctSuggestions s
    ON c.suggestion = s.suggestion
    AND c.product_label = s.product_label
  WHERE char_length(s.suggestion) > 0 AND c.c > {{ params.min_include }}
  ORDER BY c DESC
);

SELECT 
  *
FROM suggestions
