CREATE OR REPLACE TEMP VIEW parsedSubsets AS (
  SELECT * FROM staging_boards.product_smart_tag
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

CREATE OR REPLACE TEMPORARY VIEW Suggestions AS (
  WITH distinctSuggestions AS (
    SELECT 
      suggestion,
      max(product_label) as product_label,
      first(suggestion_hash) as suggestion_hash,
      first(secondary_subset) as secondary_labels,
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
    secondary_labels,
    internal_color
  FROM hash_counts c
  INNER JOIN distinctSuggestions s
  ON c.suggestion = s.suggestion
  WHERE char_length(s.suggestion) > 0 AND c.c > {{ params.min_include }}
  ORDER BY c DESC
);

CREATE OR REPLACE TEMPORARY VIeW smart_tags AS (
WITH t AS (
  SELECT 
    suggestion_hash as smart_tag_id,
    suggestion,
    product_label,
    array_remove(secondary_labels, '') as product_secondary_labels,
    n_hits
  FROM test.tmp_suggestions
  WHERE suggestion not rlike internal_color
)
SELECT
  *
FROM t
WHERE 
  size(product_secondary_labels) = 1 or 
  ( size(product_secondary_labels) = 2 and char_length(product_label) > 1 )
);

SELECT *
FROM smart_tags
