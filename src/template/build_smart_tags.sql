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
      first(suggestion_hash) as suggestion_hash,
      max(product_label) as product_label,
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
  FROM Suggestions 
  WHERE suggestion not rlike internal_color
)
SELECT
  *
FROM t
WHERE 
  size(product_secondary_labels) IN (1, 2)
);

CREATE OR REPLACE TEMPORARY VIEW redundant_tag_ids AS (
  WITH t2 AS (
    SELECT 
      *,
      substring_index(
        suggestion,
        ' ',
        size(split(suggestion, ' ')) - 1
      ) as no_label_suggestion
    FROM smart_tags
  )

  SELECT t1.smart_tag_id
  FROM smart_tags t1
  INNER JOIN t2
    ON t1.suggestion=t2.no_label_suggestion
  WHERE 
    t2.n_hits/t1.n_hits > .6
);

SELECT 
  smart_tag_id,
  first(suggestion) as suggestion,
  max(product_label) as product_label,
  first(product_secondary_labels) as product_secondary_labels,
  first(n_hits) as n_hits
FROM smart_tags
WHERE smart_tag_id NOT IN (
  SELECT smart_tag_id
  FROM redundant_tag_ids
)
GROUP BY smart_tag_id 
