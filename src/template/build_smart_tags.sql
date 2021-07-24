CREATE OR REPLACE TEMP VIEW parsedSubsets AS (
  SELECT * FROM {{ params.product_smart_tag_table }} 
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
  SELECT *
  FROM t
  WHERE c = max_c
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
  FROM filtered_hash_counts c
  INNER JOIN distinctSuggestions s
    ON c.suggestion = s.suggestion
    AND c.product_label = s.product_label
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
  WHERE internal_color is null -- do not filter out null internal colors
    or suggestion not rlike internal_color -- filter out colors tho
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
    t2.n_hits/t1.n_hits > .65
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
