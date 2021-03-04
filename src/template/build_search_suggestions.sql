CREATE OR REPLACe TEMP VIEW secondary_subsets AS (
  WITH t AS (
    SELECT 
      product_id,
      array_union(product_labels, ARRAY('')) as product_labels,
      explode(
        array_union(
          product_secondary_labels, 
          ARRAY('')
        )
      ) as sl
    FROM {{ params.active_products_table }} 
  )

  SELECT 
    t.product_id,
    t.product_labels,
    t.sl as sl1,
    t2.sl as sl2,
    t3.sl as sl3,
    t4.sl as sl4,
    t5.sl as sl5
  FROM t
  INNER JOIN t as t2
    ON t.product_id = t2.product_id
  INNER JOIN t as t3
    ON t.product_id = t3.product_id
  INNER JOIN t as t4
    ON t.product_id = t4.product_id
  INNER JOIN t as t5
    ON t.product_id = t5.product_id
);

CREATE OR REPLACE TEMPORARY VIEW parsedSubsets AS (
  WITH t AS (
  SELECT DISTINCT
      product_id,
      explode(product_labels) as product_label,
      array_distinct(ARRAY(sl1, sl2, sl3, sl4, sl5)) as secondary_subset
    FROM secondary_subsets
  )
  SELECT 
    product_id,
    secondary_subset,
    array_remove(
      if(
        {{ params.product_hidden_labels_filter }},
        secondary_subset,
        array_union(secondary_subset, ARRAY(product_label))
      ),
      ''
    ) as suggestion_subset,
    product_label
  FROM t
);

CREATE OR REPLACE TEMPORARY VIEW suggestionsV1 AS (
  SELECT 
    product_id,
    trim(
      array_join(
        suggestion_subset, 
        ' '
      )
    ) as suggestion,
    abs(
      xxhash64(
        trim(
          array_join(
            array_sort(
              suggestion_subset
            ), 
            ' '
          )
        )
      )
    ) as suggestion_hash,
    secondary_subset,
    product_label
  FROM parsedSubsets
);

CREATE OR REPLACE TEMP VIEW hash_counts AS (
  WITH t AS (
    SELECT DISTINCT
      product_id,
      suggestion_hash
    FROM suggestionsV1
  )

  SELECT
    COUNT(*) as c,
    suggestion_hash
  FROM t
  GROUP BY suggestion_hash
);

CREATE OR REPLACE TEMPORARY VIEW suggestions AS (
  WITH suggestions AS (
    SELECT 
      suggestion,
      first(suggestion_hash) as suggestion_hash,
      first(product_label) as product_label
    FROM suggestionsV1
    GROUP BY suggestion, product_label
  )

  SELECT 
    s.suggestion,
    c.c as n_hits,
    c.suggestion_hash as suggestion_hash,
    s.product_label,
    s.suggestion = s.product_label as is_base_label,
    c.c > {{ params.min_strong }} as is_strong_suggestion
  FROM hash_counts c
  INNER JOIN suggestions s
  ON c.suggestion_hash = s.suggestion_hash
  WHERE char_length(s.suggestion) > 0 AND c.c > {{ params.min_include }}
  ORDER BY c DESC
);

SELECT
  *
FROM suggestions
