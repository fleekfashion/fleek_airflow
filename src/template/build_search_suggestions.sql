CREATE OR REPLACE TEMP VIEW synonyms AS (
  SELECT
    root,
    array_union(
      collect_set(synonym),
      array(root)
    )  as synonyms
  FROM {{ params.synonyms_table }} 
  GROUP BY root
);

CREATE OR REPLACE TEMP VIEW secondary_subsets AS (
  WITH t_sl AS (
    SELECT 
      product_id,
      explode(
        array_distinct(
          array_union(
            product_secondary_labels, 
            ARRAY('', coalesce(internal_color, ''))
          )
        )
      ) as sl
    FROM {{ params.active_products_table }}
  ), t_syn AS (
    SELECT 
      t_sl.*,
      coalesce(sy.synonyms, array()) as synonyms
    FROM t_sl
    LEFT JOIN synonyms sy
      ON t_sl.sl = sy.root
  )
  SELECT 
    t.product_id,
    array_distinct(
      ARRAY(t.sl, t2.sl, t3.sl)
    ) as secondary_subset
  FROM t_syn as t
  INNER JOIN t_syn as t2
    ON t.product_id = t2.product_id
  INNER JOIN t_syn as t3
    ON t.product_id = t3.product_id
  WHERE NOT array_contains(t.synonyms, t2.sl) 
    AND NOT array_contains(t.synonyms, t3.sl) 
    AND NOT array_contains(t2.synonyms, t3.sl)
);

CREATE OR REPLACE TEMP VIEW labelSyn AS (
  WITH t_pl AS (
    SELECT 
      product_id,
      explode(
        array_distinct(
          array_union(
            product_labels, 
            ARRAY('')
          )
        )
      ) as pl
    FROM {{ params.active_products_table }} 
  )
  SELECT 
    t_pl.*,
    coalesce(sy.synonyms, array()) as synonyms
  FROM t_pl
  LEFT JOIN synonyms sy
    ON t_pl.pl = sy.root
);

CREATE OR REPLACE TEMP VIEW suggestion_subsets AS (
 WITH t_filter AS (
    SELECT 
      labelSyn.product_id,
      filter(
        ss.secondary_subset,
        x -> array_contains(labelSyn.synonyms, x)
      )[0] as syn,
      labelSyn.pl,
      ss.secondary_subset
    FROM labelSyn
    INNER JOIN secondary_subsets ss
      ON labelSyn.product_id = ss.product_id
  )
  SELECT 
    product_id,
    pl as product_label,
    array_union(
      if(
        syn is not null,
        array_remove(
          secondary_subset,
          syn
        ),
        secondary_subset
      ),
      ARRAY(coalesce(syn, pl))
    ) as suggestion_subset
  FROM t_filter 
);

CREATE OR REPLACE TEMPORARY VIEW parsedSubsets AS (
  SELECT 
    product_id,
    product_label,
    array_join(
      array_remove(
        suggestion_subset,
        ''
      ),
      ' '
    ) as suggestion,
    abs(xxhash64(trim(
      array_join(
        array_sort(
          suggestion_subset
        ), 
        ' '
      )
    ))) as suggestion_hash
  FROM suggestion_subsets
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
      first(suggestion_hash) as suggestion_hash
    FROM parsedSubsets
    GROUP BY suggestion
  
  )
  SELECT 
    s.suggestion,
    c.c as n_hits,
    s.suggestion_hash,
    s.product_label,
    s.suggestion = s.product_label as is_base_label,
    c.c > {{ params.min_strong }} as is_strong_suggestion
  FROM hash_counts c
  INNER JOIN distinctSuggestions s
  ON c.suggestion = s.suggestion
  WHERE char_length(s.suggestion) > 0 AND c.c > {{ params.min_include }}
  ORDER BY c DESC
);

SELECT 
  *
FROM suggestions
