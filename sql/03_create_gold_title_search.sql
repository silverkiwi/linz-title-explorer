CREATE OR REPLACE TABLE gold.linz_title_search AS
WITH estate_agg AS (
  SELECT
    ttl_title_no,
    COUNT(*) AS estate_count,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_estate_count,
    element_at(sort_array(collect_set(CASE WHEN is_current THEN type END)), 1) AS primary_estate_type,
    sort_array(filter(collect_set(type), x -> x IS NOT NULL)) AS estate_types,
    concat_ws('; ', filter(collect_list(share), x -> x IS NOT NULL)) AS estate_share_summary,
    concat_ws('; ', filter(collect_list(purpose), x -> x IS NOT NULL)) AS estate_purpose_summary,
    max(CASE WHEN timeshare_week_no IS NOT NULL THEN TRUE ELSE FALSE END) AS has_timeshare_estate,
    max(CASE WHEN term IS NOT NULL THEN TRUE ELSE FALSE END) AS has_term_estate,
    collect_list(named_struct(
      'id', id,
      'type', type,
      'status', status,
      'share', share,
      'purpose', purpose,
      'term', term,
      'original_flag', original_flag
    )) AS estates
  FROM silver.linz_title_estate
  GROUP BY ttl_title_no
),
instrument_detail AS (
  SELECT
    tit.ttl_title_no,
    ti.id AS tin_id,
    ti.inst_no,
    ti.trt_grp,
    ti.trt_type,
    tt.description AS transaction_type_description,
    ti.status,
    ti.lodged_datetime,
    ti.priority_no
  FROM silver.linz_title_instrument_title tit
  JOIN silver.linz_title_instrument ti
    ON tit.tin_id = ti.id
  LEFT JOIN silver.linz_transaction_type tt
    ON ti.trt_grp = tt.grp AND ti.trt_type = tt.type
),
instrument_agg AS (
  SELECT
    ttl_title_no,
    COUNT(DISTINCT tin_id) AS instrument_count,
    SUM(CASE WHEN status IN ('REGD', 'LIVE') THEN 1 ELSE 0 END) AS registered_instrument_count,
    max_by(tin_id, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_id,
    max_by(inst_no, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_no,
    max_by(trt_type, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_type,
    max_by(transaction_type_description, named_struct('lodged_datetime', coalesce(lodged_datetime, DATE '1900-01-01'), 'tin_id', tin_id)) AS latest_instrument_description,
    max(lodged_datetime) AS latest_lodged_date,
    collect_list(named_struct(
      'tin_id', tin_id,
      'inst_no', inst_no,
      'trt_grp', trt_grp,
      'trt_type', trt_type,
      'description', transaction_type_description,
      'status', status,
      'lodged_datetime', lodged_datetime,
      'priority_no', priority_no
    )) AS instruments
  FROM instrument_detail
  GROUP BY ttl_title_no
),
encumbrance_agg AS (
  SELECT
    te.ttl_title_no,
    COUNT(*) AS encumbrance_count,
    SUM(CASE WHEN te.is_current THEN 1 ELSE 0 END) AS current_encumbrance_count,
    MAX(CASE WHEN te.is_current THEN TRUE ELSE FALSE END) AS has_current_encumbrance,
    max(te.enc_id) AS latest_encumbrance_id,
    collect_list(named_struct(
      'title_encumbrance_id', te.id,
      'enc_id', te.enc_id,
      'status', te.status,
      'act_tin_id_crt', te.act_tin_id_crt,
      'act_id_crt', te.act_id_crt,
      'encumbrance_status', e.status,
      'term', e.term
    )) AS encumbrances
  FROM silver.linz_title_encumbrance te
  LEFT JOIN silver.linz_encumbrance e
    ON te.enc_id = e.id
  GROUP BY te.ttl_title_no
),
follow_on_agg AS (
  SELECT
    ttl_title_no_prior AS title_no,
    COUNT(*) AS follow_on_title_count,
    sort_array(collect_set(ttl_title_no_flw)) AS follow_on_titles
  FROM silver.linz_title_hierarchy
  WHERE ttl_title_no_prior IS NOT NULL AND ttl_title_no_flw IS NOT NULL
  GROUP BY ttl_title_no_prior
),
prior_agg AS (
  SELECT
    ttl_title_no_flw AS title_no,
    COUNT(*) AS prior_title_count,
    sort_array(collect_set(ttl_title_no_prior)) AS prior_titles
  FROM silver.linz_title_hierarchy
  WHERE ttl_title_no_prior IS NOT NULL AND ttl_title_no_flw IS NOT NULL
  GROUP BY ttl_title_no_flw
),
doc_ref_agg AS (
  SELECT
    tit.ttl_title_no,
    COUNT(*) AS document_reference_count,
    collect_list(named_struct(
      'id', dr.id,
      'type', dr.type,
      'tin_id', dr.tin_id,
      'reference', dr.reference
    )) AS document_references
  FROM silver.linz_title_instrument_title tit
  JOIN silver.linz_title_document_reference dr
    ON tit.tin_id = dr.tin_id
  GROUP BY tit.ttl_title_no
)
SELECT
  t.title_no,
  t.status AS title_status,
  t.type AS title_type,
  t.register_type,
  t.issue_date,
  t.guarantee_status,
  t.provisional,
  t.maori_land,
  t.ste_id,
  t.sur_wrk_id,
  t.ldt_loc_id,
  t.ttl_title_no_srs,
  t.ttl_title_no_head_srs,
  t.is_current,
  CASE WHEN t.status = 'LIVE' THEN TRUE ELSE FALSE END AS is_live,
  coalesce(e.estate_count, 0) AS estate_count,
  coalesce(e.current_estate_count, 0) AS current_estate_count,
  e.primary_estate_type,
  coalesce(e.estate_types, array()) AS estate_types,
  e.estate_share_summary,
  e.estate_purpose_summary,
  coalesce(e.has_timeshare_estate, FALSE) AS has_timeshare_estate,
  coalesce(e.has_term_estate, FALSE) AS has_term_estate,
  coalesce(e.estates, array()) AS estates,
  coalesce(i.instrument_count, 0) AS instrument_count,
  coalesce(i.registered_instrument_count, 0) AS registered_instrument_count,
  i.latest_instrument_id,
  i.latest_instrument_no,
  i.latest_instrument_type,
  i.latest_instrument_description,
  i.latest_lodged_date,
  coalesce(i.instruments, array()) AS instruments,
  coalesce(enc.encumbrance_count, 0) AS encumbrance_count,
  coalesce(enc.current_encumbrance_count, 0) AS current_encumbrance_count,
  coalesce(enc.has_current_encumbrance, FALSE) AS has_current_encumbrance,
  enc.latest_encumbrance_id,
  coalesce(enc.encumbrances, array()) AS encumbrances,
  coalesce(p.prior_title_count, 0) AS prior_title_count,
  coalesce(f.follow_on_title_count, 0) AS follow_on_title_count,
  coalesce(p.prior_titles, array()) AS prior_titles,
  coalesce(f.follow_on_titles, array()) AS follow_on_titles,
  CASE
    WHEN coalesce(p.prior_title_count, 0) > 0 OR coalesce(f.follow_on_title_count, 0) > 0 THEN TRUE
    ELSE FALSE
  END AS has_title_lineage,
  coalesce(d.document_reference_count, 0) AS document_reference_count,
  coalesce(d.document_references, array()) AS document_references,
  concat_ws(' ',
    t.title_no,
    t.type,
    t.register_type,
    coalesce(i.latest_instrument_no, ''),
    coalesce(i.latest_instrument_description, '')
  ) AS search_text,
  current_timestamp() AS updated_at
FROM silver.linz_title t
LEFT JOIN estate_agg e ON t.title_no = e.ttl_title_no
LEFT JOIN instrument_agg i ON t.title_no = i.ttl_title_no
LEFT JOIN encumbrance_agg enc ON t.title_no = enc.ttl_title_no
LEFT JOIN prior_agg p ON t.title_no = p.title_no
LEFT JOIN follow_on_agg f ON t.title_no = f.title_no
LEFT JOIN doc_ref_agg d ON t.title_no = d.ttl_title_no;

OPTIMIZE gold.linz_title_search
ZORDER BY (title_no, title_status, title_type, latest_instrument_no, latest_lodged_date);
