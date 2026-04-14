# LINZ Databricks + Website Plan

## Summary

We can proceed now, but there is one important constraint:

`lds-world-10layers-DBF.zip` is **title/instrument-centric**, not parcel/map-centric.

That means:
- we **can** build a very useful first website now
- we **cannot yet** build the full parcel-map experience from this zip alone
- for the true "easy property search + map" product, we will later need weekly parcel/boundary/property layers as well

So the right plan is:

1. **Phase 1:** load the 10 DBF layers into Databricks
2. build a **wide title-centric serving table** for search and detail pages
3. ship a very simple website around title / instrument / encumbrance discovery
4. **Phase 2:** add parcel/property/boundary layers and evolve to the full map-based property experience
5. later automate the **weekly acquisition + ingest pipeline**

---

## What is in `lds-world-10layers-DBF.zip`

Confirmed layers:

1. `landonline-title`
2. `landonline-title-estate`
3. `landonline-title-instrument`
4. `landonline-title-instrument-title`
5. `landonline-title-hierarchy`
6. `landonline-title-encumbrance`
7. `landonline-encumbrance`
8. `landonline-encumbrance-share`
9. `landonline-title-document-reference`
10. `landonline-transaction-type`

Approximate row counts from the DBFs:

- `landonline-title`: 4,255,960
- `landonline-title-estate`: 2,700,000
- `landonline-title-instrument-title`: 28,105,465
- `landonline-title-instrument`: 15,400,000
- `landonline-title-hierarchy`: 3,405,974
- `landonline-title-encumbrance`: 3,443,144
- `landonline-encumbrance`: 1,835,605
- `landonline-encumbrance-share`: 1,568,408
- `landonline-title-document-reference`: 168,535
- `landonline-transaction-type`: 524

---

## What is missing for the final parcel-map product

Not present in this 10-layer zip:

- parcel geometry
- parcel labels
- appellations / legal descriptions
- proprietors / owner names
- survey parcel relationships
- localities / land districts as dimensions
- site / statistical area layers

So:
- **search by title number** = yes
- **search by dealing/instrument** = yes
- **search by encumbrance / legal chain** = yes
- **search by owner name** = not from this zip alone
- **map parcels** = not from this zip alone
- **search by legal description / appellation** = not from this zip alone

---

## Product recommendation

## MVP website now: title search

Build the first website around:

- title number search
- title details
- estate summary
- instrument history
- prior/follow-on title lineage
- encumbrance summary
- linked document references

Very simple UX:

### Homepage
- one search box
- tabs or filter chips:
  - Titles
  - Instruments
  - Encumbrances
- results list
- details drawer / detail page

### Result card
- title number
- status
- title type
- register type
- issue date
- current estate summary
- counts:
  - linked instruments
  - prior titles
  - follow-on titles
  - encumbrances

### Detail page
- header: title number + status
- estate section
- instrument history section
- encumbrance section
- title lineage section
- references section

This is still easy to use, even before parcels arrive.

---

## Databricks data architecture

### Bronze
Raw ingested copies of each DBF.

Tables:
- `bronze.linz_title_raw`
- `bronze.linz_title_estate_raw`
- `bronze.linz_title_instrument_raw`
- `bronze.linz_title_instrument_title_raw`
- `bronze.linz_title_hierarchy_raw`
- `bronze.linz_title_encumbrance_raw`
- `bronze.linz_encumbrance_raw`
- `bronze.linz_encumbrance_share_raw`
- `bronze.linz_title_document_reference_raw`
- `bronze.linz_transaction_type_raw`

Recommended raw metadata columns added during ingest:
- `source_file`
- `source_table`
- `source_extract_date`
- `ingested_at`
- `ingest_batch_id`

### Silver
Cleaned, typed, de-truncated, semantically named tables.

Examples:
- `silver.linz_title`
- `silver.linz_title_estate`
- `silver.linz_title_instrument`
- `silver.linz_title_instrument_title`
- `silver.linz_title_hierarchy`
- `silver.linz_title_encumbrance`
- `silver.linz_encumbrance`
- `silver.linz_encumbrance_share`
- `silver.linz_title_document_reference`
- `silver.linz_transaction_type`

### Gold
Serving layer for app/API.

For current feed:
- `gold.linz_title_search`
- optional later: `gold.linz_instrument_search`

For future parcel feed:
- `gold.linz_property_search`

---

## Important DBF field-name mapping

DBF truncates field names to 10 chars. We should normalize these in silver.

### `landonline-title`
- `register_t` -> `register_type`
- `guarantee_` -> `guarantee_status`
- `provisiona` -> `provisional`
- `ttl_title_` -> `ttl_title_no_srs`
- `ttl_titl_1` -> `ttl_title_no_head_srs`

### `landonline-title-estate`
- `ttl_title_` -> `ttl_title_no`
- `timeshare_` -> `timeshare_week_no`
- `act_tin_id` -> `act_tin_id_crt`
- `original_f` -> `original_flag`
- `tin_id_ori` -> `tin_id_orig`

### `landonline-title-instrument`
- `lodged_dat` -> `lodged_datetime` (DBF stores only date here)
- `priority_n` -> `priority_no`
- `tin_id_par` -> `tin_id_parent`

### `landonline-title-instrument-title`
- `tin_id` -> `tin_id`
- `ttl_title_` -> `ttl_title_no`

### `landonline-title-hierarchy`
- `ttl_title_` -> `ttl_title_no_prior`
- `ttl_titl_1` -> `ttl_title_no_flw`

### `landonline-title-document-reference`
- `reference_` -> `reference`

### `landonline-encumbrance`
- `act_tin__1` -> `act_tin_id_orig`

### `landonline-encumbrance-share`
- `act_tin__1` -> `act_tin_id_ext`
- `system_crt` -> `system_created`
- `system_ext` -> `system_extinguished`

### `landonline-transaction-type`
- `descriptio` -> `description`

---

## Recommended wide serving model now

Because the available data is title-centric, the first wide table should be:

## `gold.linz_title_search`

**Grain:** 1 row per title number

This is the best current serving table because:
- `landonline-title` is the clean anchor
- most website actions are title lookups
- we can aggregate instruments, encumbrances, hierarchy, references
- Databricks handles wide columnar tables well

### Core scalar columns
- `title_no`
- `title_status`
- `title_type`
- `register_type`
- `issue_date`
- `guarantee_status`
- `provisional`
- `maori_land`
- `ste_id`
- `sur_wrk_id`
- `ldt_loc_id`
- `ttl_title_no_srs`
- `ttl_title_no_head_srs`

### Estate summary columns
- `estate_count`
- `current_estate_count`
- `estate_types`
- `current_estate_types`
- `primary_estate_type`
- `estate_share_summary`
- `estate_purpose_summary`
- `has_timeshare_estate`
- `has_term_estate`

### Instrument summary columns
- `instrument_count`
- `registered_instrument_count`
- `latest_instrument_id`
- `latest_instrument_no`
- `latest_instrument_type`
- `latest_instrument_description`
- `latest_lodged_date`

### Encumbrance summary columns
- `encumbrance_count`
- `current_encumbrance_count`
- `latest_encumbrance_id`
- `has_current_encumbrance`

### Hierarchy summary columns
- `prior_title_count`
- `follow_on_title_count`
- `has_title_lineage`
- `prior_titles`
- `follow_on_titles`

### Reference summary columns
- `document_reference_count`
- `document_references`

### Search helper columns
- `search_text`
- `is_live`
- `is_current`
- `sort_issue_date`
- `sort_latest_lodged_date`
- `updated_at`

### Nested columns
Use arrays/structs rather than exploding rows.

- `estates ARRAY<STRUCT<...>>`
- `instruments ARRAY<STRUCT<...>>`
- `encumbrances ARRAY<STRUCT<...>>`
- `prior_titles ARRAY<STRING>`
- `follow_on_titles ARRAY<STRING>`
- `document_references ARRAY<STRUCT<...>>`

This remains a "one big wide table" while still handling one-to-many relationships cleanly.

---

## Source-to-target mapping for `gold.linz_title_search`

| Target column | Source | Notes |
|---|---|---|
| `title_no` | `silver.linz_title.title_no` | primary key |
| `title_status` | `silver.linz_title.status` | e.g. LIVE |
| `title_type` | `silver.linz_title.type` | e.g. FHOL |
| `register_type` | `silver.linz_title.register_type` | e.g. FREE |
| `issue_date` | `silver.linz_title.issue_date` | |
| `guarantee_status` | `silver.linz_title.guarantee_status` | |
| `provisional` | `silver.linz_title.provisional` | |
| `maori_land` | `silver.linz_title.maori_land` | |
| `ttl_title_no_srs` | `silver.linz_title.ttl_title_no_srs` | |
| `ttl_title_no_head_srs` | `silver.linz_title.ttl_title_no_head_srs` | |
| `estate_count` | `silver.linz_title_estate` aggregate by `ttl_title_no` | all estates |
| `current_estate_count` | `silver.linz_title_estate` filtered current statuses | define current statuses in silver |
| `primary_estate_type` | `silver.linz_title_estate` | heuristically first current estate |
| `estate_types` | `silver.linz_title_estate` | collect_set |
| `estate_share_summary` | `silver.linz_title_estate.share` | concat summary |
| `instrument_count` | `silver.linz_title_instrument_title` | count distinct tin_id |
| `latest_instrument_id` | joined instrument aggregate | latest by lodged date / id |
| `latest_instrument_no` | `silver.linz_title_instrument.inst_no` | via link table |
| `latest_instrument_type` | `silver.linz_title_instrument.trt_type` | |
| `latest_instrument_description` | `silver.linz_transaction_type.description` | join on grp/type |
| `latest_lodged_date` | `silver.linz_title_instrument.lodged_datetime` | |
| `encumbrance_count` | `silver.linz_title_encumbrance` | |
| `current_encumbrance_count` | `silver.linz_title_encumbrance` filtered current | |
| `has_current_encumbrance` | derived | |
| `prior_title_count` | `silver.linz_title_hierarchy.ttl_title_no_prior` | |
| `follow_on_title_count` | `silver.linz_title_hierarchy.ttl_title_no_flw` | |
| `document_reference_count` | title -> instruments -> doc refs | aggregated |
| `search_text` | derived | concatenate title_no, title_type, register_type, instrument nos, refs |

---

## Silver modelling rules

### Current-status normalization
Create helper flags such as:
- `is_current`
- `is_live`
- `is_registered`

Because tables use different status codes like:
- `LIVE`
- `REGD`
- `HIST`

### Transaction type enrichment
Join:
- `silver.linz_title_instrument.trt_grp`
- `silver.linz_title_instrument.trt_type`
with:
- `silver.linz_transaction_type.grp`
- `silver.linz_transaction_type.type`

This gives human-friendly instrument descriptions.

### Lineage direction
From `Title Hierarchy`:
- `ttl_title_no_prior`
- `ttl_title_no_flw`

We should present both directions in the app:
- prior titles
- resulting / following titles

### Encumbrance semantics
For MVP, keep it simple:
- count them
- show ids and statuses
- show creating instrument links where present

Do not try to fully render all legal meaning in v1.

---

## Databricks SQL DDL sketch

## Bronze example

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

## Silver title table

```sql
CREATE OR REPLACE TABLE silver.linz_title AS
SELECT
  CAST(title_no AS STRING)                         AS title_no,
  CAST(ldt_loc_id AS BIGINT)                       AS ldt_loc_id,
  CAST(register_t AS STRING)                       AS register_type,
  CAST(ste_id AS BIGINT)                           AS ste_id,
  CAST(issue_date AS DATE)                         AS issue_date,
  CAST(guarantee_ AS STRING)                       AS guarantee_status,
  CAST(status AS STRING)                           AS status,
  CAST(type AS STRING)                             AS type,
  CAST(provisiona AS STRING)                       AS provisional,
  CAST(sur_wrk_id AS BIGINT)                       AS sur_wrk_id,
  NULLIF(CAST(ttl_title_ AS STRING), '')           AS ttl_title_no_srs,
  NULLIF(CAST(ttl_titl_1 AS STRING), '')           AS ttl_title_no_head_srs,
  NULLIF(CAST(maori_land AS STRING), '')           AS maori_land,
  CAST(audit_id AS BIGINT)                         AS audit_id,
  CASE WHEN status IN ('LIVE', 'REGD') THEN true ELSE false END AS is_current
FROM bronze.linz_title_raw;
```

## Gold title serving table

```sql
CREATE OR REPLACE TABLE gold.linz_title_search AS
WITH estate_agg AS (
  SELECT
    ttl_title_no,
    COUNT(*) AS estate_count,
    SUM(CASE WHEN status IN ('REGD', 'LIVE') THEN 1 ELSE 0 END) AS current_estate_count,
    element_at(sort_array(collect_set(CASE WHEN status IN ('REGD', 'LIVE') THEN type END)), 1) AS primary_estate_type,
    sort_array(collect_set(type)) AS estate_types,
    concat_ws('; ', collect_list(NULLIF(share, ''))) AS estate_share_summary,
    concat_ws('; ', filter(collect_list(NULLIF(purpose, '')), x -> x IS NOT NULL)) AS estate_purpose_summary,
    max(CASE WHEN NULLIF(timeshare_week_no, '') IS NOT NULL THEN true ELSE false END) AS has_timeshare_estate,
    max(CASE WHEN NULLIF(term, '') IS NOT NULL THEN true ELSE false END) AS has_term_estate,
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
    max_by(tin_id, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_id,
    max_by(inst_no, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_no,
    max_by(trt_type, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_type,
    max_by(transaction_type_description, coalesce(lodged_datetime, DATE '1900-01-01')) AS latest_instrument_description,
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
    SUM(CASE WHEN te.status IN ('REGD', 'LIVE') THEN 1 ELSE 0 END) AS current_encumbrance_count,
    MAX(CASE WHEN te.status IN ('REGD', 'LIVE') THEN true ELSE false END) AS has_current_encumbrance,
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
hierarchy_agg AS (
  SELECT
    ttl_title_no_prior AS title_no,
    COUNT(*) AS follow_on_title_count,
    sort_array(collect_set(ttl_title_no_flw)) AS follow_on_titles
  FROM silver.linz_title_hierarchy
  WHERE ttl_title_no_prior IS NOT NULL AND ttl_title_no_flw IS NOT NULL
  GROUP BY ttl_title_no_prior
),
reverse_hierarchy_agg AS (
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
  coalesce(t.is_current, false) AS is_current,
  CASE WHEN t.status = 'LIVE' THEN true ELSE false END AS is_live,
  coalesce(e.estate_count, 0) AS estate_count,
  coalesce(e.current_estate_count, 0) AS current_estate_count,
  e.primary_estate_type,
  e.estate_types,
  e.estate_share_summary,
  e.estate_purpose_summary,
  coalesce(e.has_timeshare_estate, false) AS has_timeshare_estate,
  coalesce(e.has_term_estate, false) AS has_term_estate,
  e.estates,
  coalesce(i.instrument_count, 0) AS instrument_count,
  coalesce(i.registered_instrument_count, 0) AS registered_instrument_count,
  i.latest_instrument_id,
  i.latest_instrument_no,
  i.latest_instrument_type,
  i.latest_instrument_description,
  i.latest_lodged_date,
  i.instruments,
  coalesce(enc.encumbrance_count, 0) AS encumbrance_count,
  coalesce(enc.current_encumbrance_count, 0) AS current_encumbrance_count,
  coalesce(enc.has_current_encumbrance, false) AS has_current_encumbrance,
  enc.latest_encumbrance_id,
  enc.encumbrances,
  coalesce(rh.prior_title_count, 0) AS prior_title_count,
  coalesce(h.follow_on_title_count, 0) AS follow_on_title_count,
  coalesce(rh.prior_titles, array()) AS prior_titles,
  coalesce(h.follow_on_titles, array()) AS follow_on_titles,
  CASE
    WHEN coalesce(rh.prior_title_count, 0) > 0 OR coalesce(h.follow_on_title_count, 0) > 0 THEN true
    ELSE false
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
LEFT JOIN hierarchy_agg h ON t.title_no = h.title_no
LEFT JOIN reverse_hierarchy_agg rh ON t.title_no = rh.title_no
LEFT JOIN doc_ref_agg d ON t.title_no = d.ttl_title_no;
```

---

## Frontend plan for current data

## Search page

Search inputs:
- title number
- instrument number
- document reference

Filters:
- current only
- title type
- register type
- has encumbrances
- has title lineage

Result display:
- title number
- LIVE / HIST badge
- title type
- register type
- issue date
- primary estate type
- latest instrument
- encumbrance count

## Detail page

Tabs:
- Summary
- Estates
- Instruments
- Encumbrances
- Lineage
- References

This can feel very clean and easy even without a map.

---

## Future state: parcel-centric gold table

When we add the missing parcel/property layers, we should build:

## `gold.linz_property_search`

**Future grain:** 1 row per parcel-title pair

That table will add:
- parcel geometry
- legal description/appellation
- owner names if available from the fuller feed
- land district / locality labels
- map-centric search columns

At that point the website becomes:
- search bar
- result list
- parcel map
- property detail drawer

For now, keep the title app architecture compatible with that future change.

---

## Weekly acquisition plan

The metadata confirms these layers are updated **weekly**.

We should automate this later with a small ingestion pipeline.

## Recommended pipeline

### Step 1: acquisition
A scheduled job:
- fetch latest LINZ export zip from a stable source URL or authenticated LDS download
- save to object storage with a dated path
  - e.g. `s3://.../linz/raw/2026-04-10/lds-world-10layers-DBF.zip`

### Step 2: unpack
- unzip into a staging area
- record manifest of extracted files
- validate expected 10 tables exist

### Step 3: ingest to bronze
For each DBF:
- read DBF via Python job
- convert to Parquet/Delta
- write into `bronze.*`
- stamp with batch metadata

### Step 4: build silver
- normalize names and types
- trim blanks
- standardize status flags
- dedupe if required

### Step 5: rebuild gold
- run `gold.linz_title_search` transformation
- run data quality checks

### Step 6: publish
- refresh API cache/search cache
- atomically switch app to latest gold snapshot

---

## Recommended orchestrator options

Any of these work:
- Databricks Workflows only
- GitHub Actions + Databricks Jobs
- Airflow / Dagster later if the platform grows

For simplicity, start with:
- **GitHub Actions** on weekly schedule
- trigger **Databricks Job** with the new zip location

---

## Data quality checks

At each weekly run, validate:

- all 10 expected layers present
- row counts non-zero
- `title_no` uniqueness in `silver.linz_title`
- no sudden row-count drops beyond threshold
- title-instrument link counts within normal band
- gold row count roughly equals title count

Track in an audit table:
- `ops.linz_ingest_runs`
- `ops.linz_table_counts`

---

## Recommended next implementation steps

1. create the Bronze/Silver/Gold schemas in Databricks
2. upload the zip to cloud storage reachable by Databricks
3. write a Python ingest notebook/job for DBF -> Delta
4. materialize the 10 silver tables with normalized column names
5. build `gold.linz_title_search`
6. stand up a tiny API for:
   - `/api/search`
   - `/api/title/:titleNo`
7. build the first very simple search UI
8. in parallel, identify the additional LINZ weekly layers needed for parcel/property/map support

---

## Key decision

### Right now
Use a **wide title-centric table**.

### Later
Upgrade to a **wide parcel-title table** once parcel/property layers are added.

This is the safest path because it matches the data we actually have today while keeping the final product direction intact.
