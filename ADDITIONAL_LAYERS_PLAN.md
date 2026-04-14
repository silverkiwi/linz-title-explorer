# Additional 7-layer + 14-layer integration

This project now supports loading all tables from:

- `lds-world-7layers-SHP`
- `lds-world-14layers-SHP`

using the updated one-table ingest script:

- `scripts/linz_ingest_one_table_job.py`

## LDS dictionary fit (from `lds-expanded-final.md`)

7-layer tables map directly to section 4 entities:

- `landonline-appellation` -> **4.10 Appellation**
- `landonline-legal-description` -> **4.26 Legal Description**
- `landonline-legal-description-parcel` -> **4.27 Legal Description Parcel**
- `landonline-statute` -> **4.64 Statute**
- `landonline-statute-action` -> **4.65 Statute Action**
- `landonline-statutory-action-parcel` -> **4.66 Statutory Action Parcel**
- `landonline-title-parcel-association` -> **4.83 Title Parcel Association**

14-layer AIMS tables are external/deprecated address-domain tables and are integrated under `main.silver.aims_*` and `main.gold.aims_address_search_deprecated`.

## SQL build

Run after loading additional bronze tables:

- `sql/08_build_additional_7_14_layers_safe_main.sql`

This creates:

- New `main.silver.linz_*` tables for all 7-layer additions
- New `main.silver.aims_*` tables for all 14-layer additions
- `main.gold.linz_title_search_enriched`
- `main.gold.aims_address_search_deprecated`

## Ingestion notes

For split DBF files (`.dbf`, `.2.dbf`, `.3.dbf`):

- The script now loads **all matching shards** using pattern `source_table*.dbf`
- Writes all shards into one bronze target table
- Silver uses `SELECT DISTINCT` for de-dup safety
