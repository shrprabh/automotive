# Automotive Manufacturing Analytics — Medallion Architecture

This project implements a **Bronze / Silver / Gold medallion pipeline** on Databricks (Unity Catalog + Delta Lake) for an automotive manufacturing business. Raw CSV files from the business (vehicle production, quality inspection, supplier delivery) land in UC volumes, get ingested into Bronze Delta tables, are incrementally cleaned and deduplicated into Silver tables using a watermark-based control table, and roll up into a Gold SCD2 KPI table.

## 1. Project Structure

```
automotive/
├── Readme.md                              # This file
├── Automotive                             # Setup notebook: catalog, schema, volumes, landing/archive folders
├── Control                                # Control table: watermark metadata for silver/gold loads
├── Delta                                  # DDL for all 7 Delta tables (bronze, silver, gold)
├── Bronze/
│   ├── bronze_vehicle_ingestion           # Landing CSV → bronze_vehicle_production
│   ├── bronze_supplier_ingestion          # Landing CSV → bronze_supplier_delivery
│   └── bronze_quality_ingestion           # Landing CSV → bronze_quality_inspection
├── Silver/
    ├── silver_vehicle_processing          # bronze → silver_vehicle_production (incremental + dedup)
    ├── silver_supplier_processing          # bronze → silver_supplier_delivery (incremental + dedup)
    └── silver_quality_processing           # bronze → silver_quality_inspection (incremental + dedup)
└── Gold/
    └── gold_vehicle_kpi_scd_merge          # silver tables → gold_vehicle_manufacturing_kpi (SCD2 KPI build)
```

## 2. Unity Catalog Objects

* **Catalog:** `automotive_catalog`
* **Schema:** `automotive_catalog.automotive_mfg`
* **Volumes:**
  * `landing` — business drops raw CSV files here
    * `landing/vehicle_production`
    * `landing/quality_inspection`
    * `landing/supplier_delivery`
  * `archive` — files moved here after a successful bronze load (exactly-once ingestion per file)
    * `archive/vehicle_production`
    * `archive/quality_inspection`
    * `archive/supplier_delivery`

## 3. Delta Tables

### Control table

| Table | Purpose |
| --- | --- |
| `control_metadata` | Tracks the high-watermark (`last_processed_ts`) per consuming table, so silver/gold runs only process new bronze rows. Seeded with rows for `silver_vehicle_production`, `silver_quality_inspection`, `silver_supplier_delivery`, and `gold_vehicle_manufacturing_kpi`. |

### Bronze layer (raw, append-only)

| Table | Source | Key columns |
| --- | --- | --- |
| `bronze_vehicle_production` | `landing/vehicle_production/*.csv` | vin_number, plant_code, assembly_line, model_name, production_stage, production_status, production_date, shift + audit columns |
| `bronze_quality_inspection` | `landing/quality_inspection/*.csv` | inspection_id, vin_number, inspection_station, defect_category, defect_severity, rework_required, inspection_date + audit columns |
| `bronze_supplier_delivery` | `landing/supplier_delivery/*.csv` | delivery_id, supplier_id, part_number, part_description, plant_code, planned_qty, delivered_qty, delivery_status, delivery_date + audit columns |

Every bronze table carries standard audit columns: `op_code`, `ingest_ts`, `source_file_name`, `batch_run_id`, `load_date`.

### Silver layer (cleaned, deduplicated, incremental)

| Table | Source | Dedup key | Cleaning applied |
| --- | --- | --- | --- |
| `silver_vehicle_production` | `bronze_vehicle_production` | `vin_number` (latest `ingest_ts` wins) | Null checks on vin/plant; upper+trim on status, stage, model |
| `silver_quality_inspection` | `bronze_quality_inspection` | `inspection_id` (latest `ingest_ts` wins) | Null checks on inspection/vin; upper+trim on category, severity, rework |
| `silver_supplier_delivery` | `bronze_supplier_delivery` | `delivery_id` (latest `ingest_ts` wins) | Null checks on delivery/supplier; upper+trim on status, description |

Silver tables drop the file/load audit columns and add `processed_ts` + `batch_run_id` (UUID) instead.

### Gold layer

| Table | Purpose |
| --- | --- |
| `gold_vehicle_manufacturing_kpi` | SCD2 KPI table per VIN joining vehicle production with quality and supplier data: total defects, high-severity defects, rework flag, supplier delay flag, planned/delivered parts qty, delivery fill rate, quality risk level. Uses a generated `surrogate_key`, `is_current`, `effective_from`/`effective_to` for SCD2 history. |

## 4. How It Works

### Bronze ingestion (each notebook in `Bronze/`)

1. Lists `.csv` files in the landing volume for its domain.
2. Reads them with an explicit schema, adding `ingest_ts`, `source_file_name`, `batch_run_id` (batch timestamp), and `load_date`.
3. Appends rows to the bronze Delta table.
4. Only after a successful write, moves each file to the archive volume — this makes ingestion **exactly-once per file**.
5. If no files are found, the notebook exits without writing anything.

### Silver processing (each notebook in `Silver/`)

1. Reads its **own** watermark from `control_metadata` (defaults to `1900-01-01` on first run).
2. Fetches only bronze rows with `ingest_ts` > watermark (incremental fetch).
3. Cleans (null filters, upper/trim) and deduplicates by business key, keeping the latest row.
4. Appends to the silver table, then advances its own watermark to the max `ingest_ts` consumed — **only after a successful write**.

Each silver table owns exactly one `control_metadata` row, so layers never fight over the same watermark. Bronze never writes to the control table.

## 5. Execution Order

1. **`Automotive`** — creates the catalog, schema, `landing`/`archive` volumes, and per-domain subdirectories (run once).
2. **`Control`** — creates and seeds the `control_metadata` watermark table (run once).
3. **`Delta`** — creates all 7 Delta tables (bronze, silver, gold) if they don't exist (run once).
4. Drop the business CSV files into the landing volumes.
5. **`Bronze/*`** — run the three bronze ingestion notebooks to load and archive files.
6. **`Silver/*`** — run the three silver processing notebooks to clean and deduplicate incrementally.
7. **`Gold/gold_vehicle_kpi_scd_merge`** — run the gold KPI SCD2 build (joins the silver tables into the SCD2 KPI table).

Steps 5–7 are automated every 12 hours by the jobs in section 7.

## 6. Data Flow

```
Business CSVs
      │
      ▼
UC Volumes (landing)  ──archive after load──▶  UC Volumes (archive)
      │
      ▼
Bronze Delta tables (raw, append-only, full audit trail)
      │  (incremental fetch via control_metadata watermark)
      ▼
Silver Delta tables (cleaned, deduplicated, batch-tagged)
      │
      ▼
Gold: gold_vehicle_manufacturing_kpi (SCD2 KPI)
```

## 7. Jobs & Orchestration

The pipeline is automated with Lakeflow Jobs on **serverless compute** (no cluster configuration, max 1 concurrent run, queueing enabled, no per-task timeout).

### Individual layer jobs (7 jobs — active, every 12 hours)

Each job wraps exactly one notebook task and fires on a 12-hour periodic trigger:

| Layer | Job | Job ID | Notebook task |
| --- | --- | --- | --- |
| Bronze | `bronze_vehicle_ingestion` | 861806828459292 | `Bronze/bronze_vehicle_ingestion` |
| Bronze | `bronze_supplier_ingestion` | 1090427536491910 | `Bronze/bronze_supplier_ingestion` |
| Bronze | `bronze_quality_ingestion` | 795061177036678 | `Bronze/bronze_quality_ingestion` |
| Silver | `silver_vehicle_processing` | 950654803551448 | `Silver/silver_vehicle_processing` |
| Silver | `silver_quality_processing` | 759545564776052 | `Silver/silver_quality_processing` |
| Silver | `silver_supplier_processing` | 755678534420776 | `Silver/silver_supplier_processing` |
| Gold | `gold_vehicle_kpi_scd_merge` | 585303488539777 | `Gold/gold_vehicle_kpi_scd_merge` |

### How a scheduled cycle runs

1. Business CSVs are dropped into the `landing` volume subdirectories.
2. Every 12 hours each job fires independently — the bronze notebooks ingest and archive landing files, the silver notebooks process rows past their watermark, and the gold notebook merges the SCD2 KPI table.
3. Because the seven jobs run on separate timers, bronze and silver can fire at the same moment; silver simply picks up whatever bronze rows are already past its watermark and catches the rest on the next cycle.

### Consolidated pipeline job: `automotive_medallion_pipeline` (ID 929104859434980)

One job chains the whole medallion pipeline in a single run — it reuses the same seven notebooks (no new notebooks were created), and the seven individual jobs above were left untouched.

```
bronze_vehicle_ingestion   bronze_supplier_ingestion   bronze_quality_ingestion
        └───────────────┬──────────────┴───────────────┘
                        ▼
silver_vehicle_processing   silver_quality_processing   silver_supplier_processing
        └───────────────┬──────────────┴───────────────┘
                        ▼
                gold_vehicle_kpi_scd_merge
```

* **Task DAG:** the 3 bronze tasks start together in parallel; every silver task waits until all bronze tasks succeed; gold waits until all silver tasks succeed. A failure in any bronze task skips silver and gold for that run.
* **Schedule:** 12-hour periodic trigger, **paused** — the job was created but never run, and will not fire until un-paused.
* **Benefit:** within a single run, silver always processes the bronze rows written in that same run, so there is no cross-cycle watermark lag between layers.

### How it was created / how to switch over

1. Created as a single job with 7 notebook tasks pointing at the existing notebooks; its trigger was paused at creation so it would not double-process alongside the active individual jobs.
2. To try it once: open `automotive_medallion_pipeline` and click **Run now** — the individual jobs are unaffected.
3. To switch over permanently: **un-pause** `automotive_medallion_pipeline` and **pause** the 7 individual jobs, so the pipeline is not processed twice per cycle.
4. The one-off setup notebooks (`Automotive`, `Control`, `Delta`) are not part of any job — they were run manually once during initial setup.

