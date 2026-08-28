# Zomato AI Data Engineering, End-to-End Project

A complete batch data pipeline that takes Zomato-style food delivery data from raw CSVs all the way to AI-powered analytics:

**Zomato/Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (OpenAI)**

The dataset lands in an S3 data lake and flows into Snowflake through a storage integration, where dbt transforms it through medallion layers — RAW (Bronze) tables loaded via COPY INTO, cleaned STAGING (Silver) views, and business-ready MARTS (Gold) with dimensions, incremental facts, and aggregate marts. Apache Airflow orchestrates the whole pipeline as one daily DAG. On top of the warehouse sits an AI lane powered by OpenAI: LLM enrichment turns free-text reviews into structured, queryable columns; RAG lets you chat with your reviews; and text-to-SQL lets you query the warehouse in plain English. Streamlit serves the dashboards and AI apps.

## Architecture

<img width="1672" height="941" alt="architecture_ai_data_engineerinng-project" src="https://github.com/user-attachments/assets/0d34e6ed-18ae-4be9-8c06-862574727cbd" />

## 📂**Dataset:** 
[Google Drive folder](https://drive.google.com/drive/folders/1vCXTLNUELPkUyQbvboMx7gq7osTzevjs?usp=sharing) — the zip files with data.zip and data2.zip have all the 10+ million rows data (they're too large to commit to the repo).

---

## What Gets Built

| Layer | Where | What |
|-------|-------|------|
| Source | `data/` (local) | 4 real dimension CSVs (restaurants, users, food, menu) + 3 generated fact files: 10M orders, ~23M order items, 300K free-text reviews |
| Lake | Amazon S3 | One bucket, `raw/<table>/` folder per CSV |
| Bronze | Snowflake `ZOMATO.RAW` | COPY INTO from S3 via a keyless storage integration |
| Silver | Snowflake `ZOMATO.STAGING` | dbt staging views — clean, type, rename every source |
| Gold | Snowflake `ZOMATO.MARTS` | Dimensions, incremental facts (MERGE), business marts + an SCD2 snapshot |
| AI | Snowflake `ZOMATO.AI` | LLM-enriched reviews (sentiment/topic), RAG chat, text-to-SQL |
| Orchestration | Airflow (Docker) | One daily DAG: load → transform → enrich → AI mart |

## Tech Stack

Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · OpenAI (gpt-4o-mini, text-embedding-3-small) · Streamlit

---

## Repository Structure

```text

├── airflow/                  # Airflow 3 on Docker
│   ├── Dockerfile            #   Snowflake + OpenAI providers, dbt in its own venv
│   ├── docker-compose.yaml   #   postgres + api-server + scheduler; creds via env vars
│   ├── example.env           #   template for SNOWFLAKE_* / OPENAI_API_KEY
│   └── dags/zomato_batch.py  #   the pipeline DAG (4 tasks)
├── zomato/                   # dbt project
│   ├── models/staging/       #   7 staging views (Silver) + sources + tests
│   ├── models/marts/         #   dims, incremental facts, business marts (Gold)
│   └── macros/               #   custom schema-name macro
├── ai/                       # AI layer
│   ├── enrich_reviews.py     #   LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py           #   RAG — "chat with your reviews" (Streamlit)
│   ├── text_to_sql.py        #   text-to-SQL — "chat with your warehouse" (Streamlit)
│   └── example.env           #   template for the AI credentials
├── snowflake/                # Snowflake setup SQL (run in Snowsight, in order)
│   ├── 01_setup.sql          #   warehouse ZOMATO_WH, database ZOMATO, schemas, role
│   ├── 02_storage_integration.sql  # keyless S3 link (pairs with aws/iam/)
│   ├── 03_stage_and_formats.sql    # external stage + CSV file format
│   ├── 04_raw_tables.sql     #   RAW (Bronze) table DDL, column order matches the CSVs
│   └── 05_copy_into.sql      #   COPY INTO RAW from the stage
├── aws/iam/                  # IAM policy + role trust policies for the S3 ↔ Snowflake handshake
```

> `data/` (~2.3 GB of CSVs), `logs/`, and dbt `target/` artifacts are intentionally not committed

---

## How the Pipeline Works

### 1 · Data Lands in S3

The seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/` — one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2 · S3 → Snowflake: One Keyless Handshake

Snowflake reads the bucket with no stored keys, using a storage integration + an IAM role. The Snowflake side is `snowflake/02_storage_integration.sql`; the AWS JSON documents live in `aws/iam/`:

| File | Used for |
|------|----------|
| `s3-read-policy.json` | IAM policy `zomato-s3-read` — read-only access to the bucket |
| `snowflake-role-trust-policy-initial.json` | IAM role `snowflake-s3-role` — placeholder trust used at creation time |
| `snowflake-role-trust-policy-final.json` | Final trust — Snowflake's IAM user ARN + external ID from `DESC INTEGRATION` |

The order matters: create the AWS policy + role → create the Snowflake STORAGE INTEGRATION pointing at the role ARN → `DESC INTEGRATION` to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` → paste both into the role's trust policy.

### 3 · Load — COPY INTO

Table DDL (`snowflake/04_raw_tables.sql`) matches each CSV's column order, then `snowflake/05_copy_into.sql` pulls each file from the stage into `ZOMATO.RAW` tables: 10M orders, ~23M order items, 300K reviews.

### 4 · Transform — dbt (Medallion)

- **Staging (Silver)** — one view per source: parse the messy restaurant dimension (`--` → null, `₹ 200` → `200`), lowercase emails, derive `is_delivered`, etc.
- **Dimensions (Gold)** — `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, a generated `dim_date` calendar.
- **Facts (Gold, incremental)** — `fct_orders` and `fact_order_items` use `materialized='incremental'` with a MERGE strategy, so a re-run processes only new rows instead of rebuilding 10M+.
- **Marts (Gold)** — one table per business question: daily city revenue (GMV/AOV/cancel rate), restaurant performance, delivery SLA (p50/p90 by city & hour), review insights.
- **Tests** — unique / not_null / relationships / accepted_values plus a singular reconciliation test; `dbt build` runs models and tests in dependency order.

### 5 · Orchestrate — Airflow

One daily DAG, `zomato_batch`, runs the whole thing as a single graph:
```text
reload_raw → dbt_build_core → enrich_reviews → dbt_build_ai
(COPY from S3) (dbt build + tests) (OpenAI enrichment) (AI mart)
```


Credentials never touch the code: docker-compose injects `SNOWFLAKE_*` env vars (read by dbt's `profiles.yml` via `env_var()`) and an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the COPY task.

### 6 · AI Layer — Three Capabilities

- **LLM enrichment** (`ai/enrich_reviews.py`) — LLM as a transformation step. Reads review text, asks gpt-4o-mini for structured JSON (sentiment + topic), writes it back to `ZOMATO.AI.REVIEW_ENRICHED` — which dbt then models into `mart_review_insights` like any other table. Idempotent and sample-capped (`SAMPLE_N`) so you never pay twice for the same review.
- **RAG** (`ai/rag_chat.py`) — chat with your reviews. Embeds reviews, retrieves the most similar ones for a question, and generates an answer grounded in real reviews (with sources).
- **Text-to-SQL** (`ai/text_to_sql.py`) — chat with your warehouse. The LLM gets the marts' schema, writes Snowflake SQL for an English question, and a SELECT-only guard validates it before running as `DBT_ROLE`.

<img width="583" height="866" alt="Screenshot 2026-08-26 at 5 39 10 PM" src="https://github.com/user-attachments/assets/fff51815-09ee-412d-869b-a2a2222aeeef" />

---

## Running It

```bash
# Snowflake objects (warehouse ZOMATO_WH, database ZOMATO, schemas RAW/STAGING/MARTS/SNAPSHOTS/AI, role DBT_ROLE)
# + the S3 storage integration: run snowflake/01→05 in Snowsight — see aws/iam/ for the AWS side.

# dbt
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# Airflow
cd airflow
cp example.env .env          # fill SNOWFLAKE_* , OPENAI_API_KEY, SAMPLE_N
docker compose build && docker compose up -d
# http://localhost:8080 → un-pause zomato_batch → Trigger

# AI apps
export OPENAI_API_KEY=sk-...
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```
