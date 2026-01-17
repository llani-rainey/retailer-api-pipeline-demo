# GAIA – Iceland (Phase 2) Product Enrichment (Public Sample)

This repo contains a **public-safe sample** of one part of my GAIA grocery data pipeline: an **Iceland** product enrichment script that reads Phase 1 product IDs + category metadata, fetches product JSON from Iceland’s public endpoint, and outputs a **normalized CSV**.

It is intentionally scoped to:
- **One retailer only (Iceland)** and **one phase** of the pipeline (Phase 2 enrichment).
- **A fixed test set of product IDs** (no full-catalog scraping mode enabled here).
- **No API keys, no private endpoints, and no user-specific absolute paths**.

## What this demonstrates
- batching + concurrency (ThreadPoolExecutor)
- retry/degradation logic for unstable requests
- normalization + defensive parsing (messy real-world data)
- consistent CSV schema + final de-dupe/sort pass
- real-world parsing logic:
  - packet quantity detection (bakery + countables)
  - weight/unit parsing (g/ml + cl support)
  - inferring pack size from per-100 unit pricing
  - nutrition parsing + kJ/kcal handling
 
## Roadmap (planned)
This repo shows a public-safe slice of a larger grocery comparison + nutrition pipeline. Next steps:

- Add Phase 1 “catalog discovery” for Iceland as a separate module (currently this repo expects a small Phase 1 CSV input).
- Extend enrichment adapters to additional retailers behind a common interface (shared output schema, per-retailer parsing plugins).
- Add canonical product matching (normalize brands/units, map retailer items to a master product list).
- Persist to a database (PostgreSQL) and build an API layer for querying price/macro metrics efficiently.
- Build basket comparison + meal planning features (weekly basket cost + nutrition totals, daily/weekly calorie calculator (BMR etc)
- Add price history tracking and highlight best buys

## Repo structure (suggested)
```text
.
├── scripts/
│   └── iceland_phase2_public.py
├── data/
│   ├── input/
│   │   └── p1_iceland.csv
│   └── output/
└── logs/
```

## Setup

### 1) Create a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2) Install requirements
Install dependencies from `requirements.txt`:
```bash
pip install -r requirements.txt
```

## Input (Phase 1 CSV)
The Phase 2 script expects an input CSV with at least these columns:

- `product_code`
- `product_name`
- `product_url`
- `category_path`
- `level_1`, `level_2`, `level_3`, `level_4`, `level_5`, `level_6`

Create:
- `data/input/p1_iceland.csv`

Minimum header example:
```csv
product_code,product_name,product_url,category_path,level_1,level_2,level_3,level_4,level_5,level_6
```

Populate it with a subset of product codes that overlap the script’s `TEST_PRODUCT_IDS`.

## Run
From the repo root:
```bash
python3 scripts/iceland_phase2_public.py   --input data/input/p1_iceland.csv   --outdir data/output
```

Notes:
- The script is intentionally locked to a fixed test list (`TEST_PRODUCT_IDS`) and will filter the input CSV to those IDs.
- If none of the test IDs exist in your input file, it will exit.

## Output
When you run the script, it writes:
- `data/output/p2_iceland_test_YYYYMMDD_HHMM.csv` (normalized output)
- `logs/iceland_phase2_test_YYYYMMDD_HHMM.log` (run log)
- `logs/iceland_enrich_errors.csv` (optional; only if errors occur)

## Public-safety / scope notes
This is intentionally not the full GAIA system. It’s a portfolio-safe slice that demonstrates engineering and parsing quality while keeping broader business logic private:
- one retailer
- one phase
- a test ID subset
- no API keys or private endpoints

## Responsible use
This code is provided for demonstration/portfolio purposes. If you adapt it:
- respect retailer terms and robots policies
- rate-limit requests
- avoid unnecessary load
