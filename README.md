# Combat Sports Data Platform

An end-to-end **fight-outcome forecasting platform** built in Microsoft Fabric. Raw fight data goes in one end; a Power BI report with descriptive trends and a calibrated prediction model comes out the other. UFC/MMA is the first data set; the platform is built so the source can be swapped without touching anything downstream.

```
Kaggle file ──► bronze ──► silver ──► gold (star schema) ──► baseline model ──► Power BI
               raw, as-is  cleaned to a   fact_round +          logistic          2 pages
                           source-free    4 dimensions          regression
                           contract
```

## Where it stands

| Phase | What | Status |
|---|---|---|
| **1** | Platform on Kaggle data → Power BI + baseline model | **Built** (gate Sat 17 Oct 2026) |
| 2 | Second source loaded incrementally into the same silver contract — same gold, same report, no downstream edits | Gate Sun 8 Nov |
| 3 | Real model: rating system + logistic regression, scored on calibration, benchmarked against closing odds | Gate Sun 29 Nov |

## Phase 1 in numbers

- **8,555 fights**, 20,145 rounds → `fact_round` at **fighter × round** grain (40,290 rows, 0 orphan keys).
- **Baseline model** — logistic regression, no tuning, on record, age, age gap, layoff, weight class, reach/height differential. Trained on fights before 2023, tested on 1,735 fights from 2023-01-01 to 2026-05-16:

| Metric (test) | Baseline | Coin flip |
|---|---|---|
| Accuracy | **0.598** | 0.500 |
| Log loss | **0.658** | 0.693 |
| Brier | **0.233** | 0.250 |

  This is deliberately simple. It is **the floor the Phase 3 model has to beat**. Calibration is close: bins holding 96 % of test fights land within 1–4 pp of the diagonal.

## The report

- **Trends** — win rate by age band × weight class; finish rate (KO/TKO + submission) by weight class.
- **Baseline model** — test metrics vs. naive, calibration plot, and the model's most confident wrong predictions.

## Design choices worth knowing

- **Medallion layers.** `bronze_` is raw and append-only (a load-ID guard makes re-ingest a no-op). `silv_` is cleaned to the [silver contract](docs/silver-contract.md). Gold is the star schema.
- **Source-swap contract.** Silver is source-agnostic: source-specific column names live only in that source's `20_clean_*` notebook, and nothing downstream of silver reads bronze. Phase 2 tests this for real.
- **Entity resolution.** Fighter identity resolved on UFCStats `fighter_id` (17,110 corner rows → 2,686 fighters); fuzzy name matching arrives with the Phase 2 source if it lacks those IDs.
- **Star schema.** `fact_round` with `dim_fight`, `dim_fighter`, `dim_event`, `dim_date`. `dim_fighter` is SCD Type 1 for now — time-varying attributes (weight class, age at fight) already live at fight grain ([why](docs/decisions.md)).
- **No leakage in the model.** Fighter A vs B is assigned by a stable hash of the fight key, not by corner (corner 1 wins 64 %), and the split is by date.
- **Failures are logged, not hidden.** Bugs found along the way — and how they were caught — are in [`decisions.md`](docs/decisions.md).

## Repo layout

```
├── Fabric/                 # Fabric items, synced via Git integration
│   ├── 10_ingest_kaggle    #   bronze
│   ├── 20_clean_kaggle     #   silver (Kaggle column names live only here)
│   ├── 30_warehouse_dims   #   gold dimensions
│   ├── 31_warehouse_fact_round
│   ├── 40_model_baseline   #   baseline model → predictions, metrics, calibration in gold
│   ├── sm_combat_sports.SemanticModel
│   └── DB_Fight_Stats_by_Weightclass.Report
└── docs/
    ├── project-definition.md   # scope, phases, gates (source of truth)
    ├── build-plan.md           # what gets built which week
    ├── silver-contract.md      # source-agnostic silver schema
    ├── data-coverage.md        # what the Kaggle data covers
    └── decisions.md            # dated decision log
```

## Run it

In the Fabric workspace, run the notebooks in order: `10 → 20 → 30 → 31 → 40`, then refresh the report. Kaggle files land under `Files/kaggle_file_load/<load_id>/` in the bronze lakehouse; `data/` is gitignored because the Kaggle pull is reproducible.

---

*Framing: this is a forecasting-methodology project — calibration and honest evaluation, not a betting system.*
