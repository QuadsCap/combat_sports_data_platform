# Combat Sports Data Platform

End-to-end forecasting pipeline in Microsoft Fabric: ingestion (Kaggle bulk, then UFCStats scraped incrementally) → PySpark cleaning & entity resolution → dimensional warehouse (`fact_round` grain, SCD2) → calibrated fight-outcome model (Elo + logistic regression, Brier/log-loss vs. market benchmark) → Power BI.

## What "end-to-end" means here

The pipeline is not finished when the data is clean. **Clean data is the middle of the project, not the end.** Every phase ends with something a human can look at:

- **Phase 1** ends at a **Power BI report** built on the gold tables (Kaggle data), carrying descriptive trends *and* a deliberately simple baseline model (win/loss record, age and age gap, layoff, weight class). Simple on purpose: it is the floor the model has to beat.
- **Phase 2** ends with the **source switched**: UFCStats scraped and loaded incrementally into the same silver contract, the same gold tables and the same report, with no changes downstream of silver.
- **Phase 3** ends at the **real model**: Elo ratings feeding a logistic regression, scored on calibration and log loss, benchmarked against closing odds, published.

Power BI is the least-practised layer in this stack, so Phase 1 budgets learning time for it rather than assuming the report is an afterthought.

## Repo layout

```
├── CLAUDE.md              # Instructions for Claude Code in this project
├── docs/
│   ├── project-definition.md   # Source of truth: scope, phases, gates
│   ├── build-plan.md           # Sprint plan: what gets built which week
│   ├── silver-contract.md      # Source-agnostic silver schema (Sprint 2)
│   └── decisions.md            # Dated decision log
├── notebooks/             # Fabric notebooks, synced via Git integration
│   ├── 10_ingest_*        # Bronze, one per source (kaggle, ufcstats)
│   ├── 20_clean_*         # Silver, one per source, all to the silver contract
│   ├── 30_warehouse_*     # Gold
│   └── 40_model_*         # Baseline (Phase 1) and full model (Phase 3)
├── src/                   # Shared Python (entity resolution helpers, etc.)
└── data/                  # Local samples only — gitignored
```

## Setup checklist (Phase 0)

- [x] Create GitHub repo from this skeleton; `project-definition.md` + `decisions.md` in `docs/`
- [x] Open in Claude Code (CLAUDE.md auto-loads)
- [x] Connect Fabric workspace ↔ repo (Workspace settings → Git integration), and understand the sync flow both directions
- [ ] Kaggle sample pull: verify per-round dataset coverage/recency → write the coverage note
- [ ] Eyeball ufcstats.com robots.txt/ToS (Sprint 1, blocking for Phase 2)
- [ ] Salvage reusable structure from the Market Stress repo (point Claude Code at it)
- [x] Confirm gate dates: Phase 1 Sat 17 Oct · Phase 2 Sun 8 Nov · Phase 3 Sun 29 Nov

Week-by-week execution lives in [`docs/build-plan.md`](docs/build-plan.md).
