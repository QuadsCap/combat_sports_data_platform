# Combat Sports Data Platform

End-to-end forecasting pipeline in Microsoft Fabric: ingestion → PySpark cleaning & entity resolution → dimensional warehouse (`fact_round` grain, SCD2) → calibrated fight-outcome model (Elo + logistic regression, Brier/log-loss vs. market benchmark) → Power BI.

## What "end-to-end" means here

The pipeline is not finished when the data is clean. **Clean data is the middle of the project, not the end.** Every phase ends with something a human can look at:

- **Phase 1** ends at a **Power BI report** built on the gold tables, carrying descriptive trends *and* a deliberately simple baseline model (win/loss record, age and age gap, layoff, weight class). Simple on purpose: it is the floor the Phase 2 model has to beat.
- **Phase 2** ends at the **real model** — Elo ratings feeding a logistic regression, scored on calibration and log loss, benchmarked against closing odds.

Power BI is the least-practised layer in this stack, so Phase 1 budgets learning time for it rather than assuming the report is an afterthought.

## Repo layout

```
├── CLAUDE.md              # Instructions for Claude Code in this project
├── docs/
│   ├── project-definition.md   # Source of truth: scope, phases, gates
│   ├── build-plan.md           # Sprint plan: what gets built which week
│   └── decisions.md            # Dated decision log
├── notebooks/             # Fabric notebooks, synced via Git integration
│   ├── 10_ingest_*        # Bronze
│   ├── 20_clean_*         # Silver
│   ├── 30_warehouse_*     # Gold
│   └── 40_model_*         # Baseline (Phase 1) and full model (Phase 2)
├── src/                   # Shared Python (entity resolution helpers, etc.)
└── data/                  # Local samples only — gitignored
```

## Setup checklist (Phase 0)

- [x] Create GitHub repo from this skeleton; `project-definition.md` + `decisions.md` in `docs/`
- [x] Open in Claude Code (CLAUDE.md auto-loads)
- [ ] Connect Fabric workspace ↔ repo (Workspace settings → Git integration), and understand the sync flow both directions
- [ ] Kaggle sample pull: verify per-round dataset coverage/recency → write the coverage note
- [ ] Eyeball ufcstats.com robots.txt/ToS (pre-scraping check, needed for v2 only)
- [ ] Salvage reusable structure from the Market Stress repo (point Claude Code at it)
- [ ] Confirm gate dates in `docs/project-definition.md`

Week-by-week execution lives in [`docs/build-plan.md`](docs/build-plan.md).
