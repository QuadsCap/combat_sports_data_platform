# MMA Fight-Data Platform

End-to-end forecasting pipeline in Microsoft Fabric: ingestion → PySpark cleaning & entity resolution → dimensional warehouse (`fact_round` grain, SCD2) → calibrated fight-outcome model (Elo + logistic regression, Brier/log-loss vs. market benchmark) → Power BI.

## Repo layout

```
├── CLAUDE.md              # Instructions for Claude Code in this project
├── docs/
│   ├── project-definition.md   # Source of truth: scope, phases, gates
│   └── decisions.md            # Dated decision log
├── notebooks/             # Fabric notebooks, synced via Git integration
│   ├── 10_ingest_*        # Bronze
│   ├── 20_clean_*         # Silver
│   ├── 30_warehouse_*     # Gold
│   └── 40_model_*         # Phase 2
├── src/                   # Shared Python (entity resolution helpers, etc.)
└── data/                  # Local samples only — gitignored
```

## Setup checklist (Phase 0)

- [ ] Create GitHub repo from this skeleton; move `project-definition.md` + `decisions.md` into `docs/`
- [ ] Open in Claude Code (CLAUDE.md auto-loads)
- [ ] Connect Fabric workspace ↔ repo (Workspace settings → Git integration)
- [ ] Kaggle sample pull: verify per-round dataset coverage/recency
- [ ] Eyeball ufcstats.com robots.txt/ToS (pre-scraping check, needed for v2 only)
- [ ] Salvage reusable structure from the Market Stress repo (point Claude Code at it)
- [ ] Confirm gate dates in `docs/project-definition.md`
```
