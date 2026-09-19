# Decision Log

*One dated line per decision. If it isn't here, it wasn't decided. Parked items get a reconsider date.*

## Decisions

- **2026-09-13** — Dataset confirmed: MMA fight data, round-level grain (`fact_round`). Same layered architecture as originally specced; Market Stress project superseded as the active build.
- **2026-09-13** — Monetization fork (betting / selling picks / selling model access) **parked** until Phase 2 model exists with holdout numbers. Reconsider: at Phase 2 completion. [Claude's standing recommendation: start a public timestamped paper prediction log vs. closing odds the day the model runs; decide money questions against that data.]
- **2026-09-14** — Data validation: **GO.** Kaggle bulk (per-round stats + odds datasets, free) as v1 source; UFCStats scrape as v2 incremental source. Residual: Emil's sample pull + robots/ToS eyeball.
- **2026-09-18** — DP-700 exam **pinned**. Project decoupled from the exam; consultancy outreach launched to measure real-world DP-600/700 emphasis (tag responses consultancy vs. in-house — consultancies structurally over-weight certs via partner tiers). Exam-area mapping retained in project definition at zero cost.
- **2026-09-18** — Project moves to its own repo + Claude Code project. v1 CI/CD = GitHub + Fabric native Git sync only; deployment pipelines/multi-env = v2.
- **2026-09-18** — Gate dates carried as proposals pending Emil's confirmation: Phase 1 end-to-end 3 Oct (cut scope, don't extend); Phase 2 published 2 Nov.

## Parked (with reconsider dates)

- Betting / bankroll / picks / model-access sales — **reconsider at Phase 2 completion** (see 2026-09-13 entry).
- DP-700 retake booking — **reconsider when consultancy outreach returns** (target: ~2 weeks).
- Eventstream / real-time layer — reconsider only if v1 ships before gate.
- Full CI/CD buildout — reconsider at v2.
- Warrior Channel — prior reconsider date 20 Nov stands, subordinate to this project.
