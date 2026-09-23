# Decision Log

*One dated line per decision. If it isn't here, it wasn't decided. Parked items get a reconsider date.*

## Decisions

- **2026-09-13** — Dataset confirmed: MMA fight data, round-level grain (`fact_round`). Same layered architecture as originally specced; Market Stress project superseded as the active build.
- **2026-09-13** — Monetization fork (betting / selling picks / selling model access) **parked** until Phase 2 model exists with holdout numbers. Reconsider: at Phase 2 completion. [Claude's standing recommendation: start a public timestamped paper prediction log vs. closing odds the day the model runs; decide money questions against that data.]
- **2026-09-14** — Data validation: **GO.** Kaggle bulk (per-round stats + odds datasets, free) as v1 source; UFCStats scrape as v2 incremental source. Residual: Emil's sample pull + robots/ToS eyeball.
- **2026-09-18** — DP-700 exam **pinned**. Project decoupled from the exam; consultancy outreach launched to measure real-world DP-600/700 emphasis (tag responses consultancy vs. in-house — consultancies structurally over-weight certs via partner tiers). Exam-area mapping retained in project definition at zero cost.
- **2026-09-18** — Project moves to its own repo + Claude Code project. v1 CI/CD = GitHub + Fabric native Git sync only; deployment pipelines/multi-env = v2.
- **2026-09-18** — Gate dates carried as proposals pending Emil's confirmation: Phase 1 end-to-end 3 Oct (cut scope, don't extend); Phase 2 published 2 Nov.
- **2026-09-20** — Doc review completed by Emil. Learning protocol added to CLAUDE.md: Emil writes the code first, Claude explains the approach and reviews after; if Emil asks Claude to write the whole thing, Claude checks once whether he is sure.
- **2026-09-20** — **Scope change:** Phase 1 no longer ends at clean data. It now ends at a Power BI report plus a simple baseline model (win/loss record, age and age gap, layoff, weight class, reach/height). Power BI learning time is budgeted inside Sprint 4, and is the one exception to the no-study-tracks rule.
- **2026-09-20** — Baseline model doubles as the floor the Phase 2 Elo/logistic model must beat; both get reported against the same holdout. A hard-to-beat baseline is a finding, not a failure.
- **2026-09-20** — **Displaced from Phase 1 by the above:** RLS/masking demo and monitoring + one diagnosed pipeline failure. Moved to Sprint 5–7 slack or v2.
- **2026-09-20** — `docs/build-plan.md` created: 8 sprints (Sprint 0 + 7), calendar weeks aligned to the `Todo.md` Uge numbering, each with a binary "Done when", plus an ordered cut list.
- **2026-09-20** — Life OS `Todo.md` and `# Project Dashboard.md` confirmed stale by Emil. The "data dashboard project / deadline September / jobmesse" line refers to the superseded Market Stress project; MMA is the single active discretionary track. Both Obsidian files need updating.
- **2026-09-20** — **CONFIRMED by Emil: gate dates and capacity.** Phase 1 end-to-end **Sat 17 Oct**; Phase 2 published **Sun 8 Nov**. Committed capacity **15 h/week** (3 h × 5 weekdays), chosen over a nominal 4–5 h/day because the job search, training and drawing run concurrently and unfamiliar-technology hours burn double. Supersedes the 3 Oct / 2 Nov proposals of 2026-09-18. Hours above 15 in a good week go to **buffer, not to an earlier gate** — Sprint 3 (entity resolution) and Sprint 4 (Power BI) are the two that will need it. The date is a ceiling: finishing early is allowed, extending is not.
- **2026-09-20** — **Platform is combat sports, not MMA-only.** The repo was always named `combat_sports_data_platform`; the docs had narrowed it. Titles and framing corrected across CLAUDE.md, README, project-definition and build-plan. Historical entries above left as written.
- **2026-09-20** — Design consequence of the above, settled now because it is cheap now: `dim_event` carries `sport` + `promotion`, `fact_round` inherits them, fighter natural keys scoped by source/promotion rather than name. Constant values in v1. Round-level measures treated as sport-specific, not one flat MMA-shaped table.
- **2026-09-20** — v1 **load** stays UFC/MMA only. Combat-sports naming and schema do not reopen the out-of-scope list; second sport/organisation ingest remains v2.

## Parked (with reconsider dates)

- Other organisations' data (ONE FC, PFL, Bellator) — **reconsider at Phase 2 completion.** Raised 2026-09-20.
- Cross-source reliability reconciliation — Phase 1 ships the descriptive version only (record coverage claims, count disagreements). Anything further: **reconsider at v2.** Raised 2026-09-20.
- Blog / long-form write-up of the analysis — the Phase 2 README is v1 of this. **Reconsider once Phase 2 ships.** Raised 2026-09-20.

- Betting / bankroll / picks / model-access sales — **reconsider at Phase 2 completion** (see 2026-09-13 entry).
- DP-700 retake booking — **reconsider when consultancy outreach returns** (target: ~2 weeks).
- Eventstream / real-time layer — reconsider only if v1 ships before gate.
- Full CI/CD buildout — reconsider at v2.
- Warrior Channel — prior reconsider date 20 Nov stands, subordinate to this project.
