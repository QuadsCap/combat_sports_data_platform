# MMA Fight-Data Platform — Project Definition

*v1.0 · 2026-09-18 · Owner: Emil · Status: active. This is the source of truth for scope. Changes to it get a dated entry in `decisions.md`.*

## Mission

Build an end-to-end data platform and forecasting model on MMA fight data in Microsoft Fabric: ingestion → PySpark cleaning/entity resolution → warehouse star schema → calibrated fight-outcome model → Power BI serving. The public framing is **"an end-to-end forecasting pipeline with calibrated probability estimation"** — method first, sport second.

**The project serves three purposes, in this order:**
1. Learn production data-engineering workflows by building, not studying.
2. A portfolio artifact with real users' questions in mind (recruiters, consultancies): platform + model + honest evaluation.
3. Foundation for possible later phases (analysis products, AI agents) — see Phase 3.

*(DP-700 exam: pinned as of 2026-09-18, pending consultancy outreach on cert value. The architecture still maps to exam areas 3/4/7 — see Appendix — so the retake option stays warm at zero extra cost.)*

## Phases & gates

| Phase | Content | Gate |
|---|---|---|
| **0. Setup** (this week) | GitHub repo, Fabric workspace Git sync, Kaggle sample pull verified, ufcstats.com ToS eyeballed | Repo exists, data validated on Emil's side |
| **1. Platform** (~4 wks) | Bronze: Kaggle bulk ingest, incremental-ready design → Silver: PySpark cleaning, fighter-name entity resolution, dedup, late-arriving handling → Gold: warehouse star schema, `fact_round` grain, `dim_fighter` as SCD2, `dim_event`, `dim_date` → RLS/masking demo → monitoring + one diagnosed pipeline failure | **[proposed] 3 Oct: runs end-to-end or scope gets cut.** No extension — cut scope instead. |
| **2. Model** | Elo/Glicko ratings → logistic regression → calibration curve, Brier score, log loss → holdout backtest → benchmarked against closing odds (benchmark column) → public, timestamped paper prediction log starts the day the model runs | **[proposed] 2 Nov: published** (repo public, README tells the story) |
| **3. Placeholder — zero build now** | Possible: analysis products, AI-agent workflows, monetization. **Decision point: when Phase 2's model exists and has holdout numbers.** Nothing from this phase may enter Phase 1–2 scope. | — |

## Data sources

- **v1 bulk (no scraping):** Kaggle — per-round stats + odds ("UFC Data: Stats & Rankings & Betting Odds"), "UFC Fights 2010–2020 with Betting Odds", daily-updated odds dataset. Free.
- **v2 incremental:** UFCStats round-by-round scrape (mature OSS scrapers exist: DavesAnalytics Python/R, Scrapy variants). Gives a genuine incremental-load story.
- **Residual checks (Emil, minutes):** ☐ sample-download the Kaggle per-round dataset, verify coverage/recency/quality · ☐ eyeball ufcstats.com robots.txt/ToS before any scraping.

## Architecture decisions (settled — reopen only via decisions.md)

- Medallion: Bronze (raw, immutable) → Silver (cleaned, conformed) → Gold (warehouse, star schema).
- **Grain: `fact_round`** — round-level, with strike/control measures. Big enough to make optimization real; the grain decision itself is portfolio material.
- `dim_fighter` as **SCD Type 2** (fighters change camps, weight classes, reach entries get corrected).
- Odds appear as a **benchmark column** for model evaluation.
- Version control: GitHub + Fabric native Git integration. **v1 CI/CD stops there** — no deployment pipelines, no multi-environment, no custom Actions. That is v2, after data flows end-to-end.

## Out of scope (v1) — the list that protects the gates

- Betting, bankroll, stake-sizing, selling anything — parked until the Phase 3 decision point (model + holdout numbers in hand).
- Full CI/CD apparatus (see above).
- Eventstream/real-time (stretch only if v1 ships early).
- Second dataset, second sport, custom tooling, pipeline automation beyond what Fabric gives natively.
- Intensive SQL/PySpark/KQL *study tracks* — learning happens inside the build; no parallel course work.

## Working agreements

- One discretionary track at a time; this is it.
- Weekly: short review against the gate date; decisions and scope changes get dated entries in `decisions.md`.
- Claims in project docs are labeled **[fact]** or **[recommendation]** when it matters.

## Appendix — DP-700 area mapping (retained; exam pinned)

Bronze/ingestion → Area 2 · Silver/PySpark cleaning → Area 3 · Gold/warehouse + T-SQL + SCD2 → Areas 3+4 · RLS/masking → Area 4 · Monitoring/troubleshooting → Area 7 · Eventstream (stretch) → Area 5. The Dataflow-vs-Notebook-vs-T-SQL decision table (2 pages) remains worth writing: interview-relevant regardless of the exam.
