# Combat Sports Data Platform — Project Definition

*v1.2 · 2026-09-20 · Owner: Emil · Status: active*

> Source of truth for scope. Any change to this file gets a dated entry in `decisions.md`.
> Week-by-week execution lives in `build-plan.md`.

---

## Mission

Build an end-to-end data platform and forecasting model on **combat sports** fight data in Microsoft Fabric. MMA/UFC is the v1 load; the platform is deliberately not MMA-only.

- **Pipeline:** ingestion → PySpark cleaning/entity resolution → warehouse star schema → model → Power BI serving.
- **Public framing:** "an end-to-end forecasting pipeline with calibrated probability estimation." Method first, sport second. Never "betting system."

**Three purposes, in this order:**

1. **Learn** production data-engineering workflows by building, not studying.
2. **Portfolio artifact** aimed at real readers (recruiters, consultancies): platform + model + honest evaluation.
3. **Foundation** for possible later phases — see Phase 3.

*DP-700 exam: pinned as of 2026-09-18, pending consultancy outreach on cert value. The architecture still maps to exam areas 3/4/7 (see Appendix), so the retake option stays warm at zero extra cost.*

---

## Phases & gates

### Phase 0 — Setup

- GitHub repo ✔
- Fabric workspace ↔ Git sync, understood in both directions
- Kaggle sample pull verified, coverage note written
- ufcstats.com ToS eyeballed (v2 only)

**Gate:** repo exists, data validated on Emil's side.

### Phase 1 — Platform

- **Bronze:** Kaggle bulk ingest, raw and immutable, incremental-ready design.
- **Silver:** PySpark cleaning, fighter-name entity resolution, dedup, late-arriving handling.
- **Gold:** star schema — `fact_round` grain, `dim_fighter` as SCD2, `dim_event`, `dim_date`.
- **Baseline model:** deliberately simple, interpretable (see *Modelling* below).
- **Power BI report:** descriptive trend pages + the baseline. This is what makes it end-to-end.
- **If time allows:** RLS/masking demo · monitoring + one diagnosed pipeline failure.

**Gate:** runs end-to-end by the gate date, or scope gets **cut** — never extended.

### Phase 2 — Model

- Elo/Glicko ratings.
- Logistic regression on rating + contextual features.
- Calibration curve, Brier score, log loss.
- Holdout backtest, benchmarked against closing odds.
- Public timestamped paper prediction log starts the day the model runs.
- **Must beat the Phase 1 baseline**, reported honestly either way.

**Gate:** published — repo public, README tells the story.

### Phase 3 — Placeholder, zero build now

- Possible: analysis products, AI-agent workflows, monetization.
- **Decision point:** when Phase 2's model exists and has holdout numbers.
- Nothing from this phase may enter Phase 1–2 scope.

*Gate dates live in `build-plan.md` and are confirmed via `decisions.md`.*

---

## Modelling

Two models, and the order matters.

### Baseline (Phase 1) — simple on purpose

- Plainly interpretable features only: win/loss record, age and **age gap between fighters**, weight class, layoff since last fight, reach/height differential.
- No Elo, no tuning, no feature engineering beyond arithmetic.
- **Two jobs:** (a) put something real on the Phase 1 dashboard; (b) set the floor the Phase 2 model must clear.
- Paired with descriptive trend pages — win rate by age band per weight class, finish rate by weight class, title-fight vs. undercard splits. Big trends, read across weight classes.

### Full model (Phase 2)

- Elo/Glicko ratings → logistic regression → calibration.
- Scored against the baseline **and** against closing odds.

A baseline that is hard to beat is a finding, not a failure. Publish it either way.

---

## Data sources

- **v1 bulk (no scraping):** Kaggle — per-round stats + odds ("UFC Data: Stats & Rankings & Betting Odds"), "UFC Fights 2010–2020 with Betting Odds", daily-updated odds dataset. Free.
- **v2 incremental:** UFCStats round-by-round scrape. Mature OSS scrapers exist (DavesAnalytics Python/R, Scrapy variants). Gives a genuine incremental-load story.
- **Residual checks (Emil, minutes):**
  - ☐ sample-download the Kaggle per-round dataset; verify coverage, recency, quality
  - ☐ eyeball ufcstats.com robots.txt/ToS before any scraping

### Reliability & provenance — open question

Source reliability is a real portfolio topic, not a footnote: Kaggle datasets are third-party re-publications of UFCStats with unknown maintenance. Phase 1 handles this *descriptively* — record what each source claims to cover, and where two sources disagree, count the disagreements. Anything beyond that (reconciliation logic, additional org data — ONE FC, PFL, Bellator) is **parked**; see `decisions.md`.

---

## Architecture decisions (settled — reopen only via `decisions.md`)

- **Medallion:** Bronze (raw, immutable) → Silver (cleaned, conformed) → Gold (warehouse, star schema).
- **Sport and promotion are first-class keys.** `dim_event` carries `sport` and `promotion`; `fact_round` inherits them; fighter natural keys are scoped by source and promotion, never by name alone. In v1 every row reads `MMA` / `UFC` — constant and free now, expensive to retrofit once SCD2 history exists.
- **Round-level measures are sport-specific.** The `fact_round` grain holds across combat sports; the measure set does not (MMA control time and takedowns vs. boxing knockdown counts). Do not model one flat wide table that assumes MMA columns forever.
- **Grain: `fact_round`** — round-level, with strike/control measures. Big enough to make optimization real; the grain decision itself is portfolio material.
- **`dim_fighter` as SCD Type 2** — fighters change camps and weight classes, reach entries get corrected.
- **Odds as a benchmark column** for model evaluation, never as a model input.
- **Version control:** GitHub + Fabric native Git integration. **v1 CI/CD stops there** — no deployment pipelines, no multi-environment, no custom Actions. That is v2, after data flows end-to-end.

---

## Out of scope (v1) — the list that protects the gates

- Betting, bankroll, stake-sizing, selling anything — parked until the Phase 3 decision point.
- Full CI/CD apparatus (see above).
- Eventstream / real-time — stretch only if v1 ships early.
- Second dataset, second sport, second organisation — **loading** them is v2. The schema accommodates them from day one (see above); v1 still ingests UFC only.
- Custom tooling, pipeline automation beyond what Fabric gives natively.
- Intensive SQL/PySpark/KQL *study tracks* — learning happens inside the build. The one budgeted exception is Power BI, because Phase 1 now ends in a report.

---

## Working agreements

- One discretionary track at a time; this is it.
- **Weekly:** short review against the gate date. Decisions and scope changes get dated entries in `decisions.md`.
- Claims in project docs are labeled **[fact]** or **[recommendation]** when it matters.
- Emil writes the code first; Claude explains the approach and reviews afterwards.

---

## Appendix — DP-700 area mapping (retained; exam pinned)

| Component | Exam area |
|---|---|
| Bronze / ingestion | Area 2 |
| Silver / PySpark cleaning | Area 3 |
| Gold / warehouse + T-SQL + SCD2 | Areas 3 + 4 |
| RLS / masking | Area 4 |
| Monitoring / troubleshooting | Area 7 |
| Eventstream (stretch) | Area 5 |

The Dataflow-vs-Notebook-vs-T-SQL decision table (2 pages) remains worth writing: interview-relevant regardless of the exam.
