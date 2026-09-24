# Combat Sports Data Platform — Project Definition

*v1.3 · 2026-09-24 · Owner: Emil · Status: active*

> Source of truth for scope. Any change to this file gets a dated entry in `decisions.md`.
> Week-by-week execution lives in `build-plan.md`.
> **One timeline only: Phases.** There is no "v1/v2". Anything not in a phase is on the Parked list in
> `decisions.md` with a reconsider date.

---

## Mission

Build an end-to-end data platform and forecasting model on **combat sports** fight data in Microsoft Fabric. MMA/UFC is the initial load; the platform is deliberately not MMA-only.

- **Pipeline:** ingestion → PySpark cleaning/entity resolution → warehouse star schema → model → Power BI serving.
- **Public framing:** "an end-to-end forecasting pipeline with calibrated probability estimation." Method first, sport second. Never "betting system."

**Three purposes, in this order:**

1. **Learn** production data-engineering workflows by building, not studying.
2. **Portfolio artifact** aimed at real readers (recruiters, consultancies): platform + model + honest evaluation.
3. **Foundation** for possible later phases — see Phase 4.

*DP-700 exam: pinned as of 2026-09-18, pending consultancy outreach on cert value. The architecture still maps to exam areas 3/4/7 (see Appendix), so the retake option stays warm at zero extra cost.*

---

## Phases & gates

| Phase | What | Gate |
|---|---|---|
| 0 | Setup | done except ToS check (moved into Sprint 1) |
| 1 | Platform on Kaggle data → Power BI + baseline | **Sat 17 Oct** |
| 2 | Source switch: UFCStats scrape, incremental load | **Sun 8 Nov** |
| 3 | Model: Elo + logistic, calibrated, published | **Sun 29 Nov** |
| 4 | Placeholder — products/monetization, zero build | decision at Phase 3 gate |

**Why this order:** the model is trained and evaluated on the data the platform will actually run on, not on a frozen third-party snapshot. The switch must be done before modelling starts.

### Phase 0 — Setup

- GitHub repo ✔
- Fabric workspace ↔ Git sync, understood in both directions ✔
- Kaggle sample pull verified, coverage note written
- ufcstats.com robots.txt/ToS eyeballed — **blocking for Phase 2, so it is done in Sprint 1, not later.** If scraping is not allowed, Phase 2 has to be redesigned before 17 Oct, not discovered after.

**Gate:** repo exists, data validated on Emil's side.

### Phase 1 — Platform (Kaggle)

- **Bronze:** Kaggle bulk ingest, raw and immutable, incremental-ready design.
- **Silver:** PySpark cleaning, fighter-name entity resolution, dedup. Built to the **silver contract** (see *Source-swap readiness*).
- **Gold:** star schema — `fact_round` grain, `dim_fighter` as SCD2, `dim_event`, `dim_date`.
- **Baseline model:** deliberately simple, interpretable (see *Modelling* below).
- **Power BI report:** descriptive trend pages + the baseline. This is what makes it end-to-end.

**Gate:** runs end-to-end by the gate date, or scope gets **cut** — never extended.

### Phase 2 — Source switch (UFCStats scrape)

- Scraper for UFCStats event → fight → round pages, run from a Fabric notebook, landing `bronze_ufcstats_*`.
- `20_clean_ufcstats` maps the scrape onto the **same silver contract** as Kaggle.
- **Incremental load:** new events appended via Delta `MERGE` on natural keys; re-running is idempotent (no duplicates). Late-arriving corrections handled here.
- **Overlap reconciliation:** events present in both sources compared; disagreements counted and explained.
- Fighter identity bridged Kaggle ↔ UFCStats (by UFCStats fighter ID if Kaggle carries it; otherwise the Sprint 3 resolution ladder).
- Gold and Power BI refresh with the scraped data **without changes downstream of silver**. Any change that was needed is listed honestly — that list is the test of the contract.
- **Open, decided in Sprint 5:** does the scrape backfill full history, or does Kaggle stay the historical source up to 2026-05-16 with the scrape taking over after it?

**Gate:** gold tables include fights after 2026-05-16 from the scrape, the incremental load re-runs cleanly, and the disagreement count is written up.

### Phase 3 — Model

- Elo/Glicko ratings.
- Logistic regression on rating + contextual features.
- Calibration curve, Brier score, log loss.
- Holdout backtest, benchmarked against closing odds.
- Public timestamped paper prediction log starts the day the model runs (the scraper supplies upcoming cards).
- **Must beat the Phase 1 baseline**, reported honestly either way.

**Gate:** published — repo public, README tells the story.

### Phase 4 — Placeholder, zero build now

- Possible: analysis products, AI-agent workflows, monetization.
- **Decision point:** when Phase 3's model exists and has holdout numbers.
- Nothing from this phase may enter Phase 1–3 scope.

*Gate dates are confirmed via `decisions.md`; sprint detail lives in `build-plan.md`.*

---

## Modelling

Two models, and the order matters.

### Baseline (Phase 1) — simple on purpose

- Plainly interpretable features only: win/loss record, age and **age gap between fighters**, weight class, layoff since last fight, reach/height differential.
- No Elo, no tuning, no feature engineering beyond arithmetic.
- **Two jobs:** (a) put something real on the Phase 1 dashboard; (b) set the floor the Phase 3 model must clear.
- Paired with descriptive trend pages — win rate by age band per weight class, finish rate by weight class, title-fight vs. undercard splits. Big trends, read across weight classes.
- Re-run on the post-switch data at the start of Phase 3, so baseline and model share one holdout.

### Full model (Phase 3)

- Elo/Glicko ratings → logistic regression → calibration.
- Scored against the baseline **and** against closing odds.
- Features are computed only from fights *before* the fight being predicted. Third-party pre-computed feature tables (e.g. Kaggle `ufc_features`) are not model inputs.

A baseline that is hard to beat is a finding, not a failure. Publish it either way.

---

## Data sources

- **Phase 1 — bulk (no scraping):** Kaggle — per-round stats + odds ("UFC Data: Stats & Rankings & Betting Odds"), "UFC Fights 2010–2020 with Betting Odds", daily-updated odds dataset. Free. Coverage as loaded: 8,555 fights, 1994-03-11 → 2026-05-16 (see `decisions.md` 2026-09-24).
- **Phase 2 — live:** UFCStats round-by-round scrape. Mature OSS scrapers exist (DavesAnalytics Python/R, Scrapy variants). Gives a genuine incremental-load story.
- **Residual checks (Emil, minutes):**
  - ☐ verify Kaggle coverage, recency, quality → `docs/data-coverage.md`
  - ☐ eyeball ufcstats.com robots.txt/ToS — Sprint 1, blocking for Phase 2

### Reliability & provenance

Source reliability is a real portfolio topic, not a footnote: Kaggle datasets are third-party re-publications of UFCStats with unknown maintenance. Handled *descriptively* — record what each source claims to cover, and where two sources disagree, count the disagreements (Phase 2 overlap reconciliation). Reconciliation logic beyond counting and additional org data (ONE FC, PFL, Bellator) are **parked**; see `decisions.md`.

---

## Architecture decisions (settled — reopen only via `decisions.md`)

- **Medallion:** Bronze (raw, immutable) → Silver (cleaned, conformed) → Gold (warehouse, star schema).
- **Sport and promotion are first-class keys.** `dim_event` carries `sport` and `promotion`; `fact_round` inherits them; fighter natural keys are scoped by source and promotion, never by name alone. For now every row reads `MMA` / `UFC` — constant and free now, expensive to retrofit once SCD2 history exists.
- **Round-level measures are sport-specific.** The `fact_round` grain holds across combat sports; the measure set does not (MMA control time and takedowns vs. boxing knockdown counts). Do not model one flat wide table that assumes MMA columns forever.
- **Grain: `fact_round`** — round-level, with strike/control measures. Big enough to make optimization real; the grain decision itself is portfolio material.
- **`dim_fighter` as SCD Type 2** — fighters change camps and weight classes, reach entries get corrected.
- **Odds as a benchmark column** for model evaluation, never as a model input.
- **Version control:** GitHub + Fabric native Git integration. CI/CD stops there — no deployment pipelines, no multi-environment, no custom Actions (parked).

### Source-swap readiness — built into Phase 1, because Phase 2 depends on it

The Kaggle → scrape switch must be a new ingest + new cleaning notebook, nothing more. Rules:

1. **Silver is the contract.** `docs/silver-contract.md` defines `silv_fight`, `silv_round`, `silv_fighter`: our column names, types, grain, natural keys. Nothing source-specific.
2. **One bronze table set and one cleaning notebook per source:** `10_ingest_kaggle` → `20_clean_kaggle`; later `10_ingest_ufcstats` → `20_clean_ufcstats`. Source-specific column names exist **only** inside that source's cleaning notebook.
3. **Nothing downstream of silver** (warehouse, model, Power BI) reads bronze or uses a source-specific column name.
4. **Every silver row carries `source`** and the source's own record ID/URL where one exists.
5. **Prefer UFCStats IDs as natural keys.** Both sources ultimately come from UFCStats; if Kaggle carries UFCStats fight/fighter IDs or URLs, they are the bridge. Checked in Sprint 1.
6. **Kaggle is added to, not replaced.** It stays in bronze as the historical source and the reconciliation reference.

---

## Out of scope (Phases 1–3) — the list that protects the gates

- Betting, bankroll, stake-sizing, selling anything — parked until the Phase 4 decision point.
- Full CI/CD apparatus (see above).
- Eventstream / real-time streaming — the Phase 2 scrape is scheduled batch, not streaming.
- Second sport, second organisation — **loading** them is parked. The schema accommodates them from day one (see above); Phases 1–3 ingest UFC only.
- Custom tooling, pipeline automation beyond what Fabric gives natively.
- Intensive SQL/PySpark/KQL *study tracks* — learning happens inside the build. The one budgeted exception is Power BI, because Phase 1 ends in a report.

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
| Incremental load / MERGE (Phase 2) | Areas 2 + 3 |
| Silver / PySpark cleaning | Area 3 |
| Gold / warehouse + T-SQL + SCD2 | Areas 3 + 4 |
| RLS / masking | Area 4 |
| Monitoring / troubleshooting | Area 7 |
| Eventstream (parked) | Area 5 |

The Dataflow-vs-Notebook-vs-T-SQL decision table (2 pages) remains worth writing: interview-relevant regardless of the exam.
