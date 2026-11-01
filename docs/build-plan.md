# Build Plan — Combat Sports Data Platform

*v1.1 · 2026-09-24 · Owner: Emil*

> Scope lives in `project-definition.md`. This file is only **what gets built which week**.
> Sprints are calendar weeks aligned to the `Todo.md` Uge numbering (Mon–Sun), so next actions
> drop straight into the existing weekly blocks. No parallel tracking system.

---

## Confirmed

**Gate dates: Phase 1 = Sat 17 Oct · Phase 2 (source switch) = Wed 4 Nov · Phase 3 (model) = Sun 29 Nov.**
**Committed capacity: 15 h/week — 3 h x 5 weekdays.** Weekends free.

Capacity confirmed 2026-09-20. Phase order and the 29 Nov model gate confirmed 2026-09-24: the
scrape comes before the model, which moved the model's publish date from 8 Nov to 29 Nov.

Chosen over a nominal 4-5 h/day: the job search, training and drawing run concurrently, and hours
spent on unfamiliar technology burn roughly double. Sprints below are sized 12-14 h, so each fits
its week with slack.

Hours above 15 in a good week buy **buffer, not an earlier gate.** Sprint 3 (entity resolution),
Sprint 4 (Power BI) and Sprint 6 (incremental merge) are where the buffer gets spent.

The date is a ceiling, not a target. Finish early and the gate lands early. Extending is not an
option -- scope gets cut instead, from the cut lists below.

---

## The spine

If everything else is cut, this still has to exist by the Phase 1 gate:

> **Kaggle file → bronze table → clean silver table (to the contract) → `fact_round` + `dim_fighter` → one Power BI page.**

And by the Phase 2 gate:

> **Cito event → bronze → same silver contract → same gold → same report, no downstream edits.**

---

## Phase 1 — Platform on Kaggle · gate Sat 17 Oct · **done 2026-10-16**

End-to-end run clean (decisions.md 2026-10-16). Cuts taken: fuzzy matching (→ Phase 2), SCD2 → SCD1.

### Sprint 0 — Sun 20 Sep · done

- [x] Connect Fabric workspace ↔ GitHub repo (Workspace settings → Git integration).
- [x] Sync both directions. This is the whole of the CI/CD scope — do not build more than this.
- [x] Kaggle: sample-download the per-round dataset. Open it. Look at it.

---

### Sprint 1 — Uge 4 (Mon 21 – Sun 27 Sep) · ~12 h

**Goal: raw data is in Fabric and you know what it actually contains.**

- [x] `10_ingest_kaggle` — land the raw files into the lakehouse as `bronze_*`. Raw and immutable:
      no cleaning, no renaming, no type casting. Add ingest timestamp + source filename columns.
- [x] **Coverage note** → `docs/data-coverage.md`: date range, fight count, round count, how many
      fights have per-round detail vs. only totals, how many have odds, null rates on the columns
      that matter. Plain numbers.
- [x] **Swap check:** does Kaggle carry UFCStats fight/fighter IDs or URLs? Yes/no in the coverage
      note — it decides how Phase 2 bridges fighter identity.
- [x] **ufcstats.com robots.txt/ToS** — 10 min. Blocking for Phase 2; if the answer is no, it
      must surface now, not on 19 Oct.
- [x] Write down the first three things that look wrong. They will all matter later.

**Done when:** `bronze_*` tables are queryable in Fabric, and `data-coverage.md` answers "can this
dataset support a `fact_round` grain at all?" with a yes/no and the numbers behind it.

**Concept this sprint:** why bronze stays immutable, and what an incremental-ready design means
while you are still doing full loads.

---

### Sprint 2 — Uge 5 (Mon 28 Sep – Sun 4 Oct) · ~12 h

**Goal: clean, typed, deduplicated silver — to a written contract.**

- [x] `docs/silver-contract.md` — first, before code: `silv_fight`, `silv_round`, `silv_fighter`
      with our column names, types, grain, natural keys, and a `source` column. One page.
- [x] `20_clean_kaggle` — types, dates parsed (`date` everywhere, not `timestamp`), weight classes
      normalised, method-of-victory standardised, obvious junk rows dropped **and counted**.
      Kaggle column names appear only in this notebook.
- [x] Rounds stored wide (`r1_…r5_`)? Unpivot to one row per fight per round.
- [x] Dedup: identify the natural key for a fight and for a round. Count what dedup removes.
- [x] Reconcile row counts bronze → silver. Every dropped row explained by a number.

**Done when:** `silv_fight` and `silv_round` exist, match the contract, and you can state
bronze→silver row counts with a one-line reason for each delta.

---

### Sprint 3 — Uge 6 (Mon 5 – Sun 11 Oct) · ~14 h · hardest sprint

**Goal: fighters resolve to one identity, and the dimensions exist.**

- [x] Entity resolution: UFCStats ID if available → exact match → normalised match → fuzzy match  *(ID only — every Kaggle fighter has a UFCStats ID; fuzzy moved to Phase 2, decisions.md 2026-10-07)*
      → manual override map. Deterministic first, always. `src/` holds the helpers.
- [x] **Keep the failure log.** Every name the fuzzy pass got wrong, and what caught it. Per  *(n/a in Phase 1 — no fuzzy pass; see above)*
      CLAUDE.md this is the portfolio differentiator, not an appendix.
- [x] `dim_fighter` as SCD2 — valid_from / valid_to / is_current.  *(built as SCD1 — decisions.md 2026-10-07)*
- [x] `dim_event`, `dim_date`.

**Done when:** a fighter with known name variants resolves to one `fighter_key`, and one fighter
shows two SCD2 rows with correct validity windows.

**Cut first:** fuzzy matching entirely — ship exact + manual map, and say so honestly.

---

### Sprint 4 — Uge 7 (Mon 12 – Sat 17 Oct) · ~14 h · **PHASE 1 GATE**

**Goal: end-to-end. A human opens a report and learns something.**

- [x] `30_warehouse_fact_round` — the fact table, keys wired to the dimensions. Reads silver only.
- [x] `40_model_baseline` — win/loss record, age, age gap, layoff, weight class, reach/height
      differential. Logistic regression, no tuning. Record accuracy and log loss. **This is the
      number the Phase 3 model has to beat.**
- [x] **Power BI** — budget ~5 h of this sprint, learning included. One report, two pages:
      descriptive trends (win rate by age band across weight classes, finish rate by weight class)
      and baseline model output.
- [x] Update the README so a stranger understands the project in 60 seconds.

**Done when — Sat 17 Oct:** a Power BI report opens on the gold tables and shows real numbers, and
the whole path Kaggle → report can be re-run. If it does not run end-to-end, **scope gets cut, not
the date.**

---

## Phase 2 — Source switch · gate Wed 4 Nov · **in progress from 19 Oct**

> **Gate: Wed 4 Nov** (decisions.md 2026-10-24). No buffer after it — 5–8 Nov is not build time.

> **Source: Cito API** (decisions.md 2026-10-18), replacing the own UFCStats scraper the ToS check
> ruled out. Cito event IDs = UFCStats IDs, so fights and fighters join to Kaggle without name matching.

### Sprint 5 — Uge 8 (Mon 19 – Sun 25 Oct) · ~12 h

**Goal: one Cito event lands in bronze.**

- [x] **Decide the Phase 2 source** → Cito API (decisions.md 2026-10-18).
- [x] API key out of the notebook (Key Vault `combat-sports-vang-v1`, verified 2026-10-18): Fabric Git sync commits notebook source, so a key typed in a
      cell lands on GitHub.
- [x] Explore the free tier on one recent event (`11_explore_cito`, decisions.md 2026-10-18): response shape, IDs, calls per event, and whether
      Cito carries odds, judge scores and fighter profiles (dob, reach, height).
- [x] Gap 16 May → ~18 Sep: filled from Kaggle scarekrow v2, not Cito Pro (decisions.md 2026-10-18).
      Its own `10_ingest` / `20_clean` pair, after the Cito path works.
- [x] `10_ingest_cito` — one event → bouts → rounds, raw JSON landed as `bronze_cito_*` with
      ingest timestamp + request URL. Raw, immutable; no flattening in bronze.

**Done when:** one event after 2026-05-16 is queryable in bronze with round-level stats. **Met 2026-10-19:** 5 events (19 Sep → 17 Oct), 12–14 bouts and 46–54 round-stat rows each.

---

### Sprint 6 — Uge 9 (Mon 26 Oct – Sun 1 Nov) · ~13 h

**Goal: Cito data flows into the same silver, incrementally.**

- [x] `20_clean_cito` — map the JSON onto the silver contract. Cito field names only here.
      **Done 2026-10-21** (decisions.md): 5 events / 62 fights / 126 rounds / 21 new fighters in silver, 46 checks pass.
      Writes with `replaceWhere "source = 'cito'"` (full rebuild of Cito rows from all of bronze), not `MERGE` — see below.
- [x] Refresh fighter profiles (dob, reach, height) from Cito, if it carries them, and add a silver check:
      profile completeness by debut year. Kaggle left recent fighters' profiles empty (decisions.md 2026-10-11).
      **Mostly moot since 2026-10-24:** every Cito fighter now resolves to a Kaggle/kagglesk fighter that carries a profile; only the completeness check is left.
      **Check added 2026-10-28** (decisions.md), report only; result still to be recorded.
- [x] Incremental load: Delta `MERGE` on natural keys. Run it twice; the second run changes
      nothing. That is the idempotency test.
      **Done 2026-10-25** (decisions.md): `20_clean_cito`, 35-day lookback; second run 0 inserted / 0 updated.
- [x] Load every event from 2026-05-16 to today (as far as the gap decision allows).
      **Done 2026-10-24:** gap 16 May → 17 Oct from kagglesk (`10_ingest_kagglesk` / `20_clean_kagglesk`, 249 fights); Cito through 17 Oct, no newer card with stats yet — each `10_ingest_cito` run picks up new events.

**Done when:** `silv_fight` / `silv_round` hold both sources, and a re-run adds zero rows. **Met 2026-10-24** with three sources; every `20_clean_*` writes via `replaceWhere` on its own `source`, so re-runs leave counts unchanged (verified per table + `DESCRIBE HISTORY`).

**Concept this sprint:** watermarks, idempotent loads, and why `MERGE` beats append.

---

### Sprint 7 — Uge 9–10 (Thu 29 Oct – Wed 4 Nov) · **PHASE 2 GATE**

**Goal: the switch is proven end to end.**

- [x] **Overlap reconciliation:** pull a few events Kaggle also has; count disagreements per
      column, explain the top ones. Written up in `docs/data-coverage.md`. (scarekrow v2 vs Cito on
      their overlap, ~18 Sep → 10 Oct.)
      **Done 2026-10-24** (`21_reconcile_sources`): 5 events, 62/62 fights matched, 1 fight missing in Cito, 0 disagreements on 11 fight columns after fixing a draw/NC mapping bug the check found.
- [x] Fighter identity bridged across sources; Cito-only fighters get new `fighter_key`s.
      **Done 2026-10-24** by run order (Kaggle → kagglesk → Cito): all 124 Cito fighters resolve to UFCStats IDs, 0 Cito-only fighters; confirmed by the reconciliation.
- [x] Gold + Power BI refreshed on the combined data. **List every downstream change that was
      needed** — ideally none. The list is the grade on the silver contract.
      **Done 2026-10-28** (decisions.md): all gold checks pass, 263 fights after 16 May in gold and in the
      Power BI model. Downstream changes: the overlap rule + Kaggle-only hard-coded expectations; no column,
      type or key change; none in Power BI.
- [x] **End-to-end test, "layers ready"** (Thu 29 Oct): timed run `10_ingest_cito → 20 → 30 → 31`, second run
      with identical counts, one post-16 May fight traced bronze → `fact_round` against ufcstats.com.
      **Done 2026-10-29** (decisions.md): run clean, counts equal to 28 Oct, Kavanagh vs Temirov equal to
      ufcstats.com. Run 2 and manual timing dropped (covered by 28 Oct runs / the first pipeline run).

*Added to the gate 2026-10-24 (decisions.md); day plan 2026-10-28, Saturday 31 Oct off:*

- [x] **Scheduled Fabric pipeline** (Thu 29 afternoon + Fri 30 Oct, 3–4.5 h): `10 → 20 → 30 → 31` as notebook
      activities, `LOOKBACK_DAYS` as a parameter, schedule, **email alert on failure**. Open on Fri: run
      frequency (Cito free tier 500 calls/month) and a periodic full refresh (`LOOKBACK_DAYS = None`).
      **Done 2026-11-01** (decisions.md): `pipeline_cito_refresh`, weekly Monday, `lookback_days` (-1 = full
      refresh, by hand), failure alert as ntfy push (no mailbox for email), tested with a real failure.
      Still to see: the first unattended scheduled run, Mon 2 Nov.
- [ ] **Two decisions** (Fri 30 Oct, else Mon morning): dashboard tool (Power BI vs Evidence) and how the
      public/private split is done.
- [ ] **Dashboard redesign** (Mon 2 – Tue 3 Nov morning).
- [ ] **GitHub packaging** (Tue 3 Nov): README with the Phase 1–2 story, one architecture diagram,
      screenshots, repo cleanup.
- [ ] **Public/private split executed + gate check** (Wed 4 Nov).

**Cut order for these, in this order** (Claude's proposal 2026-10-28, decided on the day): (1) pipeline run log
table → Fabric run history; (2) dashboard redesign → current report + readability fixes; (3) diagram →
screenshots only. Not cuttable: the scheduled pipeline + failure alert, the README story, the split, the gate check.

**Done when — Wed 4 Nov:** the report shows fights after 2026-05-16, the incremental load re-runs
cleanly, and the disagreement count is written up. **All three met 2026-10-29**; the gate also needs the
items added 2026-10-24 above.

---

## Phase 3 — Model · gate Sun 29 Nov

### Sprint 8 — Uge 11 (Mon 9 – Sun 15 Nov) · ~12 h

**Goal: ratings.**

- [ ] Re-run the baseline on the post-switch data; that is the number to beat from here on.
- [ ] Elo implementation, chronological, no leakage. Decide and document: K-factor, starting
      rating, handling of title fights and long layoffs.
- [ ] Rating history per fighter, spot-checked against fighters whose careers you know.

**Done when:** Elo ratings exist per fighter per date, and the top-10 list at a chosen historical
date looks plausible to someone who follows the sport.

---

### Sprint 9 — Uge 12 (Mon 16 – Sun 22 Nov) · ~12 h

**Goal: the real model.**

- [ ] Logistic regression on Elo differential + the contextual features from the baseline.
- [ ] Train/holdout split that respects time. No future data in training, ever.
- [ ] Compare against the baseline on the same holdout.

**Done when:** one table — baseline vs. full model, same holdout, log loss and accuracy.

---

### Sprint 10 — Uge 13 (Mon 23 – Sun 29 Nov) · ~14 h · **PHASE 3 GATE**

**Goal: published, with honest numbers.**

- [ ] Calibration curve + Brier score + log loss.
- [ ] Benchmark against closing odds. Expect to lose to the market; report the gap plainly.
- [ ] Start the public timestamped paper prediction log the day the model first runs.
- [ ] README tells the story end to end: architecture, the grain decision, the source switch and
      its disagreement count, the entity-resolution failures, baseline vs. model vs. market.
- [ ] Repo public.

**Done when — Sun 29 Nov:** the repo is public and the README contains real numbers, including the
unflattering ones.

---

## Cut lists — in this order

When a sprint runs over, cut from the top. Do not improvise a new order mid-sprint.

**Phase 1**
1. Fuzzy entity matching → exact + manual map only
2. Second Power BI page → one page
3. SCD2 on `dim_fighter` → SCD1, with the SCD2 design written up but not built

**Phase 2**
1. Full-history backfill from Cito → Kaggle stays the history
2. Overlap reconciliation → one event instead of several
3. Scheduled runs → manual re-run of the notebook

The silver contract, the incremental `MERGE` and the no-downstream-edits test are not cuttable:
they are the phase.

Below the last line of a list there is no phase left. If you reach it, the gate date was wrong —
and that is itself the finding worth writing down.

---

## Weekly review — 15 min, Sundays

Folds into the existing Life OS weekly review. Four questions:

1. Did this sprint's **Done when** actually happen? Yes or no. No partial credit.
2. If no — what gets cut, from the lists above?
3. Copy next sprint's tasks into `Todo.md` under the coming Uge.
4. Anything decided this week → one dated line in `decisions.md`.

---

## Parked — not scope, not scheduled

Lives in `decisions.md` with reconsider dates. Displaced from Phase 1 earlier and parked there:
RLS/masking demo · monitoring + one diagnosed pipeline failure.
