# Build Plan — Combat Sports Data Platform

*v1.0 · 2026-09-20 · Owner: Emil*

> Scope lives in `project-definition.md`. This file is only **what gets built which week**.
> Sprints are calendar weeks aligned to the `Todo.md` Uge numbering (Mon–Sun), so next actions
> drop straight into the existing weekly blocks. No parallel tracking system.

---

## Confirmed — 2026-09-20

**Gate dates: Phase 1 = Sat 17 Oct · Phase 2 = Sun 8 Nov.**
**Committed capacity: 15 h/week — 3 h x 5 weekdays.** Weekends free.

Chosen over a nominal 4-5 h/day: the job search, training and drawing run concurrently, and hours
spent on unfamiliar technology burn roughly double. Sprints below are sized 12-14 h, so each fits
its week with slack.

Hours above 15 in a good week buy **buffer, not an earlier gate.** Sprint 3 (entity resolution)
and Sprint 4 (Power BI, a tool not yet opened) are where the buffer gets spent.

The date is a ceiling, not a target. Finish early and the gate lands early. Extending is not an
option -- scope gets cut instead, from the cut list below.

---

## The spine

If everything else is cut, this still has to exist by the Phase 1 gate:

> **Kaggle file → bronze table → clean silver table → `fact_round` + `dim_fighter` → one Power BI page.**

Everything below is that spine plus the parts that make it portfolio-grade.

---

## Sprint 0 — today (Sun 20 Sep) · ~2–3 h

The two things that unblock every later sprint. Neither involves writing pipeline code.

- [ ] Connect Fabric workspace ↔ GitHub repo (Workspace settings → Git integration).
- [ ] Make a trivial change in Fabric, sync it, confirm the commit lands in GitHub. Then change a
      file in GitHub, sync down, confirm it appears in Fabric. **Both directions.** This is the
      whole of v1 CI/CD — do not build more than this.
- [ ] Kaggle: sample-download the per-round dataset. Open it. Look at it.

**Done when:** a commit made in Fabric is visible in GitHub, and a CSV is on disk.

---

## Sprint 1 — Uge 4 (Mon 21 – Sun 27 Sep) · ~12 h

**Goal: raw data is in Fabric and you know what it actually contains.**

- [ ] `10_ingest_kaggle` — land the raw files into the lakehouse as `bronze_*`. Raw and immutable:
      no cleaning, no renaming, no type casting. Add ingest timestamp + source filename columns.
- [ ] **Coverage note** → `docs/data-coverage.md`: date range, fight count, round count, how many
      fights have per-round detail vs. only totals, how many have odds, null rates on the columns
      that matter. Plain numbers.
- [ ] Write down the first three things that look wrong. They will all matter later.

**Done when:** `bronze_*` tables are queryable in Fabric, and `data-coverage.md` answers "can this
dataset support a `fact_round` grain at all?" with a yes/no and the numbers behind it.

**Concept this sprint:** why bronze stays immutable, and what an incremental-ready design means
while you are still doing full loads.

---

## Sprint 2 — Uge 5 (Mon 28 Sep – Sun 4 Oct) · ~12 h

**Goal: clean, typed, deduplicated silver.**

- [ ] `20_clean_fights` — types, dates parsed, weight classes normalised, method-of-victory
      standardised, obvious junk rows dropped **and counted**.
- [ ] Dedup: identify the natural key for a fight and for a round. Count what dedup removes.
- [ ] Reconcile row counts bronze → silver. Every dropped row explained by a number.

**Done when:** `silv_fight` and `silv_round` exist, and you can state bronze→silver row counts
with a one-line reason for each delta.

**Cut first if short on time:** late-arriving-data handling → v2.

---

## Sprint 3 — Uge 6 (Mon 5 – Sun 11 Oct) · ~14 h · hardest sprint

**Goal: fighters resolve to one identity, and the dimensions exist.**

- [ ] Entity resolution: exact match → normalised match → fuzzy match → manual override map.
      Deterministic first, always. `src/` holds the helpers.
- [ ] **Keep the failure log.** Every name the fuzzy pass got wrong, and what caught it. Per
      CLAUDE.md this is the portfolio differentiator, not an appendix.
- [ ] `dim_fighter` as SCD2 — valid_from / valid_to / is_current.
- [ ] `dim_event`, `dim_date`.

**Done when:** a fighter with known name variants resolves to one `fighter_key`, and one fighter
shows two SCD2 rows with correct validity windows.

**Cut first:** fuzzy matching entirely — ship exact + manual map, and say so honestly.

---

## Sprint 4 — Uge 7 (Mon 12 – Sat 17 Oct) · ~14 h · **PHASE 1 GATE**

**Goal: end-to-end. A human opens a report and learns something.**

- [ ] `30_warehouse_fact_round` — the fact table, keys wired to the dimensions.
- [ ] `40_model_baseline` — win/loss record, age, age gap, layoff, weight class, reach/height
      differential. Logistic regression, no tuning. Record accuracy and log loss. **This is the
      number Phase 2 has to beat.**
- [ ] **Power BI** — budget ~5 h of this sprint, learning included. One report, two pages:
      descriptive trends (win rate by age band across weight classes, finish rate by weight class)
      and baseline model output.
- [ ] Update the README so a stranger understands the project in 60 seconds.

**Done when — Sat 17 Oct:** a Power BI report opens on the gold tables and shows real numbers, and
the whole path Kaggle → report can be re-run. If it does not run end-to-end, **scope gets cut, not
the date.**

**Explicitly deferred out of Phase 1** — was in definition v1.0, displaced by the report and the
baseline: RLS/masking demo · monitoring + one diagnosed pipeline failure. Both move to Sprint 5–7
slack, or v2.

---

## Sprint 5 — Uge 8 (Mon 19 – Sun 25 Oct) · ~12 h

**Goal: ratings.**

- [ ] Elo implementation, chronological, no leakage. Decide and document: K-factor, starting
      rating, handling of title fights and long layoffs.
- [ ] Rating history per fighter, spot-checked against fighters whose careers you know.

**Done when:** Elo ratings exist per fighter per date, and the top-10 list at a chosen historical
date looks plausible to someone who follows the sport.

---

## Sprint 6 — Uge 9 (Mon 26 Oct – Sun 1 Nov) · ~12 h

**Goal: the real model.**

- [ ] Logistic regression on Elo differential + the contextual features from the baseline.
- [ ] Train/holdout split that respects time. No future data in training, ever.
- [ ] Compare against the Sprint 4 baseline on the same holdout.

**Done when:** one table — baseline vs. full model, same holdout, log loss and accuracy.

---

## Sprint 7 — Uge 10 (Mon 2 – Sun 8 Nov) · ~14 h · **PHASE 2 GATE**

**Goal: published, with honest numbers.**

- [ ] Calibration curve + Brier score + log loss.
- [ ] Benchmark against closing odds. Expect to lose to the market; report the gap plainly.
- [ ] Start the public timestamped paper prediction log the day the model first runs.
- [ ] README tells the story end to end: architecture, the grain decision, the entity-resolution
      failures, baseline vs. model vs. market.
- [ ] Repo public.

**Done when — Sun 8 Nov:** the repo is public and the README contains real numbers, including the
unflattering ones.

---

## Cut list — in this order

When a sprint runs over, cut from the top. Do not improvise a new order mid-sprint.

1. Late-arriving-data handling → v2
2. RLS/masking demo → v2
3. Monitoring + diagnosed pipeline failure → v2
4. Fuzzy entity matching → exact + manual map only
5. Second Power BI page → one page
6. SCD2 on `dim_fighter` → SCD1, with the SCD2 design written up but not built

Below line 6 there is no project left. If you reach it, the gate date was wrong — and that is
itself the finding worth writing down.

---

## Weekly review — 15 min, Sundays

Folds into the existing Life OS weekly review. Four questions:

1. Did this sprint's **Done when** actually happen? Yes or no. No partial credit.
2. If no — what gets cut, from the list above?
3. Copy next sprint's tasks into `Todo.md` under the coming Uge.
4. Anything decided this week → one dated line in `decisions.md`.

---

## Open questions — not scope, not scheduled

Raised 2026-09-20, parked in `decisions.md` with reconsider dates:

- **Other organisations' data** (ONE FC, PFL, Bellator) — reconsider at Phase 2 completion.
- **Source reliability / cross-source reconciliation** — Phase 1 does the descriptive version only.
- **Blog / written documentation of the analysis** — the README *is* v1 of this. Reconsider as a
  separate artifact once Phase 2 ships.
