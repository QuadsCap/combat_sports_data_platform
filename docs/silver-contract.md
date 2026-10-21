# Silver contract

The interface between source-specific cleaning (`20_clean_<source>`) and everything downstream.
Any source — Kaggle now, Cito or another in Phase 2 — must produce exactly these tables, columns
and types. Nothing downstream of silver reads bronze or knows source column names.

**Rules**
- Column names are ours, never a source's.
- Every row carries `source`.
- Natural keys are UFCStats IDs where the source has them, never names. Where it doesn't, the key is the source's own ID with a source prefix (e.g. `cito_13002`), and fighters are mapped to existing UFCStats IDs via `silv_fighter_xref` (Emil, 2026-10-21).
- Dates are `date`, not `timestamp`.
- Keys are not enforced by Delta — `20_clean_<source>` checks and counts them (see Checks).

---

## silv_fight

**Grain:** one row per fight
**Natural key:** `fight_id`

Names below are **proposed silver names** (ours); the bronze source is in brackets where it helps.

| column  | type  | nullable | description  |
| -------- | ------ | -------- | ---------------------------------------------------------------------- |
| fight_id | string | no  | Kaggle: UFCStats fight ID, parsed from `fight_url`. Cito: bout `id` prefixed `cito_` |
| event_id | string | no  | → `silv_event.event_id`; parsed from `event_url` |
| event_date | date | no | Convenience copy of `silv_event.event_date` (Emil, 2026-10-04) |
| f1_fighter_id | string | no | Corner 1 → `silv_fighter.fighter_id`; parsed from `f_1_fighter_url` |
| f2_fighter_id | string | no | Corner 2 → `silv_fighter.fighter_id`; parsed from `f_2_fighter_url` |
| outcome | string | no | `'win'` / `'draw'` / `'no_contest'` (Emil, 2026-10-06). Kaggle: `winner` matches a corner → win; `winner` = "No Contest" + Decision → draw; "No Contest" + Could Not Continue → no_contest |
| winner_fighter_id | string | yes | `f1_fighter_id` or `f2_fighter_id`: `winner` (a name) matched to `f_1_name` / `f_2_name` within the row. Null only when `outcome` ≠ `'win'`; a win with no name match is a data error (count it) |
| weight_class | string | no | [`weight_class`] normalised list (built in `20_clean_kaggle`) |
| gender | string | no | [`gender`] `'M'` / `'F'` |
| title_fight | boolean | no | [`title_fight`] |
| scheduled_rounds | int | yes | [`num_rounds`] 3 / 5 / 1 / 2; `0` (31 fights) → null |
| referee | string | yes | [`referee`] |
| method | string | no | [`result`] whitespace-collapsed + mapped: `Decision` / `KO/TKO` / `Submission` / `DQ` / `Could Not Continue` (Emil, 2026-10-06). "TKO - Doctor's Stoppage" → KO/TKO; "Decision - X" → Decision. Source does not split KO from TKO |
| decision_type | string | yes | `'Unanimous'` / `'Split'` / `'Majority'`; null = not a decision. Set on draws too (e.g. majority draw). Kaggle: suffix of `result` if present, else `result_details` (Emil, 2026-10-06) |
| method_detail | string | yes | [`result_details`] whitespace-collapsed, otherwise raw: finish technique + position, or judges' scorecards |
| finish_round | int | no | [`finish_round`] last round fought |
| finish_time_sec | int | no | [`finish_time`] `m:ss` → seconds into the finish round |
| f1_ranking | int | yes | [`f_1_ranking`] ranking at this fight (changes per fight for 22.6 % of fighters); type/meaning of nulls to confirm in cleaning |
| f2_ranking | int | yes | [`f_2_ranking`] |
| f1_odds_decimal | double | yes | [`f_1_odds_legacy`] decimal winner odds, corner 1 |
| f2_odds_decimal | double | yes | [`f_2_odds_legacy`] decimal winner odds, corner 2 |
| odds_source | string | yes | [`odds_source`] `'legacy'` / `'bfo'`; null = no odds |
| f1_bfo_best_decimal | double | yes | [`f_1_bfo_best_decimal`] best price across books, bfo rows only |
| f2_bfo_best_decimal | double | yes | [`f_2_bfo_best_decimal`] |
| bfo_bookmaker_count | int | yes | [`bfo_bookmaker_count`] |
| f1_ko_odds | double | yes | [`f_1_ko_odds`] format not yet checked |
| f2_ko_odds | double | yes | [`f_2_ko_odds`] format not yet checked |
| f1_sub_odds | double | yes | [`f_1_sub_odds`] format not yet checked |
| f2_sub_odds | double | yes | [`f_2_sub_odds`] format not yet checked |
| source  | string | no  | Which source produced the row, e.g. `'kaggle'` |

---

## silv_event

**Grain:** one row per event
**Natural key:** `event_id`

| column  | type  | nullable | description  |
| --------- | ------ | -------- | ------------------------------------------------- |
| event_id  | string | no  | UFCStats event ID, parsed from the event URL  |
| event_name | string | no | [`event_name`] |
| event_date | date | no | [`event_date`] cast to `date` |
| event_city | string | yes | [`event_city`] |
| event_state | string | yes | [`event_state`] null outside the US expected |
| event_country | string | yes | [`event_country`] |
| sport  | string | no  | e.g. 'MMA' (constant in Phase 1)  |
| promotion | string | no  | e.g. 'UFC' (constant in Phase 1)  |
| source  | string | no  | Which source produced the row, e.g. `'kaggle'`  |

---

## silv_round

**Grain:** one row per fight × round (Emil, 2026-10-04). Both corners' stats sit on the same row as
`f1_…` / `f2_…` column pairs.
**Natural key:** `fight_id` + `round` (composite — UFCStats has no round ID)

| column  | type  | nullable | description  |
| -------- | ------ | -------- | ---------------------------------------------------------------------- |
| fight_id | string | no  | → `silv_fight.fight_id`  |
| round | int | no | 1–5, from the `rN_` prefix during the unpivot |
| stats_missing | boolean | no | True when the round has no stats in the source (21 totals-only fights; 2 fights missing R2). Stat columns are null on these rows |
| f1_knockdowns | int | yes | [`f_1_rN_knockdowns`] |
| f1_sig_strikes_att | int | yes | [`f_1_rN_sig_strikes_att`] |
| f1_sig_strikes_succ | int | yes | [`f_1_rN_sig_strikes_succ`] |
| f1_total_strikes_att | int | yes | [`f_1_rN_total_strikes_att`] |
| f1_total_strikes_succ | int | yes | [`f_1_rN_total_strikes_succ`] |
| f1_td_att | int | yes | [`f_1_rN_td_1_att`] |
| f1_td_succ | int | yes | [`f_1_rN_td_1_succ`] |
| f1_submission_att | int | yes | [`f_1_rN_submission_att`] |
| f1_reversals | int | yes | [`f_1_rN_reversals`] |
| f1_ctrl_sec | int | yes | [`f_1_rN_ctrl`] seconds |
| f1_head_att | int | yes | [`f_1_rN_head_att`] |
| f1_head_succ | int | yes | [`f_1_rN_head_succ`] |
| f1_body_att | int | yes | [`f_1_rN_body_att`] |
| f1_body_succ | int | yes | [`f_1_rN_body_succ`] |
| f1_leg_att | int | yes | [`f_1_rN_leg_att`] |
| f1_leg_succ | int | yes | [`f_1_rN_leg_succ`] |
| f1_distance_att | int | yes | [`f_1_rN_distance_att`] |
| f1_distance_succ | int | yes | [`f_1_rN_distance_succ`] |
| f1_clinch_att | int | yes | [`f_1_rN_clinch_att`] |
| f1_clinch_succ | int | yes | [`f_1_rN_clinch_succ`] |
| f1_ground_att | int | yes | [`f_1_rN_ground_att`] |
| f1_ground_succ | int | yes | [`f_1_rN_ground_succ`] |
| f2_knockdowns | int | yes | [`f_2_rN_knockdowns`] |
| f2_sig_strikes_att | int | yes | [`f_2_rN_sig_strikes_att`] |
| f2_sig_strikes_succ | int | yes | [`f_2_rN_sig_strikes_succ`] |
| f2_total_strikes_att | int | yes | [`f_2_rN_total_strikes_att`] |
| f2_total_strikes_succ | int | yes | [`f_2_rN_total_strikes_succ`] |
| f2_td_att | int | yes | [`f_2_rN_td_1_att`] |
| f2_td_succ | int | yes | [`f_2_rN_td_1_succ`] |
| f2_submission_att | int | yes | [`f_2_rN_submission_att`] |
| f2_reversals | int | yes | [`f_2_rN_reversals`] |
| f2_ctrl_sec | int | yes | [`f_2_rN_ctrl`] seconds |
| f2_head_att | int | yes | [`f_2_rN_head_att`] |
| f2_head_succ | int | yes | [`f_2_rN_head_succ`] |
| f2_body_att | int | yes | [`f_2_rN_body_att`] |
| f2_body_succ | int | yes | [`f_2_rN_body_succ`] |
| f2_leg_att | int | yes | [`f_2_rN_leg_att`] |
| f2_leg_succ | int | yes | [`f_2_rN_leg_succ`] |
| f2_distance_att | int | yes | [`f_2_rN_distance_att`] |
| f2_distance_succ | int | yes | [`f_2_rN_distance_succ`] |
| f2_clinch_att | int | yes | [`f_2_rN_clinch_att`] |
| f2_clinch_succ | int | yes | [`f_2_rN_clinch_succ`] |
| f2_ground_att | int | yes | [`f_2_rN_ground_att`] |
| f2_ground_succ | int | yes | [`f_2_rN_ground_succ`] |
| source  | string | no  | Which source produced the row, e.g. `'kaggle'` |

---

## silv_fighter

**Grain:** one row per fighter
**Natural key:** `fighter_id`

| column  | type  | nullable | description  |
| ---------- | ------ | -------- | ---------------------------------------------------------------------- |
| fighter_id | string | no  | UFCStats fighter ID. Kaggle: parsed from `f_1_fighter_url` / `f_2_fighter_url` (both corners stacked, deduplicated). Cito: looked up in `silv_fighter_xref`; `cito_<uuid>` for fighters with no match (debuts after 16 May) |
| full_name | string | no | [`f_N_name`] e.g. 'Forrest Griffin' |
| first_name | string | yes | [`f_N_fighter_f_name`] |
| last_name | string | yes | [`f_N_fighter_l_name`] |
| nickname | string | yes | [`f_N_fighter_nickname`] |
| dob | date | yes | [`f_N_fighter_dob`] cast to `date` |
| height_cm | double | yes | [`f_N_fighter_height_cm`] |
| reach_cm | double | yes | [`f_N_fighter_reach_cm`] ~5.4 % null |
| stance | string | yes | [`f_N_fighter_stance`] |
| weight_lbs | double | yes | [`f_N_fighter_weight_lbs`] **current weight only** (0.0 % change per fighter) — not the weight at each fight; use `silv_fight.weight_class` for that |
| source  | string | no  | Which source produced the row, e.g. `'kaggle'` |

## silv_fighter_xref

**Grain:** one row per source fighter ID
**Natural key:** `source` + `source_fighter_id`
*Name and columns proposed by Claude, 2026-10-21; the approach is decided (Emil, same day).*

Crosswalk from a source's own fighter ID to silver `fighter_id`. Built by entity resolution (name/slug match against `silv_fighter`) and extended on each load.

| column  | type  | nullable | description  |
| ---------- | ------ | -------- | ---------------------------------------------------------------------- |
| source | string | no | e.g. `'cito'` |
| source_fighter_id | string | no | The source's ID, e.g. Cito fighter UUID |
| fighter_id | string | no | → `silv_fighter.fighter_id`: a UFCStats ID when matched, else `cito_<uuid>` |
| match_method | string | no | How the match was made, e.g. `'exact_name'` / `'fuzzy'` / `'manual'` / `'unmatched'` (feeds the failure log) |

---

## Relationships

- `silv_fight.event_id` → `silv_event.event_id` (one event, many fights)
- `silv_round.fight_id` → `silv_fight.fight_id` (one fight, many rounds)
- `silv_fight.f1_fighter_id` → `silv_fighter.fighter_id`
- `silv_fight.f2_fighter_id` → `silv_fighter.fighter_id`
- `silv_fighter_xref.fighter_id` → `silv_fighter.fighter_id` (one fighter, one ID per source)

## Column allocation (Emil, 2026-10-04 — base names; derivables moved out)

Base names = bronze column names with `f_1_`/`f_2_` and `r1_`…`r5_` stripped; the number is how
many bronze columns collapse into it (10 = 5 rounds × 2 corners; 12 = 10 + 2 fight totals;
2 = one per corner; 1 = fight-level). Real names: see the appendix.

**Rule (Emil, 2026-10-04): silver carries only stored facts; everything derivable is computed in gold.**

**silv_event:** `event_city`, `event_country`, `event_date`, `event_name`, `event_state`, `event_url`

**silv_fight:** `fight_url`, `winner` (fighter name → resolve to f1/f2 fighter ID within the row),
`weight_class`, `gender`, `title_fight`, `num_rounds`, `referee`, `result` (method), `result_details`,
`finish_round`, `finish_time`
- Odds (all kept — Emil, 2026-10-04): `odds_legacy` (2), `odds_source`, `bfo_best_decimal` (2),
  `bfo_bookmaker_count`, `ko_odds` (2), `sub_odds` (2)

**silv_round** (per round, per corner — the 10 `f_N_rN_` columns only):
`body_att`, `body_succ`, `clinch_att`, `clinch_succ`, `ctrl` (seconds), `distance_att`, `distance_succ`,
`ground_att`, `ground_succ`, `head_att`, `head_succ`, `knockdowns`, `leg_att`, `leg_succ`, `reversals`,
`sig_strikes_att`, `sig_strikes_succ`, `total_strikes_att`, `total_strikes_succ`, `submission_att`,
`td_1_att` (takedowns attempted), `td_1_succ` (takedowns landed)

**silv_fighter:** `fighter_url`, `name`, `fighter_f_name`, `fighter_l_name`, `fighter_nickname`,
`fighter_dob`, `fighter_height_cm`, `fighter_reach_cm`, `fighter_stance`, `fighter_weight_lbs`

**Leakage check (2026-10-04, 1,846 fighters with 3+ fights):** `ranking` changes per fight (22.6 %) →
`silv_fight` as `f1_ranking` / `f2_ranking`. `fighter_weight_lbs`, `height_cm`, `reach_cm`: 0.0 % change →
fighter attributes (weight = current only).

**Not carried into silver:**
- Bronze metadata: `_ingested_at`, `_load_id`, `_source_file`
- **Leakage** (0.0 % change per fighter = scrape-date snapshot, contains the future): `fighter_w`, `fighter_l`,
  `fighter_d`, `fighter_nc_dq`, `fighter_SApM`, `fighter_SlpM`, `fighter_Str_Acc`, `fighter_Str_Def`,
  `fighter_Sub_Avg`, `fighter_TD_Acc`, `fighter_TD_Avg`, `fighter_TD_Def`. Own as-of versions are built in gold.
- Duplicates: `url` (2) — `f_N_url` = `f_N_fighter_url` in 8,555/8,555 rows; `sig_pct_att`, `sig_pct_succ`
  (= `sig_strikes_att` / `_succ`)
- Derivable (computed in gold): `implied_prob` (2), `sig_pct` (10), `sig_strikes` (10 — a percentage,
  mislabelled), `td_2` (10 — takedown %), `ctrl_time_sec` (2), `takedown_att`, `takedown_succ` (2),
  and the fight-total columns of the count-12 stats (`f_N_knockdowns`, `f_N_reversals`,
  `f_N_sig_strikes_att/_succ`, `f_N_total_strikes_att/_succ`, `f_N_submission_att`)

## Open questions

### Grain and keys — decided 2026-10-04
- [x] `silv_round` grain: fight × round, corner stats as `f1_`/`f2_` column pairs (Emil). *Claude's reading of "one row, columns per round" — confirm.*
- [x] `silv_round` natural key: `fight_id` + `round`.
- [x] Fighter IDs in `silv_fight`: `f1_fighter_id`, `f2_fighter_id` (Emil: "F_1/F_2"; lowercase per convention).
- [x] `event_id` in `silv_fight` as FK to `silv_event`.
- [x] Keep `event_date` convenience copy in `silv_fight` (Emil).
- [x] Names: keep `full_name`, `first_name`, `last_name`, `nickname` as attributes. Never a key — identity is `fighter_id` (2026-10-04).

### Coverage edge cases
- [x] Totals-only fights (21): in `silv_fight` **and** `silv_round` (Emil, 2026-10-04). Round rows 1…`finish_round` with null stats and `stats_missing = true`. Their fight totals are not carried. *Claude: create rounds from `finish_round`, not an assumed 3.*
- [x] Fights missing R2 stats (2): keep existing rounds (Emil); the missing R2 row gets null stats + `stats_missing = true`.

### Reallocations — resolved 2026-10-04
- [x] `finish_round`, `finish_time` → `silv_fight`; `gender` → `silv_fight` (Emil moved them).
- [x] Derivables out of silver, computed in gold (Emil).
- [x] Leakage check done: records + career stats not carried; `ranking` → `silv_fight` (2026-10-04).

### Odds — decided 2026-10-04
- [x] Keep all odds columns in silver, incl. `ko_odds` / `sub_odds` (Emil: method and other markets are central to later analysis).
- [x] [fact] `odds_legacy` = decimal winner odds, filled on legacy + bfo rows (6,660 fights). `bfo_best_decimal` = best price across books, not closing. Odds timing (open/close) unknown → README limitation.
- [ ] Sourcing more odds data (BestFightOdds etc.) and a non-winner baseline: parked — Richer odds markets, reconsider 29 Nov.

### Column checks (run in `11_profile_kaggle`)

One row per bronze column (or group) that needs looking at before it is allocated. Names are the real
bronze names; `a` … `b` (n) means n columns from `a` to `b` — every one is listed in the inventory below. Most checks are a
`display(df.select(...).limit(20))` plus a null count or a `groupBy().count()` on distinct values.

| bronze columns (real names)                                                                                                                                                                                                                                                                                                                                  | check                                                                                     | why                                                              | Answer                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------ |
| `winner`                                                                                                                                                                                                                                                                                                                                                     | Contains a fighter URL/ID, a name, or 1/2? Nulls = draws/NC?                              | Decides how the outcome joins to fighters                        | Contains the fighters name           |
| `f_1_fighter_url`, `f_2_fighter_url`, `winner`                                                                                                                                                                                                                                                                                                               | Is `f_1` always red corner? Always the winner? (share of fights `f_1` wins)               | If `f_1` = winner → leakage; corner naming in the contract       | No, they are randomized. No leakage. |
| `result`                                                                                                                                                                                                                                                                                                                                                     | Distinct values                                                                           | Method of victory? Overlap with `result_details` / `winner`      | [fact] 12 raw values. Dirty: 4,213 rows wrapped in newlines + spaces (e.g. KO/TKO, Submission, DQ). Mixed granularity: `Decision` (3,698) vs `Decision - Unanimous/Split/Majority` (310). After trim: Decision*, KO/TKO, Submission, TKO - Doctor's Stoppage, DQ, Could Not Continue. No Draw / No Contest value. → trim + map to a method list in `20_clean_kaggle`. |
| `result_details`                                                                                                                                                                                                                                                                                                                                             | Distinct values (sample)                                                                  | Same as above; may hold the finer method (e.g. which submission) |                                      |
| `finish_round`                                                                                                                                                                                                                                                                                                                                               | Range, nulls; matches the last round with stats?                                          | Fight-level; cross-check vs. round data                          |                                      |
| `finish_time`                                                                                                                                                                                                                                                                                                                                                | Format (`m:ss` string? seconds?)                                                          | Type in the contract                                             | [fact] string `m:ss` (2:18, 4:15). [recommendation] store as int seconds in silver. |
| `num_rounds`                                                                                                                                                                                                                                                                                                                                                 | Distinct values (3/5? anything else?)                                                     | Scheduled rounds, not rounds fought                              | [fact] 3 (7,585), 5 (761), 1 (167), 0 (31), 2 (11). 0 is not a real scheduled-round count → check which events (likely early UFC, no round limit). |
| `title_fight`                                                                                                                                                                                                                                                                                                                                                | Type and distinct values (bool? 0/1? string?)                                             | Type in the contract                                             | [fact] boolean, no nulls: false 8,112 / true 443. |
| `weight_class`                                                                                                                                                                                                                                                                                                                                               | Distinct values; catchweights, women's divisions, spelling variants                       | Normalisation list for `20_clean_kaggle`                         |                                      |
| `gender`                                                                                                                                                                                                                                                                                                                                                     | Distinct values; does it match the women's weight classes?                                | Allocation: fight-level vs. fighter-level                        | [fact] M 7,646 / F 909, no nulls. One value per fight → `silv_fight`. |
| `referee`                                                                                                                                                                                                                                                                                                                                                    | Nulls; name spelling variants                                                             | Keep or drop                                                     |                                      |
| `f_1_ranking`, `f_2_ranking`                                                                                                                                                                                                                                                                                                                                 | What it holds, null rate, changes per fight for the same fighter?                         | Fight-level (as of the fight) vs. fighter-level                  |                                      |
| `f_1_url`, `f_2_url`, `f_1_fighter_url`, `f_2_fighter_url`                                                                                                                                                                                                                                                                                                   | Compare with `fighter_url` — same values?                                                 | Duplicate → drop                                                 | [fact] `f_1_url` = `f_1_fighter_url` in 8,555 / 8,555 rows → duplicate, drop `f_N_url`. (`f_2` not tested; confirm in cleaning.) |
| `f_1_name`, `f_2_name`, `f_1_fighter_f_name`, `f_2_fighter_f_name`, `f_1_fighter_l_name`, `f_2_fighter_l_name`                                                                                                                                                                                                                                               | Compare with `fighter_f_name` + `fighter_l_name`                                          | Duplicate → keep one                                             | [fact, 3-row sample] `f_1_name` = `f_1_fighter_f_name` + space + `f_1_fighter_l_name`. Duplicate → keep one. |
| `f_1_fighter_dob`, `f_2_fighter_dob`                                                                                                                                                                                                                                                                                                                         | Type, null rate, plausible range                                                          | Age-at-fight feature later                                       |                                      |
| `f_1_fighter_weight_lbs`, `f_2_fighter_weight_lbs`                                                                                                                                                                                                                                                                                                           | Same for one fighter across all fights?                                                   | Current-only value vs. per fight                                 |                                      |
| `f_1_fighter_height_cm`, `f_2_fighter_height_cm`, `f_1_fighter_reach_cm`, `f_2_fighter_reach_cm`                                                                                                                                                                                                                                                             | Same for one fighter across all fights?                                                   | Expected static; confirms fighter-level                          |                                      |
| `f_1_fighter_w`, `f_2_fighter_w`, `f_1_fighter_l`, `f_2_fighter_l`, `f_1_fighter_d`, `f_2_fighter_d`, `f_1_fighter_nc_dq`, `f_2_fighter_nc_dq`                                                                                                                                                                                                               | Same for one fighter across all fights?                                                   | If yes → scrape-date record → leakage, not a model input         |                                      |
| `f_1_fighter_SApM`, `f_2_fighter_SApM`, `f_1_fighter_SlpM`, `f_2_fighter_SlpM`, `f_1_fighter_Str_Acc`, `f_2_fighter_Str_Acc`, `f_1_fighter_Str_Def`, `f_2_fighter_Str_Def`, `f_1_fighter_Sub_Avg`, `f_2_fighter_Sub_Avg`, `f_1_fighter_TD_Acc`, `f_2_fighter_TD_Acc`, `f_1_fighter_TD_Avg`, `f_2_fighter_TD_Avg`, `f_1_fighter_TD_Def`, `f_2_fighter_TD_Def` | Same for one fighter across all fights?                                                   | If yes → scrape-date career stats → leakage, not a model input   |                                      |
| `f_1_r1_sig_strikes` … `f_2_r5_sig_strikes` (10), `f_1_r1_sig_strikes_att` … `f_2_sig_strikes_att` (12), `f_1_r1_sig_strikes_succ` … `f_2_sig_strikes_succ` (12)                                                                                                                                                                                             | `f_N_rN_sig_strikes` vs `_att` / `_succ` — what does each hold? (string like "12 of 30"?) | Duplicate or different stat                                      | [fact, 3-row sample] `f_N_rN_sig_strikes` is a **percentage** (75 = `sig_pct` 75.0), not a count — mislabelled. `_att` / `_succ` are the counts (bigint). Keep `_att`/`_succ`; drop `sig_strikes`. |
| `f_1_r1_sig_pct` … `f_2_r5_sig_pct` (10), `f_1_r1_sig_pct_att` … `f_2_r5_sig_pct_att` (10), `f_1_r1_sig_pct_succ` … `f_2_r5_sig_pct_succ` (10)                                                                                                                                                                                                               | What each holds; is `sig_pct` = succ/att?                                                 | Derivable → drop                                                 | [fact, 3-row sample] `sig_pct` = succ/att × 100. `sig_pct_att` = `sig_strikes_att` and `sig_pct_succ` = `sig_strikes_succ` in all 3 rows → duplicates/derivable → drop all three. (Confirm full-table equality in cleaning.) |
| `f_1_r1_ctrl` … `f_2_r5_ctrl` (10)                                                                                                                                                                                                                                                                                                                           | Format (seconds? `m:ss` string?)                                                          | Type in the contract                                             | [fact] bigint, **seconds** (124, 92, 105). |
| `f_1_ctrl_time_sec`, `f_2_ctrl_time_sec`, `f_1_r1_ctrl` … `f_2_r5_ctrl` (10)                                                                                                                                                                                                                                                                                 | = SUM of round `ctrl`?                                                                    | Derivable → drop                                                 | [fact] bigint seconds, **fight total**: 1-round fight r1 124 = total 124; another fight r1 92 vs total 327. Derivable → drop (except possibly for the 21 totals-only fights). |
| `f_1_r1_td_1_att` … `f_2_r5_td_1_att` (10), `f_1_r1_td_1_succ` … `f_2_r5_td_1_succ` (10), `f_1_r1_td_2` … `f_2_r5_td_2` (10)                                                                                                                                                                                                                                 | What each holds                                                                           | Unclear names; allocate after                                    | [fact, 3-row sample] `td_1_att` / `td_1_succ` = takedowns attempted / landed per round (bigint). `td_2` = takedown **%** (100, 50, 0) → derivable, drop. |
| `f_1_takedown_att`, `f_2_takedown_att`, `f_1_takedown_succ`, `f_2_takedown_succ`, `f_1_r1_td_1_att` … `f_2_r5_td_1_att` (10), `f_1_r1_td_1_succ` … `f_2_r5_td_1_succ` (10)                                                                                                                                                                                   | = SUM of round takedowns?                                                                 | Fight total → derivable → drop                                   | [fact] bigint, **fight totals** (r1 4 att vs total 6). Derivable → drop. |
| `f_1_r1_submission_att` … `f_2_submission_att` (12)                                                                                                                                                                                                                                                                                                          | Which 2 columns are not per-round; are they totals?                                       | Same pattern as the other count-12 stats                         |                                      |
| `f_1_knockdowns`, `f_2_knockdowns`, `f_1_reversals`, `f_2_reversals`, `f_1_sig_strikes_att`, `f_2_sig_strikes_att`, `f_1_sig_strikes_succ`, `f_2_sig_strikes_succ`, `f_1_total_strikes_att`, `f_2_total_strikes_att`, `f_1_total_strikes_succ`, `f_2_total_strikes_succ`, `f_1_submission_att`, `f_2_submission_att`                                         | Fight-total columns (no `rN_`): do they = SUM of the round columns?                       | Confirms totals are derivable before dropping them               |                                      |
| `f_1_odds_legacy`, `f_2_odds_legacy`                                                                                                                                                                                                                                                                                                                         | Format (decimal? American?), range, null rate by year                                     | Phase 3 benchmark                                                | [fact] **Decimal** odds (1.408, 3.05). Filled on both legacy and bfo rows (2022 bfo row: legacy 1.339 = bfo 1.339) → the merged winner-odds column. `odds_source`: legacy 5,530 + bfo 1,130 = 6,660 fights; NULL 1,895. `f_N_implied_prob` = 1/odds on legacy rows (1/1.408 = 0.710) but not on the bfo row (0.726 vs 1/1.339 = 0.747) → don't use it; derive our own. |
| `f_1_bfo_best_decimal`, `f_2_bfo_best_decimal`, `f_1_odds_legacy`, `f_2_odds_legacy`, `bfo_bookmaker_count`                                                                                                                                                                                                                                                  | Closing line or best available? Compare with `odds_legacy` for a few fights               | Pick the benchmark odds                                          | [fact] Only filled on bfo rows (sample: 7 bookmakers). 'Best decimal' = best price across books, not a closing consensus line. Whether `legacy` is opening or closing is not in the data. [recommendation] Benchmark on `f_N_odds_legacy` + `odds_source`; state in the README that odds timing is unknown. |
| `odds_source`                                                                                                                                                                                                                                                                                                                                                | Distinct values                                                                           | Which bookmaker/source the odds come from                        | [fact] legacy 5,530 / bfo 1,130 / NULL 1,895. |
| `event_date`                                                                                                                                                                                                                                                                                                                                                 | Type (timestamp?) → contract says `date`                                                  | Cast in `20_clean_kaggle`                                        |                                      |
| `event_url`, `fight_url`, `f_1_fighter_url`, `f_2_fighter_url`                                                                                                                                                                                                                                                                                               | ID parses cleanly from every URL? Same format throughout?                                 | Natural keys                                                     | [fact] All four: 0 nulls, 0 bad format. `fight_url` 8,555 distinct = row count → fight ID unique per row (no duplicate fights). `event_url`: 774 distinct events. Fighters: 1,974 distinct in `f_1`, 2,510 in `f_2`. |

## Checks (run in `20_clean_<source>`, counts recorded)

-Duplicates on each table's natural key: <count>
- `silv_round` rows with no matching `silv_fight`: <count>
- `silv_fight` fighter IDs with no matching `silv_fighter`: <count>

## Appendix: bronze column inventory (`bronze_kaggle_full_data_silver_plus`, 367 source columns)

Real bronze names, grouped by base name (corner `f_1_`/`f_2_` and round `r1_`…`r5_` stripped). Excludes the three bronze metadata columns `_ingested_at`, `_load_id`, `_source_file`.

- **bfo_best_decimal** (2): `f_1_bfo_best_decimal`, `f_2_bfo_best_decimal`
- **bfo_bookmaker_count** (1): `bfo_bookmaker_count`
- **body_att** (10): `f_1_r1_body_att`, `f_1_r2_body_att`, `f_1_r3_body_att`, `f_1_r4_body_att`, `f_1_r5_body_att`, `f_2_r1_body_att`, `f_2_r2_body_att`, `f_2_r3_body_att`, `f_2_r4_body_att`, `f_2_r5_body_att`
- **body_succ** (10): `f_1_r1_body_succ`, `f_1_r2_body_succ`, `f_1_r3_body_succ`, `f_1_r4_body_succ`, `f_1_r5_body_succ`, `f_2_r1_body_succ`, `f_2_r2_body_succ`, `f_2_r3_body_succ`, `f_2_r4_body_succ`, `f_2_r5_body_succ`
- **clinch_att** (10): `f_1_r1_clinch_att`, `f_1_r2_clinch_att`, `f_1_r3_clinch_att`, `f_1_r4_clinch_att`, `f_1_r5_clinch_att`, `f_2_r1_clinch_att`, `f_2_r2_clinch_att`, `f_2_r3_clinch_att`, `f_2_r4_clinch_att`, `f_2_r5_clinch_att`
- **clinch_succ** (10): `f_1_r1_clinch_succ`, `f_1_r2_clinch_succ`, `f_1_r3_clinch_succ`, `f_1_r4_clinch_succ`, `f_1_r5_clinch_succ`, `f_2_r1_clinch_succ`, `f_2_r2_clinch_succ`, `f_2_r3_clinch_succ`, `f_2_r4_clinch_succ`, `f_2_r5_clinch_succ`
- **ctrl** (10): `f_1_r1_ctrl`, `f_1_r2_ctrl`, `f_1_r3_ctrl`, `f_1_r4_ctrl`, `f_1_r5_ctrl`, `f_2_r1_ctrl`, `f_2_r2_ctrl`, `f_2_r3_ctrl`, `f_2_r4_ctrl`, `f_2_r5_ctrl`
- **ctrl_time_sec** (2): `f_1_ctrl_time_sec`, `f_2_ctrl_time_sec`
- **distance_att** (10): `f_1_r1_distance_att`, `f_1_r2_distance_att`, `f_1_r3_distance_att`, `f_1_r4_distance_att`, `f_1_r5_distance_att`, `f_2_r1_distance_att`, `f_2_r2_distance_att`, `f_2_r3_distance_att`, `f_2_r4_distance_att`, `f_2_r5_distance_att`
- **distance_succ** (10): `f_1_r1_distance_succ`, `f_1_r2_distance_succ`, `f_1_r3_distance_succ`, `f_1_r4_distance_succ`, `f_1_r5_distance_succ`, `f_2_r1_distance_succ`, `f_2_r2_distance_succ`, `f_2_r3_distance_succ`, `f_2_r4_distance_succ`, `f_2_r5_distance_succ`
- **event_city** (1): `event_city`
- **event_country** (1): `event_country`
- **event_date** (1): `event_date`
- **event_name** (1): `event_name`
- **event_state** (1): `event_state`
- **event_url** (1): `event_url`
- **fight_url** (1): `fight_url`
- **fighter_d** (2): `f_1_fighter_d`, `f_2_fighter_d`
- **fighter_dob** (2): `f_1_fighter_dob`, `f_2_fighter_dob`
- **fighter_f_name** (2): `f_1_fighter_f_name`, `f_2_fighter_f_name`
- **fighter_height_cm** (2): `f_1_fighter_height_cm`, `f_2_fighter_height_cm`
- **fighter_l** (2): `f_1_fighter_l`, `f_2_fighter_l`
- **fighter_l_name** (2): `f_1_fighter_l_name`, `f_2_fighter_l_name`
- **fighter_nc_dq** (2): `f_1_fighter_nc_dq`, `f_2_fighter_nc_dq`
- **fighter_nickname** (2): `f_1_fighter_nickname`, `f_2_fighter_nickname`
- **fighter_reach_cm** (2): `f_1_fighter_reach_cm`, `f_2_fighter_reach_cm`
- **fighter_SApM** (2): `f_1_fighter_SApM`, `f_2_fighter_SApM`
- **fighter_SlpM** (2): `f_1_fighter_SlpM`, `f_2_fighter_SlpM`
- **fighter_stance** (2): `f_1_fighter_stance`, `f_2_fighter_stance`
- **fighter_Str_Acc** (2): `f_1_fighter_Str_Acc`, `f_2_fighter_Str_Acc`
- **fighter_Str_Def** (2): `f_1_fighter_Str_Def`, `f_2_fighter_Str_Def`
- **fighter_Sub_Avg** (2): `f_1_fighter_Sub_Avg`, `f_2_fighter_Sub_Avg`
- **fighter_TD_Acc** (2): `f_1_fighter_TD_Acc`, `f_2_fighter_TD_Acc`
- **fighter_TD_Avg** (2): `f_1_fighter_TD_Avg`, `f_2_fighter_TD_Avg`
- **fighter_TD_Def** (2): `f_1_fighter_TD_Def`, `f_2_fighter_TD_Def`
- **fighter_url** (2): `f_1_fighter_url`, `f_2_fighter_url`
- **fighter_w** (2): `f_1_fighter_w`, `f_2_fighter_w`
- **fighter_weight_lbs** (2): `f_1_fighter_weight_lbs`, `f_2_fighter_weight_lbs`
- **finish_round** (1): `finish_round`
- **finish_time** (1): `finish_time`
- **gender** (1): `gender`
- **ground_att** (10): `f_1_r1_ground_att`, `f_1_r2_ground_att`, `f_1_r3_ground_att`, `f_1_r4_ground_att`, `f_1_r5_ground_att`, `f_2_r1_ground_att`, `f_2_r2_ground_att`, `f_2_r3_ground_att`, `f_2_r4_ground_att`, `f_2_r5_ground_att`
- **ground_succ** (10): `f_1_r1_ground_succ`, `f_1_r2_ground_succ`, `f_1_r3_ground_succ`, `f_1_r4_ground_succ`, `f_1_r5_ground_succ`, `f_2_r1_ground_succ`, `f_2_r2_ground_succ`, `f_2_r3_ground_succ`, `f_2_r4_ground_succ`, `f_2_r5_ground_succ`
- **head_att** (10): `f_1_r1_head_att`, `f_1_r2_head_att`, `f_1_r3_head_att`, `f_1_r4_head_att`, `f_1_r5_head_att`, `f_2_r1_head_att`, `f_2_r2_head_att`, `f_2_r3_head_att`, `f_2_r4_head_att`, `f_2_r5_head_att`
- **head_succ** (10): `f_1_r1_head_succ`, `f_1_r2_head_succ`, `f_1_r3_head_succ`, `f_1_r4_head_succ`, `f_1_r5_head_succ`, `f_2_r1_head_succ`, `f_2_r2_head_succ`, `f_2_r3_head_succ`, `f_2_r4_head_succ`, `f_2_r5_head_succ`
- **implied_prob** (2): `f_1_implied_prob`, `f_2_implied_prob`
- **knockdowns** (12): `f_1_knockdowns`, `f_1_r1_knockdowns`, `f_1_r2_knockdowns`, `f_1_r3_knockdowns`, `f_1_r4_knockdowns`, `f_1_r5_knockdowns`, `f_2_knockdowns`, `f_2_r1_knockdowns`, `f_2_r2_knockdowns`, `f_2_r3_knockdowns`, `f_2_r4_knockdowns`, `f_2_r5_knockdowns`
- **ko_odds** (2): `f_1_ko_odds`, `f_2_ko_odds`
- **leg_att** (10): `f_1_r1_leg_att`, `f_1_r2_leg_att`, `f_1_r3_leg_att`, `f_1_r4_leg_att`, `f_1_r5_leg_att`, `f_2_r1_leg_att`, `f_2_r2_leg_att`, `f_2_r3_leg_att`, `f_2_r4_leg_att`, `f_2_r5_leg_att`
- **leg_succ** (10): `f_1_r1_leg_succ`, `f_1_r2_leg_succ`, `f_1_r3_leg_succ`, `f_1_r4_leg_succ`, `f_1_r5_leg_succ`, `f_2_r1_leg_succ`, `f_2_r2_leg_succ`, `f_2_r3_leg_succ`, `f_2_r4_leg_succ`, `f_2_r5_leg_succ`
- **name** (2): `f_1_name`, `f_2_name`
- **num_rounds** (1): `num_rounds`
- **odds_legacy** (2): `f_1_odds_legacy`, `f_2_odds_legacy`
- **odds_source** (1): `odds_source`
- **ranking** (2): `f_1_ranking`, `f_2_ranking`
- **referee** (1): `referee`
- **result** (1): `result`
- **result_details** (1): `result_details`
- **reversals** (12): `f_1_r1_reversals`, `f_1_r2_reversals`, `f_1_r3_reversals`, `f_1_r4_reversals`, `f_1_r5_reversals`, `f_1_reversals`, `f_2_r1_reversals`, `f_2_r2_reversals`, `f_2_r3_reversals`, `f_2_r4_reversals`, `f_2_r5_reversals`, `f_2_reversals`
- **sig_pct** (10): `f_1_r1_sig_pct`, `f_1_r2_sig_pct`, `f_1_r3_sig_pct`, `f_1_r4_sig_pct`, `f_1_r5_sig_pct`, `f_2_r1_sig_pct`, `f_2_r2_sig_pct`, `f_2_r3_sig_pct`, `f_2_r4_sig_pct`, `f_2_r5_sig_pct`
- **sig_pct_att** (10): `f_1_r1_sig_pct_att`, `f_1_r2_sig_pct_att`, `f_1_r3_sig_pct_att`, `f_1_r4_sig_pct_att`, `f_1_r5_sig_pct_att`, `f_2_r1_sig_pct_att`, `f_2_r2_sig_pct_att`, `f_2_r3_sig_pct_att`, `f_2_r4_sig_pct_att`, `f_2_r5_sig_pct_att`
- **sig_pct_succ** (10): `f_1_r1_sig_pct_succ`, `f_1_r2_sig_pct_succ`, `f_1_r3_sig_pct_succ`, `f_1_r4_sig_pct_succ`, `f_1_r5_sig_pct_succ`, `f_2_r1_sig_pct_succ`, `f_2_r2_sig_pct_succ`, `f_2_r3_sig_pct_succ`, `f_2_r4_sig_pct_succ`, `f_2_r5_sig_pct_succ`
- **sig_strikes** (10): `f_1_r1_sig_strikes`, `f_1_r2_sig_strikes`, `f_1_r3_sig_strikes`, `f_1_r4_sig_strikes`, `f_1_r5_sig_strikes`, `f_2_r1_sig_strikes`, `f_2_r2_sig_strikes`, `f_2_r3_sig_strikes`, `f_2_r4_sig_strikes`, `f_2_r5_sig_strikes`
- **sig_strikes_att** (12): `f_1_r1_sig_strikes_att`, `f_1_r2_sig_strikes_att`, `f_1_r3_sig_strikes_att`, `f_1_r4_sig_strikes_att`, `f_1_r5_sig_strikes_att`, `f_1_sig_strikes_att`, `f_2_r1_sig_strikes_att`, `f_2_r2_sig_strikes_att`, `f_2_r3_sig_strikes_att`, `f_2_r4_sig_strikes_att`, `f_2_r5_sig_strikes_att`, `f_2_sig_strikes_att`
- **sig_strikes_succ** (12): `f_1_r1_sig_strikes_succ`, `f_1_r2_sig_strikes_succ`, `f_1_r3_sig_strikes_succ`, `f_1_r4_sig_strikes_succ`, `f_1_r5_sig_strikes_succ`, `f_1_sig_strikes_succ`, `f_2_r1_sig_strikes_succ`, `f_2_r2_sig_strikes_succ`, `f_2_r3_sig_strikes_succ`, `f_2_r4_sig_strikes_succ`, `f_2_r5_sig_strikes_succ`, `f_2_sig_strikes_succ`
- **sub_odds** (2): `f_1_sub_odds`, `f_2_sub_odds`
- **submission_att** (12): `f_1_r1_submission_att`, `f_1_r2_submission_att`, `f_1_r3_submission_att`, `f_1_r4_submission_att`, `f_1_r5_submission_att`, `f_1_submission_att`, `f_2_r1_submission_att`, `f_2_r2_submission_att`, `f_2_r3_submission_att`, `f_2_r4_submission_att`, `f_2_r5_submission_att`, `f_2_submission_att`
- **takedown_att** (2): `f_1_takedown_att`, `f_2_takedown_att`
- **takedown_succ** (2): `f_1_takedown_succ`, `f_2_takedown_succ`
- **td_1_att** (10): `f_1_r1_td_1_att`, `f_1_r2_td_1_att`, `f_1_r3_td_1_att`, `f_1_r4_td_1_att`, `f_1_r5_td_1_att`, `f_2_r1_td_1_att`, `f_2_r2_td_1_att`, `f_2_r3_td_1_att`, `f_2_r4_td_1_att`, `f_2_r5_td_1_att`
- **td_1_succ** (10): `f_1_r1_td_1_succ`, `f_1_r2_td_1_succ`, `f_1_r3_td_1_succ`, `f_1_r4_td_1_succ`, `f_1_r5_td_1_succ`, `f_2_r1_td_1_succ`, `f_2_r2_td_1_succ`, `f_2_r3_td_1_succ`, `f_2_r4_td_1_succ`, `f_2_r5_td_1_succ`
- **td_2** (10): `f_1_r1_td_2`, `f_1_r2_td_2`, `f_1_r3_td_2`, `f_1_r4_td_2`, `f_1_r5_td_2`, `f_2_r1_td_2`, `f_2_r2_td_2`, `f_2_r3_td_2`, `f_2_r4_td_2`, `f_2_r5_td_2`
- **title_fight** (1): `title_fight`
- **total_strikes_att** (12): `f_1_r1_total_strikes_att`, `f_1_r2_total_strikes_att`, `f_1_r3_total_strikes_att`, `f_1_r4_total_strikes_att`, `f_1_r5_total_strikes_att`, `f_1_total_strikes_att`, `f_2_r1_total_strikes_att`, `f_2_r2_total_strikes_att`, `f_2_r3_total_strikes_att`, `f_2_r4_total_strikes_att`, `f_2_r5_total_strikes_att`, `f_2_total_strikes_att`
- **total_strikes_succ** (12): `f_1_r1_total_strikes_succ`, `f_1_r2_total_strikes_succ`, `f_1_r3_total_strikes_succ`, `f_1_r4_total_strikes_succ`, `f_1_r5_total_strikes_succ`, `f_1_total_strikes_succ`, `f_2_r1_total_strikes_succ`, `f_2_r2_total_strikes_succ`, `f_2_r3_total_strikes_succ`, `f_2_r4_total_strikes_succ`, `f_2_r5_total_strikes_succ`, `f_2_total_strikes_succ`
- **url** (2): `f_1_url`, `f_2_url`
- **weight_class** (1): `weight_class`
- **winner** (1): `winner`
