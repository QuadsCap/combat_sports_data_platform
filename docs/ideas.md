# Ideas & Useful

A capture list, not a backlog. Nothing here is in scope until it gets a dated entry in
`decisions.md` (a phase, or the Parked list with a reconsider date).

---

## Ideas

### Analysis
*Format: compare X against Y, and what it would show.*

- Model calibration on UFC debut fights vs. non-debut fights (already planned for Phase 3, decisions.md 2026-09-24).
- Corner (`f1` / `f2`, likely red / blue) against win rate. Does corner assignment carry favourite information?
- Model probability vs. market odds by year, weight class and gender. Where is the market most or least efficient?
- Method of victory (KO/TKO, Submission, Decision) vs. KO / sub odds. Needs the richer odds markets (Parked, 29 Nov).
- UFC-only history vs. UFC + other organisations. Does more history improve calibration on a UFC holdout? (Parked, 29 Nov.)
- Younger vs. older fighter win rate, cut by weight class (raised 2026-10-11). Does the younger fighter win more often, always or only sometimes? Does the edge vary across weight classes, and if so, why, and do patterns emerge (e.g. by size, gender, age-gap size)?
  - *Claude perspective:*
    - Sample size is the main risk. Small cells: women's divisions, heavyweight, and any class × age-gap bucket. Report n and a confidence interval (Wilson) per cell; drop or merge cells below a minimum n.
    - Many cells means some will look significant by chance (multiple comparisons). Better than a matrix of separate tests: one logistic model, `win ~ age_diff × weight_class`, or shrink each class towards the overall effect.
    - Use age at fight date (as-of), not today's age. Exclude fights where either `dob` is missing, and check that missingness is not concentrated in recent years (see decisions.md 2026-10-11).
    - Candidate explanations to test, not assume: lighter classes rely more on speed and cardio, which decline earlier; power holds longer, which would make the heavyweight effect smaller; selection (older fighters still active are the survivors); age gap overlapping with experience gap.
    - Fits the Sprint 8 feature brainstorm (Parked list): if the effect varies by class, an `age_diff × weight_class` interaction is a candidate model feature.

### Data platform

- 

### Business

- 

---

## Useful

- [agency-agents](https://github.com/msitarzewski/agency-agents): setting up a business run with AI agents.


