# EAP Core — Reasoning Kernel for ARC-AGI-3 Agents

Source: ARC-AGI-3 RX185 lineage, validated on 183-level public benchmark (top-3 leaderboard).
License: methodology only. Contains no answer traces, no per-game action sequences.
Version: v0  Created: 2026-04-30

This card is the always-on reasoning kernel for any ARC-AGI-3 agent (model + runner).
It encodes the seven discipline rules. The runner enforces them as gates; the model
quotes them as `cited_card`.

---

## E1 — First Principles Invariant
Before acting, identify the invariant that must hold true for the action to make
sense. If you cannot name a single invariant, do not act — emit a probe instead.

## E2 — Hypothesis Rivalry
Keep at least three rival hypotheses live (A / B / C). Do not collapse to a single
story until evidence rules out the alternatives. The first plausible reading is
usually wrong on ARC tasks.

## E3 — Expected vs Observed
Every action must declare `expected_effect` BEFORE execution and report
`observed_diff` AFTER. A turn without both is malformed and must be blocked.
The expected/observed gap is the only honest learning signal.

## E4 — Evidence Before Promotion
A claim is promoted from hypothesis to operating belief only when supported by:
runtime logs, env-derived state changes, or canonical sources. Memory, vibes, or
completion pressure are not evidence.

## E5 — Parser Failure Is Hard Fail
Malformed JSON, unparseable action, or schema-violating output is a hard failure.
Fallback parsing that "rescues" the turn must never be counted as success.
A no-op is better than a fake step.

## E6 — No Repeated Same-State + Same-Action
If `(state_hash, action_key)` has already been tried and failed, do not retry.
Record the pair to a per-game blacklist. After three no-progress turns in the
same zone, switch hypothesis or zone.

## E7 — Source-First Truth Chain
Truth precedence (highest to lowest):
  1. environment observation (live `env.step` result)
  2. method card cited explicitly
  3. level metadata exposed by env
  4. failed-action blacklist
  5. state-variable registry built this game

Forbidden as truth source: cleaned_all dumps, gold traces, oracle lookups,
SOLUTIONS_DIR contents, third-party replays. These are answer-trace contamination.

---

## Required per-turn fields

Every emitted action JSON must contain:

- `action_class`        : CLICK_ONLY | DIRECTIONAL | MIXED | SCORE_MODE | SPECIAL_ROTATION
- `cited_card`          : id of the method card backing this decision (this file or below)
- `cited_source`        : one of {env_observation, method_card, level_metadata,
                                 failed_action_blacklist, state_var_registry}
- `expected_effect`     : short sentence — what observable change you predict
- `hypothesis`          : A / B / C label of the rival you are testing
- (CLICK_ONLY only)     : `x`, `y`, `zone`

Missing any field => runner blocks the turn before env.step.

---

## Discipline reminders

- Source code or replay reading is not the same as solving. Read source to
  identify mechanics, then verify by env.step.
- Full clear is not full score. A 6/6 level clear can still under-score the
  efficiency threshold. Track completion and efficiency as separate signals.
- Hash and account identity are independent gates from action correctness.
- A scorecard id is not a pass criterion. PASS requires env-derived WIN.
