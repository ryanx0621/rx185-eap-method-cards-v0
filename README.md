# rx185-eap-method-cards-v0

Offline method package for ARC-AGI-3 Kaggle agents.

## Purpose

This package loads as `/kaggle/input/rx185-eap-method-cards-v0/` inside a Kaggle
competition notebook and provides:

- `cards/eap_core.md` — the always-on reasoning kernel (7 discipline rules)
- `cards/arc_agi3_protocol_cards.jsonl` — family-based action protocols (one card per family)
- `cards/first_principles_cards.jsonl` — meta-thinking patterns (12 cards)
- `cards/forbidden_actions.json` — explicit list of disallowed actions and shortcuts
- `manifest.json` — SHA256 per file for provenance verification

## What this package contains and does NOT contain

**Contains** (method level):
- discipline rules for hypothesis rivalry, observed_diff, source-first truth chain
- family-based exploration patterns (CLICK_ONLY_COUNTER, DIRECTIONAL_BFS, etc.)
- forbidden-action enumeration to prevent oracle leakage and replay-as-submission

**Does NOT contain**:
- per-game answer traces or gold traces
- cleaned_all dumps or SOLUTIONS_DIR contents
- specific game_id → action_sequence mappings
- API keys, credentials, hidden CoT, private chat memory

This boundary is enforced so the package is prize-eligible under ARC Prize 2026
open-source requirements.

## Usage in Kaggle Notebook

```python
from pathlib import Path
import json, hashlib

CARD_ROOT = Path("/kaggle/input/rx185-eap-method-cards-v0")
assert CARD_ROOT.exists(), "EAP/cards input not mounted"

manifest = json.loads((CARD_ROOT / "manifest.json").read_text())
print(f"manifest_sha = {hashlib.sha256((CARD_ROOT / 'manifest.json').read_bytes()).hexdigest()[:16]}")

eap_core = (CARD_ROOT / "cards" / "eap_core.md").read_text()

cards = []
for jsonl in sorted((CARD_ROOT / "cards").glob("*.jsonl")):
    cards.extend(json.loads(line) for line in jsonl.read_text().splitlines() if line.strip())

forbidden = json.loads((CARD_ROOT / "cards" / "forbidden_actions.json").read_text())
print(f"cards_loaded = {len(cards)}, forbidden_actions = {len(forbidden)}")
```

The companion notebook (`KAGGLE_R0_RX185_EAP_GEMMA4B_V6.ipynb`, V6P3 Qwen-ready)
auto-loads this package and prints `EAP_CARDS_MOUNTED = True` on success.

## License

Apache-2.0. See `LICENSE`.

## Versioning

`v0` = initial release, no canonical promotion. Patches will be appended as
`v1`, `v2` cards (additive) rather than rewritten in place, so downstream
consumers can reference exact card_id stability.
