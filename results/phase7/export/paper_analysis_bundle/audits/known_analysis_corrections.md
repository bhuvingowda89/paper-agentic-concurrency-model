# Known Analysis Corrections

- Phase 6 F6 validation initially recomputed RRR from per-operation replay booleans while `summary.json` used measured agent-attempt events. This was a validator-only defect. After correcting the validator to use the measured-event basis, preserved F6 artifacts validated and were restored to COMPLETE.
- Phase 7 recomputes RRR from measured `agent_attempt` events: replayed measured attempts divided by measured retry requests. This preserves the corrected Phase 6 definition and avoids the earlier analysis-only regression.
