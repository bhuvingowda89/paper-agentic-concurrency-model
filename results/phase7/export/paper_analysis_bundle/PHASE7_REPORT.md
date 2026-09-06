# Phase 7 Analysis Report

## A. Dataset integrity

- Expected experiments: 3340
- Valid experiments analyzed: 3340
- Excluded experiments: 0
- Final Phase 6 ledger: 3340 COMPLETE, 0 pending/running/infra retry/invalidated/failed blocked
- Missing data: 0 missing experiments, 0 unusable experiments, no NaNs in metrics
- Note: duplicate_fanout/retry_fanout are intentionally blank for non-fanout stages.

## B. Analysis corpus

- Total runs: 3340
- Total logical operations represented: 33,400,000
- Workloads: create_order, charge_payment, reserve_inventory, send_notification
- Variants: V0, V1, V2, V3, V4, V5
- Failure scenarios: F0, F1, F2, F3, F4, F5, F6, F7, F8, F9, F11
- Seeds: 10 independent seeds per planned cell where applicable

## C. Audit status

- Metric consistency: PASS; 3340 checked, 0 unexplained discrepancies
- RRR definition: measured agent-attempt events, replayed measured attempts divided by measured retry requests
- State-transition violations: 0
- Ownership violations: 0
- Observer independence: PASS
- Automated sanity flags: 0
- Outlier audit: 542 latency/throughput candidates retained for review, not excluded
- Infrastructure retry audit: 1 infra retry attempt recorded; 63 preserved non-infra validation attempts; final scientific config changed: NO

## D. RQ1 Evidence Summary

Ambiguous-failure runs show V0 duplicate-effect exposure while protected mechanisms eliminate DER in the planned representative cells. Across group_b_ambiguous, V0 mean DER was 0.044894; V4 and V5 mean DER were 0.000000. At representative F3/P10/C50, V0 vs V2/V4/V5 DER reduction was about 0.09956 absolute, with Holm-adjusted p-values 0 in the generated registry.

## E. RQ2 Evidence Summary

Capability-placement tables quantify C0/C1/C2 differences under F3/F8. C0 evidence is separated in c0_limitation_evidence.csv to support the limitation that non-idempotent, non-queryable downstream APIs cannot be claimed to provide arbitrary exactly-once-effect resolution under post-effect ambiguity.

## F. RQ3 Evidence Summary

For F8, V4 had mean UAR 0.090072 and RSR 0.000000; V5 had mean UAR 0.000000 and RSR 0.089951, matching the reconciliation distinction. Under fanout 10, V0 had DER 1.000000 and DAF 10.0/11.0 for F6/F11, while V5 had DER 0.000000 and DAF about 0.99976/1.0 with RAF still 10.0/11.0.

## G. RQ4 Evidence Summary

Baseline F0 C50 shows no duplicate/loss difference across variants. V0 mean P95 was 47.113 ms and V5 mean P95 was 46.848 ms in the compact run-level recomputation; broader overhead tables include C1/C10/C50/C100 and V0-relative P95/P99/throughput deltas.

## H. H1-H5 Evidence Direction

- H1: CONSISTENT_WITH_HYPOTHESIS
- H2: NOT_CONSISTENT
- H3: MIXED
- H4: MIXED
- H5: CONSISTENT_WITH_HYPOTHESIS

## I. Important anomalies for human review

- 542 outlier candidates are listed in audits/outlier_runs.csv and retained in primary analysis.
- H2 is not supported by DER in the representative registry because V2, V4, and V5 all reduce DER to zero in those cells; V5 advantages appear in recovery/UAR rather than DER.
- H3 is mixed because the V4/V5 distinction is strong under F8 but not under F9 in the current planned comparison set.

## J. Export bundle

- Directory: results/phase7/export/paper_analysis_bundle
- Zip: results/phase7/export/paper_analysis_bundle.zip
- Bundle size: 5.85 MB directory, 1.10 MB zip

## K. Key files for ChatGPT/manuscript analysis

1. results/phase7/PHASE7_REPORT.md
2. results/phase7/phase6_run_level.csv
3. results/phase7/tables/master_descriptive_summary.csv
4. results/phase7/tables/RQ1_evidence.csv
5. results/phase7/tables/RQ2_evidence.csv
6. results/phase7/tables/RQ3_evidence.csv
7. results/phase7/tables/RQ4_evidence.csv
8. results/phase7/statistics/hypothesis_evidence.csv
9. results/phase7/statistics/effect_sizes.csv
10. results/phase7/statistics/comparison_registry.csv
11. results/phase7/tables/exactly_once_effect_audit.csv
12. results/phase7/tables/v4_v5_deep_comparison.csv
13. results/phase7/tables/c0_limitation_evidence.csv
14. results/phase7/tables/reliability_cost_tradeoff.csv
15. results/phase7/tables/practical_effect_magnitudes.csv
16. results/phase7/audits/metrics_consistency_summary.json
17. results/phase7/audits/result_sanity_flags.csv
18. results/phase7/audits/outlier_runs.csv
19. results/phase7/representative_traces/index.csv
20. results/phase7/export/paper_analysis_bundle.zip
