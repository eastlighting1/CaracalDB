---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Benchmarks

Benchmarks protect performance-sensitive paths from drifting quietly. They are not marketing numbers; they are regression checks with clear scenarios and baselines.

## Current Flow

1. Run benchmark scenarios.
2. Write latest results as JSON.
3. Compare against `bench/results/baseline.json`.
4. Fail when a measured scenario exceeds the configured tolerance.

## Updating A Baseline

Update a baseline only when the new value is measured, reproducible, and explained in the PR. A baseline should not be raised just to make CI pass.

## Related Guide

See [Benchmark and Regression](../guides/benchmark-and-regression.md).
