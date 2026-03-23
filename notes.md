# Engineering Notes

## Design goals

- Keep ingestion and analytics logic separate
- Fail safely on malformed records instead of crashing the pipeline
- Use a data shape that maps cleanly to both Python and C++ implementations
- Make outputs deterministic enough to test

## Performance considerations

### Python
- Avoid unnecessary object churn in the hot path
- Use deques for rolling windows
- Aggregate in a single pass where possible

### C++
- Parse line by line and keep allocations limited
- Use unordered_map for per-symbol metric state
- Prefer a single-pass update model for throughput

## Trade-offs

This project favours clarity over ultra-low-level optimisation. In a real trading environment, I would expect tighter control over memory layout, binary formats, and profiling against realistic volumes.
