# GPU Sentinel benchmark — 20261026-233240-cpu

- **Engine**: `cpu` · **workers**: 3 × concurrency 4 · **load**: 100.0 req/s open-loop, 16 tokens/request
- **Host**: Windows-10-10.0.26300-SP0 · AMD64 Family 25 Model 97 Stepping 2, AuthenticAMD (32 logical CPUs) · GPU: NVIDIA GeForce RTX 5060 Laptop GPU, 8151 MiB, 596.13 · Python 3.11.9
- **Policy**: probe every 1.0 s (timeout 1.0 s), 3 consecutive failures → unhealthy, 2 passes → healthy, startup probe 0.2 s; backoff base 0.5 s cap 30.0 s (full jitter); budget 5/300.0 s
- **Repeats per fault**: 5 · total wall time 1296 s

## Fault injection (timings measured from the injection timestamp)

| Scenario | n | Detect (ms) | Removed from routing (ms) | Service restored (ms) | Stop rung | Requests in fault window | Failed | Success in window | Retried OK |
|---|---:|---:|---:|---:|---|---:|---:|---:|---:|
| crash_sigkill (`process_exited`) | 5 | 20 (17-25) | 11 (9-13) | 1747 (1456-1948) | already_dead | 178 (146-197) | 0 (0-0) | 100.00 % | 1 (0-1) |
| crash_internal (`process_exited`) | 5 | 59 (55-65) | 55 (50-61) | 1922 (1835-2326) | already_dead | 195 (184-236) | 0 (0-0) | 100.00 % | 1 (0-2) |
| hang (`timeout`) | 5 | 3226 (3207-3278) | 1236 (1204-1282) | 6116 (5751-6693) | terminate | 614 (574-669) | 0 (0-0) | 100.00 % | 3 (2-4) |
| slow (`degraded_too_long`) | 5 | 24217 (23222-24238) | 8226 (7223-8254) | 25858 (24882-26692) | drain | 2590 (2492-2671) | 0 (0-0) | 100.00 % | 0 (0-0) |
| leak (`rss_leak_trend`) | 5 | 10176 (9210-10193) | 10176 (9211-10193) | 11885 (10938-12557) | drain | 1192 (1094-1258) | 0 (0-0) | 100.00 % | 0 (0-0) |
| oom (`rss_limit`) | 5 | 959 (908-2281) | 959 (907-2007) | 3197 (2899-4837) | drain | 323 (292-483) | 0 (0-0) | 100.00 % | 0 (0-0) |

*Detect* = incident opened. *Removed from routing* = first transition out of HEALTHY (suspect/degraded/unhealthy), after which the gateway stops sending new requests to the worker. *Service restored* = replacement worker passed its readiness checks and is routable again. Ranges are min–max over repeats; the headline figure is the median.

| Scenario | p50 before fault | p99 before fault | p50 in window | p99 in window | p99 after recovery | Total failed / total requests |
|---|---:|---:|---:|---:|---:|---:|
| crash_sigkill | 9.8 ms (9.4-13.6) | 50.6 ms (43.5-59.3) | 9.7 ms (9.2-14.8) | 83.8 ms (47.8-153.7) | 55.1 ms (47.4-384.1) | 0 / 12500 |
| crash_internal | 12.8 ms (12.6-13.8) | 53.7 ms (50.6-132.6) | 14.9 ms (13.9-16.4) | 62.5 ms (54.5-77.8) | 56.2 ms (52.5-79.5) | 0 / 12500 |
| hang | 13.1 ms (12.5-13.5) | 53.9 ms (51.2-74.8) | 15.9 ms (12.4-18.3) | 55.4 ms (53.0-59.7) | 54.0 ms (52.2-57.0) | 0 / 12500 |
| slow | 13.2 ms (12.8-13.4) | 53.4 ms (51.0-77.4) | 17.8 ms (17.1-18.3) | 75.4 ms (63.4-131.4) | 54.4 ms (52.4-56.0) | 0 / 22500 |
| leak | 12.9 ms (12.6-13.6) | 53.0 ms (49.8-79.2) | 18.9 ms (17.9-20.9) | 72.1 ms (62.3-117.5) | 52.9 ms (51.6-58.3) | 0 / 37500 |
| oom | 12.8 ms (12.5-13.8) | 70.2 ms (51.7-227.6) | 20.0 ms (19.8-20.9) | 90.9 ms (50.7-462.3) | 55.3 ms (48.3-56.0) | 0 / 12500 |

## Crash loop (same worker killed again after every recovery; budget 5 restarts / 300 s)

| Kill # | Backoff slept (s) | Detect (ms) | Service restored (ms) | Outcome |
|---:|---:|---:|---:|---|
| 1 | 0.459 | 17.8 | 1938.5 | recovered |
| 2 | 0.806 | 16.3 | 2281.7 | recovered |
| 3 | 0.732 | 17.1 | 2199.5 | recovered |
| 4 | 0.117 | 17.0 | 1584.5 | recovered |
| 5 | 3.539 | 16.2 | 4994.3 | recovered |
| 6 | None | 15.3 | — | quarantined |

Final state: `quarantined`. Backoff is full-jitter, so slept time is a uniform sample from a doubling envelope, not a smooth curve.

## False positives (healthy fleet under load)

- 60 s at 99.9 req/s achieved (6000 requests, 100.0 % success, p50 12.3 ms, p99 48.91 ms)
- **Incidents opened: 0** · suspect transitions (single failed probe): 0 · degraded transitions: 0 · probe ticks: 1212

## Latency vs load (15 s per level, healthy fleet)

| Target req/s | Achieved | Success | p50 ms | p95 ms | p99 ms | max ms | Shed (503) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 50.1 | 100.0 % | 11.16 | 14.71 | 48.6 | 57.72 | 0 |
| 100 | 100.0 | 100.0 % | 11.55 | 20.2 | 53.12 | 90.33 | 0 |
| 200 | 199.5 | 100.0 % | 12.55 | 28.55 | 56.55 | 91.56 | 0 |
| 400 | 398.4 | 100.0 % | 15.77 | 35.72 | 65.77 | 84.69 | 0 |
| 800 | 699.1 | 51.683 % | 530.81 | 2468.31 | 4635.21 | 6003.06 | 67 |
