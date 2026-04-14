# PgQue roadmap notes

This file exists to keep implementation-planning bulk out of the README.

## Near-term priorities

- make README small and sharp
- keep semantics brutally clear
- harden tests around batch behavior and retry flow
- keep generated install SQL reproducible
- rerun benchmarks properly later

## Release discipline

Before release, prefer:

- fewer claims
- tighter semantics
- stronger tests
- cleaner packaging

Overpromising queue semantics is how people buy themselves a future outage.
