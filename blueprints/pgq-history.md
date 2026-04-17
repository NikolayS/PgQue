# PgQ: History

Background on PgQ's origins at Skype and its path to PgQue. For the
technical vocabulary and semantics, see `pgq-concepts.md`.

## Timeline

- **2006** -- PgQ started at Skype, inspired by ideas from Slony.
- **2007** -- First application was Londiste replication. Open-sourced as
  part of the Skytools framework.
- **2009** -- Skytools 3.0 alpha introduced cooperative consumers and
  cascading. Marko Kreen (PgQ's maintainer) and Martin Pihlak co-presented
  "Skytools: PgQ -- Queues and applications" at PgCon 2009
  ([slides PDF](https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf),
  [event page](https://www.pgcon.org/2009/schedule/events/138.en.html)),
  which remains the clearest single overview of PgQ's design. Much of the
  phrasing in PgQue's user-facing docs is adapted from that talk.
- **2026** -- PgQue repackages PgQ core (ISC, Marko Kreen / Skype
  Technologies OU) for PG14+ managed database environments: rename to
  `pgque`, modernization, single-file install, `pg_cron`-driven ticker,
  security hardening.

## Production pedigree

Skype ran "hundreds of queues and consumers" on PgQ, centrally monitored.
That operational experience is what PgQue inherits -- we are not inventing a
queue, we are repackaging a proven one.

## Lineage of the codebase

```
PgQ (2006+, Skype / Marko Kreen, ISC)
  -> Skytools 2.x (2007)
    -> Skytools 3.x (2009+, cascading, cooperative consumers)
      -> pgq standalone repository (github.com/pgq/pgq)
        -> PgQue (2026, PG14+ / managed DB edition)
```

PgQue's `pgque-core` layer is a mechanical transformation of ~4,028 lines of
PgQ PL/pgSQL (rename, PG14+ modernization, security hardening, single-file
install). The `pgque-api` layer (send/receive/ack/nack, DLQ, delayed
delivery) is new code that reduces cleanly to PgQ primitives.

## Attribution

PgQ is ISC-licensed, copyright Marko Kreen / Skype Technologies OU.
PgQue (Apache-2.0, copyright 2026 Nikolay Samokhvalov) preserves the PgQ
copyright notice in every derived source file. See `NOTICE` in the repo
root.
