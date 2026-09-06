# Contracts

- `tams-v8.1.json` and `tams-v8.2.json` record the supported BBC TAMS and
  TAMOSS revisions, required ingest operations and known gaps.
- `tamsin/ingest-events-v2.json` is the current ingest NDJSON schema.
- `tamsin/doctor-report-v1.json` and `tamsin/profiles-report-v1.json` describe
  the two finite JSON reports.

The Go tests validate runtime output against these schemas. TAMS schemas are
embedded only where the implementation needs them; this directory is not a
second copy of the upstream specification.
