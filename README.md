# watch_pg_upgrade

A read-only, log-based progress observer for pg_upgrade.

Watches an **existing** upgrade's logs. Never launches pg_upgrade, connects to a
database, or signals PostgreSQL.

## TL;DR

Requires Python 3.8+ on Linux/Unix; no pip packages. lnav 0.14.1 is optional.
Run from this checkout, selecting the exact upgrade run's log directory:

```sh
# Terminal 1: observe and save readable logfmt
LOGDIR=/path/to/NEW_PGDATA/pg_upgrade_output.d/THIS_RUN_TIMESTAMP/log
umask 077
python3 watch_pg_upgrade.py \
  --main-log "$LOGDIR/pg_upgrade_internal.log" \
  --log-dir "$LOGDIR" > "$HOME/pg-upgrade-watch.log" 2>&1

# Terminal 2, from the same checkout: grouped live view
lnav -I lnav -f lnav/progress.lnav "$HOME/pg-upgrade-watch.log"
```

**F5** refreshes; **q** returns to raw logs. For Snap, replace `lnav` with
`snap run lnav`. Keep each upgrade in a separate output file.

## Essentials

- Run pg_upgrade with **`LC_ALL=C`** and **`--retain`**. `--verbose` is optional.
  Use regular log files, not terminal transcripts or a pipe from pg_upgrade.
- **Ctrl-C stops only the observer.** `--once` takes a snapshot; `--help` lists options.
- **Quiet does not mean stalled.** Counts are log messages, not completed work.
- Earlier timings are unknown after late attachment. Coalesced means below
  observation resolution, not zero.
- For verified completion, add `--exit-file PATH`; your launcher must atomically
  publish pg_upgrade's actual exit code there. Otherwise its exit is unverified.

## Timing summary

Use the progress view for activity; use this view to see where measured time went:

```sh
lnav -I lnav -f lnav/timing.lnav "$HOME/pg-upgrade-watch.log"
```

Phases with measured durations are ranked longest first. Open phases and
warnings/errors stay visible; routine untimed phases are omitted. Shares cover
measured phase time, not total runtime or percent complete. Open-phase ages stop
at the last report. **F5** refreshes.

## Development

Tests: `python3 -m unittest discover -s tests`

Prototype parser for PostgreSQL's English log messages, not a stable progress
protocol. No license has been selected yet.
