# watch_pg_upgrade

A read-only, log-based progress observer for pg_upgrade.

The script watches an **existing** upgrade's main and per-database logs. It
reports phase transitions, relevant child activity, and contextual explanations
of quiet periods as readable logfmt. It never launches pg_upgrade, connects to a
database, or signals PostgreSQL processes.

## Requirements

- Python 3.8+ with its standard library; no pip packages.
- Read access to the selected upgrade's English, non-TTY logs.
- Linux/Unix for the supported operating workflow and tests.

Copy [watch_pg_upgrade.py](watch_pg_upgrade.py) to the machine running the
upgrade. That single file is all the observer needs. lnav is an optional viewer,
not a dependency.

```sh
python3 watch_pg_upgrade.py --help
```

This is a prototype, not a production-certified monitor. The parser recognizes
PostgreSQL's English phase framing rather than a stable progress protocol.
Synthetic compatibility cases cover selected PG16-19 and development-tree
message variants; they are not full upgrade tests of every version.

## Attach to an upgrade

Select the exact run's timestamped log directory, not a wildcard or a guessed
newest directory:

```sh
LOGDIR=/path/to/NEW_PGDATA/pg_upgrade_output.d/THIS_RUN_TIMESTAMP/log
python3 watch_pg_upgrade.py \
  --main-log "$LOGDIR/pg_upgrade_internal.log" \
  --log-dir "$LOGDIR"
```

`--main-log` also accepts pg_upgrade stdout/stderr redirected to a regular file.
Do not pipe pg_upgrade itself through this observer: stopping observation must
not give the upgrade a broken pipe.

When attaching after output already exists, earlier timings are unknown.
Later phases whose starts and ends are observed retain their durations.
`--once` reads the current state and summarizes without waiting.
Ctrl-C stops only the observer.

### pg_upgrade setup tips

- **`--retain`** keeps generated logs after success. Their deletion can
  interrupt observation; the script rejects disappeared, truncated, or replaced
  logs instead of inventing progress.
- **`--verbose` is optional.** It adds dump and file-transfer detail. Main phase
  messages and verbose pg_restore child logs remain available without it.
  `--verbose` does not imply `--retain`.
- **`LC_ALL=C`** ensures English messages.
- Use redirected output or the internal log, not a terminal transcript with
  carriage-return updates.

## Reading the output

Reports phase changes, child activity, and quiet periods. **Quiet does not mean
stalled.** Counts are log messages, not completed work.

For verified completion, add `--exit-file PATH`. Your launcher must atomically
publish pg_upgrade's actual exit code there after it exits; otherwise its exit
remains unverified.

Output is logfmt: save it to a private file and browse with `lnav watcher.log`.
No custom format is needed. See `--help` for other options.

## Development

Run from the repository root:

```sh
python3 -m unittest discover -s tests
```

No license has been selected yet.
