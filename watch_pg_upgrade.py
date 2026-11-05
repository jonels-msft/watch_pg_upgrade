#!/usr/bin/env python3
"""A read-only, log-based progress observer for pg_upgrade.
"""
import argparse
import codecs
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time

MESSAGE_WIDTH = 62
PHASE = re.compile(
    r"^(Checking|Creating|Setting|Analyzing|Freezing|Deleting|Copying|Converting|"
    r"Resetting|Restoring|Sync|Linking|Cloning|Swapping|Adding|Finding) "
)
STATUS = re.compile(r"^(ok|notice|warning|fatal|failed(?:[: ].*)?)$")
RESTORE = "Restoring database schemas in the new cluster"
DUMP = "Creating dump of database schemas"
GROUPS = (
    "Source compatibility checks",
    "Dump globals and database schemas",
    "Target compatibility checks",
    "Prepare target / transaction state",
    "Restore globals",
    "Restore database schemas",
    "Transfer relation files",
    "Finalize: OIDs, slots, sync, checks",
)
CHILD_NAME = re.compile(r"pg_upgrade_dump_(\d+)\.log$")
SGR = re.compile(r"\x1b\[[0-9;:]*m")
MAX_LINE = 1024 * 1024
DB_NAME = re.compile(
    r"""(?:^|\s)(?:'dbname=([A-Za-z0-9_.-]+)'|"dbname=([A-Za-z0-9_.-]+)"|"""
    r"""dbname=([A-Za-z0-9_.-]+))(?=\s|$)"""
)
FILE_OPERATION = re.compile(r'^(copying|linking|cloning) "')


class PhaseTimer:
    """Shared framing parser, kept here so the deployed watcher is self-contained."""

    def __init__(self, output):
        self.output = output
        self.phases = []
        self.active = None
        self.line = ""
        self.line_time = None
        self.line_utc = None
        self.prefix_seen = False
        self.last_seconds = 0.0
        self.issues = []

    def emit(self, seconds, utc, kind, message):
        stamp = f"{utc} " if utc else ""
        print(f"[{stamp}+{seconds:010.6f}s] {kind:7s} {message}",
              file=self.output, flush=True)

    def end_phase(self, seconds, utc, status):
        duration = seconds - self.active["start_seconds"]
        self.active.update(end_seconds=seconds, duration_seconds=duration, status=status,
                           coalesced=duration == 0)
        elapsed = "below capture resolution" if duration == 0 else f"{duration:.6f}s"
        self.emit(seconds, utc, "END", f"{status}; {elapsed}; {self.active['name']}")
        self.active = None

    def inspect_prefix(self):
        if self.prefix_seen or len(self.line) < MESSAGE_WIDTH:
            return
        prefix = self.line[:MESSAGE_WIDTH]
        if not (PHASE.match(prefix) and prefix.endswith(" ")):
            return
        if self.active is not None:
            self.issues.append("A new phase started before the preceding phase completed")
            self.end_phase(self.line_time, self.line_utc, "incomplete")
        self.active = {"name": prefix.rstrip(), "start_seconds": self.line_time}
        self.phases.append(self.active)
        self.emit(self.line_time, self.line_utc, "START", self.active["name"])
        self.prefix_seen = True

    def finish_line(self, seconds, utc):
        text = self.line[MESSAGE_WIDTH:] if self.prefix_seen else self.line
        status_text = text[MESSAGE_WIDTH:] if text.startswith(" " * MESSAGE_WIDTH) else text
        status = STATUS.fullmatch(status_text)
        if status and self.active is not None:
            self.end_phase(seconds, utc, status.group(1))
        elif text:
            self.emit(self.line_time, self.line_utc, "TEXT", text)
        self.line = ""
        self.line_time = None
        self.line_utc = None
        self.prefix_seen = False

    def feed(self, seconds, text, utc=None):
        if not isinstance(seconds, (int, float)) or not math.isfinite(seconds):
            raise ValueError("Capture timestamps must be finite numbers")
        if seconds < self.last_seconds:
            raise ValueError("Capture timestamps must be nonnegative and monotonic")
        if "\r" in text:
            raise ValueError("TTY/carriage-return output is unsupported; pipe pg_upgrade directly")
        self.last_seconds = seconds
        parts = text.split("\n")
        for i, part in enumerate(parts):
            if self.line_time is None and (part or i < len(parts) - 1):
                self.line_time, self.line_utc = seconds, utc
            self.line += part
            self.inspect_prefix()
            if i < len(parts) - 1:
                self.finish_line(seconds, utc)

    def finish(self, total_seconds, total_basis, upstream_exit_code=None):
        if not math.isfinite(total_seconds) or total_seconds < self.last_seconds:
            raise ValueError("Total duration must be finite and at least the last capture timestamp")
        if self.line:
            tail = self.line[MESSAGE_WIDTH:] if self.prefix_seen else self.line
            if tail:
                self.emit(self.line_time, self.line_utc, "TEXT", tail)
        if self.active is not None:
            self.issues.append("EOF before an active phase's completion marker")
            self.end_phase(self.last_seconds, None, "incomplete")
        if not self.phases:
            self.issues.append("No English padded phase labels were recognized")

        gaps = []
        cursor = 0.0
        previous = "(capture origin)"
        for phase in self.phases:
            if phase["start_seconds"] > cursor:
                gaps.append({"after": previous, "before": phase["name"],
                             "start_seconds": cursor, "end_seconds": phase["start_seconds"],
                             "duration_seconds": phase["start_seconds"] - cursor})
            cursor = phase["end_seconds"]
            previous = phase["name"]
        if cursor < total_seconds:
            gaps.append({"after": previous, "before": "(capture end)",
                         "start_seconds": cursor, "end_seconds": total_seconds,
                         "duration_seconds": total_seconds - cursor})
        labelled = sum(p["duration_seconds"] for p in self.phases)
        summary = {
            "format": 1, "parser": "PostgreSQL English non-TTY output; MESSAGE_WIDTH=62",
            "timing": "collector-observed elapsed wall time, not internal instrumentation",
            "total_seconds": total_seconds, "total_basis": total_basis,
            "upstream_exit_code": upstream_exit_code,
            "labelled_seconds": labelled, "unlabelled_seconds": total_seconds - labelled,
            "phases": self.phases, "unlabelled_intervals": gaps, "issues": self.issues,
        }
        self.emit(total_seconds, None, "SUMMARY",
                  f"{total_seconds:.6f}s total; {labelled:.6f}s labelled; "
                  f"{total_seconds - labelled:.6f}s unlabelled; "
                  f"{len(self.phases)} phases; upstream exit={upstream_exit_code}")
        for issue in self.issues:
            print("timestamp_pg_upgrade: " + issue, file=sys.stderr)
        return summary


def safe(text):
    """Keep log content from controlling the terminal (including bidi controls)."""
    return "".join(c if " " <= c <= "~" else ascii(c)[1:-1] for c in str(text))


def logfmt_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return json.dumps(round(value, 3), allow_nan=False)
    text = safe(value)
    # lnav 0.14 stops parsing at key=""; the valid bare key= preserves empty values.
    quoted = (re.search(r'[\s="\\]', text) or text in ("true", "false", "null")
              or re.fullmatch(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", text))
    return json.dumps(text) if quoted else text


def duration(seconds):
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:d}h{minutes:02d}m" if hours else f"{minutes:d}m{seconds:02d}s"


def group_for(name, previous):
    if name.startswith("Creating dump of"):
        return 1
    if (name == "Checking for extension updates"
            or name == "Checking for hash indexes" and previous == 7):
        return 7
    if name.startswith(("Checking ", "Finding ")):
        return 2 if (previous >= 1 or name.startswith("Checking new cluster")
                     or name == "Checking for presence of required libraries") else 0
    if name == "Restoring global objects in the new cluster":
        return 4
    if name == RESTORE:
        return 5
    if name.startswith(('Adding ".old"', "Linking user", "Cloning user",
                        "Copying user", "Swapping data")):
        return 6
    if name.startswith(("Setting next OID", "Restoring logical replication",
                        "Creating the replication conflict", "Sync data",
                        "Creating script to delete")):
        return 7
    if name.startswith(("Setting ", "Analyzing ", "Freezing ", "Deleting files ",
                        "Copying old ", "Converting pg_multixact", "Resetting WAL")):
        return 3
    return None


class Timeline(PhaseTimer):
    def __init__(self):
        super().__init__(None)
        self.groups = []
        self.highest_group = -1
        self.banner = None
        self.failure = None
        self.exit_code = None
        self.children = {}
        self.parent_last = None
        self.last_detail = ""
        self.last_detail_s = None
        self.last_warning = ""
        self.timing_complete = True
        self.finished_at = None
        self.child_location = ""
        self.parent_activity = None
        self.reading_history = False
        self.events = []
        self.alerts = []
        self.file_activity = {}
        self.main_activity = {}
        self.intervals = []

    def emit(self, seconds, utc, kind, message):
        if kind == "START":
            if self.banner:
                raise ValueError("Phase after completion banner; use a fresh, single-run main log")
            group = group_for(message, self.highest_group)
            self.active["timing_known"] = not self.reading_history
            self.groups.append(group)
            if group is not None:
                self.highest_group = max(self.highest_group, group)
        elif kind == "END":
            status = self.active["status"]
            if status.startswith(("failed", "fatal")):
                self.failure = message
            if status in ("warning", "notice"):
                self.last_warning = message
                self.alerts.append((seconds, "warning", message,
                                    {"phase": len(self.phases), "status": status}))
        elif kind == "TEXT":
            text = message.strip()
            if text.startswith("Performing Consistency Checks") and self.phases:
                raise ValueError("Repeated run header; use a fresh, single-run main log")
            if text == "Upgrade Complete":
                self.banner = "upgrade"
            elif text == "*Clusters are compatible*":
                self.banner = "check"
            elif text in ("Failure, exiting", "*failure*"):
                self.failure = f"{self.last_detail}; {text}" if self.last_detail else text
            if text and not text.startswith(("executing:", '"', "command:")):
                self.last_detail = text
                self.last_detail_s = seconds
            if text:
                self.parent_activity = seconds
            if self.active is not None and FILE_OPERATION.match(text):
                activity = self.file_activity.setdefault(len(self.phases) - 1,
                                                         {"count": 0, "last": seconds})
                activity["count"] += 1
                activity["last"] = seconds
            elif text and self.active is not None:
                activity = self.main_activity.setdefault(len(self.phases), {"count": 0})
                activity.update(last=seconds, message=text)
                activity["count"] += 1
        if kind in ("START", "END"):
            self.parent_activity = seconds
            if not self.reading_history:
                self.events.append((seconds, kind, len(self.phases) - 1))

    def observe(self, event):
        seconds, text = event["seconds"], event["text"]
        if not isinstance(text, str):
            raise ValueError("Event text must be a string")
        source = event.get("source", "main")
        if source == "main":
            self.feed(seconds, text)
            if len(self.line) > MAX_LINE:
                raise ValueError("Main log line exceeds 1 MiB")
            if text:
                self.parent_last = seconds
            return
        match = CHILD_NAME.fullmatch(Path(source).name)
        if match is None:
            raise ValueError(f"Unrecognized child log source: {source}")
        if not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
            raise ValueError("Child observation timestamps must be finite and nonnegative")
        oid = match.group(1)
        child = self.children.setdefault(oid, {
            "source": source, "pending": "", "last": seconds, "message": "",
            "counts": Counter(), "restore_seen": False, "error": None,
            "activity_last": None, "revision": 0, "program": None, "name": None,
            "milestones": set(), "programs": {},
            "interval": None, "interval_id": 0, "pending_history": False,
        })
        if child["source"] != source:
            raise ValueError(f"More than one run contains database OID {oid}")
        if seconds < child["last"]:
            raise ValueError(f"Child observation timestamps moved backwards: {source}")
        pending_history = child["pending_history"]
        child["pending"] += text
        if text:
            child["last"] = seconds
        lines = child["pending"].split("\n")
        child["pending"] = lines.pop()
        child["pending_history"] = bool(child["pending"]) and (
            self.reading_history or (pending_history and not lines))
        if len(child["pending"]) > MAX_LINE:
            raise ValueError("Child log line exceeds 1 MiB")
        for index, line in enumerate(lines):
            historical = self.reading_history or (index == 0 and pending_history)
            line = SGR.sub("", line)
            if line.startswith("command:"):
                self.finish_interval(child, "command")
                names = {next(value for value in match if value)
                         for match in DB_NAME.findall(line)}
                if len(names) == 1:
                    name = names.pop()
                    if child["name"] is not None and child["name"] != name:
                        raise ValueError(f"Conflicting database names in child log for OID {oid}")
                    child["name"] = name
            if not line.startswith(("pg_restore:", "pg_dump:")):
                if line.strip():
                    self.finish_interval(child, "unrecognized_output")
                continue
            child["message"] = line
            child["activity_last"] = seconds
            child["revision"] += 1
            child["program"] = line.split(":", 1)[0]
            detail = line.split(": ", 1)[-1]
            self.observe_interval(child, oid, detail, seconds, historical)
            observations = child["programs"].setdefault(child["program"], {})
            window = observations.setdefault(activity_label(detail), {
                "first": None if historical else seconds, "messages": 0,
            })
            window.update(last=None if historical else seconds,
                          seen=seconds, message=detail)
            window["messages"] += 1
            if line.startswith("pg_restore:"):
                child["restore_seen"] = True
            if line.startswith(("pg_restore: error:", "pg_dump: error:")):
                child["error"] = line
                self.failure = f"Database OID {oid}: {line}"
                self.alerts.append((seconds, "error", self.failure,
                                    {"db_oid": int(oid), "program": child["program"]}))
            if line.startswith(("pg_restore: warning:", "pg_dump: warning:")):
                self.alerts.append((seconds, "warning", f"Database OID {oid}: {line}",
                                    {"db_oid": int(oid), "program": child["program"]}))
            if line.startswith("pg_restore: creating "):
                kind = line[len("pg_restore: creating "):].split(' "', 1)[0]
                child["counts"][kind] += 1
                if kind in ("TABLE", "CONSTRAINT", "INDEX"):
                    child["milestones"].add(kind)
            elif line.startswith("pg_restore: processing data for table "):
                child["milestones"].add("DATA")

    def child_phase(self, program, seconds):
        name = RESTORE if program == "pg_restore" else DUMP
        candidates = [(i + 1, phase) for i, phase in enumerate(self.phases)
                      if phase["name"] == name]
        if len(candidates) != 1:
            return None
        number, phase = candidates[0]
        if seconds < phase["start_seconds"] or seconds > phase.get("end_seconds", seconds):
            return None
        return number

    def finish_interval(self, child, boundary, **fields):
        if child["interval"] is not None:
            self.intervals.append(dict(child["interval"], boundary=boundary, **fields))
            child["interval"] = None

    def observe_interval(self, child, oid, message, seconds, historical):
        program = child["program"]
        label = activity_label(message)
        if program == "pg_restore" and label not in (
            "creating TABLE", "creating CONSTRAINT", "creating INDEX", "processing table data",
            "error", "warning",
        ):
            label = "other restore messages"
        number = self.child_phase(program, seconds)
        current = child["interval"]
        if current and current["program"] != program:
            self.finish_interval(child, "program")
        elif current and current["phase"] != number:
            self.finish_interval(child, "phase_association")
        elif current and current["from_activity"] != label:
            self.finish_interval(child, "next_category", to_activity=label,
                                 to_s=None if historical else seconds)
        if child["interval"] is None:
            child["interval_id"] += 1
            child["interval"] = {
                "source": child["source"], "db_oid": int(oid), "program": program,
                "phase": number, "interval_id": child["interval_id"],
                "from_activity": label, "first_s": None if historical else seconds,
                "messages": 0, "boundary": "unreported",
            }
        current = child["interval"]
        current["last_s"] = None if historical else seconds
        current["messages"] += 1

    def result(self):
        if self.failure or (self.exit_code is not None and self.exit_code != 0):
            return "failed"
        if self.exit_code == 0:
            if not self.phases or self.active or self.issues or any(
                p.get("status") not in ("ok", "notice", "warning") for p in self.phases
            ):
                return "incomplete"
            if self.banner == "upgrade":
                return "success"
            if self.banner == "check":
                return "check_only"
            return "missing_banner"
        if self.banner:
            return "exit_unverified"
        return "observing"

    def outcome(self):
        return {
            "failed": "FAILED",
            "incomplete": "INCOMPLETE / unrecognized output",
            "success": "SUCCESS (exit 0 and completion banner)",
            "check_only": "CHECK ONLY complete (not an upgrade)",
            "missing_banner": "INCOMPLETE / no completion banner",
            "exit_unverified": "Completion banner observed; process exit UNVERIFIED",
            "observing": "OBSERVING (process state unknown)",
        }[self.result()]

    def returncode(self):
        if self.exit_code:
            return self.exit_code if 0 < self.exit_code < 126 else 1
        if self.failure:
            return 1
        return 0 if self.result() in ("success", "check_only") else 2


def load_reference(path):
    data = json.loads(path.read_text())
    if data.get("upstream_exit_code") != 0 or data.get("issues"):
        raise ValueError(f"Reference must have a verified successful exit and no issues: {path}")
    if not data.get("watcher_timing_complete", True):
        raise ValueError(f"Late-attach timings cannot be used as a reference: {path}")
    if data.get("watcher_outcome", "SUCCESS").split()[0] != "SUCCESS":
        raise ValueError(f"Reference must be a full upgrade, not a check: {path}")
    total = data["total_seconds"]
    if not isinstance(total, (int, float)) or not math.isfinite(total) or total <= 0:
        raise ValueError(f"Invalid reference total: {path}")
    phases = data["phases"]
    if not phases:
        raise ValueError(f"Reference contains no phases: {path}")
    end = 0
    for phase in phases:
        start, stop = phase["start_seconds"], phase["end_seconds"]
        if (not all(isinstance(t, (int, float)) and math.isfinite(t) for t in (start, stop))
                or not end <= start <= stop <= total
                or phase["status"] not in ("ok", "notice", "warning")):
            raise ValueError(f"Invalid reference phase: {path}")
        end = stop
    return data


def estimate_details(model, now, references):
    if not references:
        return ("ETA unknown: no comparable reference supplied",
                {"available": False, "reason": "no_reference"})
    if not model.timing_complete:
        return ("ETA withheld: attached after main output already existed",
                {"available": False, "reason": "late_attach"})
    if model.failure or model.issues or model.exit_code is not None or model.banner:
        return "ETA not applicable", {"available": False, "reason": "not_applicable"}
    if not model.phases:
        return ("ETA unknown: waiting for first phase",
                {"available": False, "reason": "no_phase"})
    names = [p["name"] for p in model.phases]
    for ref in references:
        if [p["name"] for p in ref["phases"][:len(names)]] != names:
            return ("ETA withheld: observed phase sequence differs from reference",
                    {"available": False, "reason": "sequence_mismatch"})
    remaining = []
    current = model.phases[-1]
    for ref in references:
        prior = ref["phases"][len(names) - 1]
        if model.active:
            elapsed = now - current["start_seconds"]
            if elapsed > prior["end_seconds"] - prior["start_seconds"]:
                return ("ETA withheld: current phase exceeded a reference duration",
                        {"available": False, "reason": "phase_overrun"})
            value = ref["total_seconds"] - prior["start_seconds"] - elapsed
        else:
            elapsed = now - current["end_seconds"]
            next_index = len(names)
            next_start = (ref["phases"][next_index]["start_seconds"]
                          if next_index < len(ref["phases"]) else ref["total_seconds"])
            if elapsed > next_start - prior["end_seconds"]:
                return ("ETA withheld: between-phase silence exceeded reference",
                        {"available": False, "reason": "gap_overrun"})
            value = ref["total_seconds"] - prior["end_seconds"] - elapsed
        remaining.append(value)
    low, high = min(remaining), max(remaining)
    span = duration(low) if len(remaining) == 1 else f"{duration(low)} - {duration(high)}"
    return (f"Historical ETA ~{span} remaining; NOT a bound",
            {"available": True, "references": len(references),
             "remaining_low_s": low, "remaining_high_s": high})


def estimate(model, now, references):
    return estimate_details(model, now, references)[0]


def counts_fields(counts):
    fields = {"object_starts": sum(counts.values())}
    fields.update((kind.lower() + "_starts", counts[kind])
                  for kind in ("TABLE", "CONSTRAINT", "INDEX") if counts[kind])
    other = sum(counts.values()) - sum(counts[k] for k in ("TABLE", "CONSTRAINT", "INDEX"))
    if other:
        fields["other_starts"] = other
    return fields


def activity_label(message):
    if message.startswith("processing data for table "):
        return "processing table data"
    for prefix in ("dropping ", "executing ", "disabling triggers ", "enabling triggers "):
        if message.startswith(prefix):
            return prefix.strip()
    if message.startswith(("error:", "warning:")):
        return message.split(":", 1)[0]
    return re.split(r"""["']|\b\d+\b""", message, maxsplit=1)[0].rstrip(": ")


def observation_label(activity):
    if activity.startswith("creating "):
        return activity[len("creating "):] + " announcement"
    if activity == "processing table data":
        return "table-data announcement"
    if activity == "other restore messages":
        return "other restore message"
    return activity + " message"


def relevant_children(model):
    name = model.active["name"] if model.active else ""
    program = "pg_restore" if name == RESTORE else "pg_dump" if name == DUMP else None
    return {oid: child for oid, child in model.children.items()
            if program and child["program"] == program}


def quiet_context(model, children):
    if model.active is None:
        if model.phases and model.phases[-1]["name"] == RESTORE:
            return ("Schema restore reported complete. Catalog rediscovery can run before file "
                    "transfer without a phase label; the current operation is unconfirmed.")
        return ("No current phase is announced. Between-phase work/server waits may be unreported; "
                "verify the log path and English output if no phases have appeared.")
    name = model.active["name"]
    if name == RESTORE:
        latest = max(children.values(), key=lambda c: c["activity_last"]) if children else None
        if latest and (latest["counts"]["CONSTRAINT"] or latest["counts"]["INDEX"]):
            return ("After constraint/index start messages, SQL, statistics (when enabled), commits "
                    "and worker waits can be silent. No statistics start/end boundary is logged.")
        if latest and "DATA" in latest["milestones"]:
            return ("Internal/table data, table statistics (when enabled) and commits can run "
                    "before constraint messages. This is not an isolated statistics timer.")
        return ("Restore SQL, statistics and worker waits may be unreported. Object starts do not "
                "identify the executing SQL or child completion; verify child logs if none appear.")
    if name == DUMP:
        return ("Catalog queries and waiting for dump workers can be silent. "
                "Per-database completion is not reported.")
    if name.startswith("Converting pg_multixact"):
        return "Multixact conversion has no per-ID progress messages in these logs."
    if name.startswith("Copying old "):
        return "Transaction-status directory copying has no incremental progress in these logs."
    if name.startswith("Sync data"):
        return "Final filesystem synchronization has no incremental progress, even in the utility log."
    if name.startswith(("Copying user", "Linking user", "Cloning user")):
        return "File messages mark operations, not completed bytes; work within one file is unreported."
    if name.startswith("Swapping data"):
        return "Swap-mode directory/catalog work and synchronization lack normal per-file messages."
    if name.startswith(("Checking ", "Finding ")):
        return "Catalog queries can be silent; database completion counters are not published."
    if name.startswith("Restoring logical"):
        return "Individual logical-slot creation can be silent; there are no within-slot counters."
    if name == "Restoring global objects in the new cluster":
        return "Global restore details are outside the monitored per-database logs; consult utility logs."
    return "This phase has no recognized incremental messages; SQL, I/O and waits may be unreported."


class EventReporter:
    def __init__(self, references=(), *, activity_interval=5, quiet_after=3,
                 heartbeat=30, max_children=4, source=None, mode="live", origin=None):
        self.references = references
        self.activity_interval = activity_interval
        self.quiet_after = quiet_after
        self.heartbeat = heartbeat
        self.max_children = max_children
        self.source = source
        self.mode = mode
        self.origin = origin or datetime.now(timezone.utc)
        self.clock_start = time.monotonic()
        self.time_basis = "attachment" if mode == "live" else "replay_clock"
        self.now = 0.0
        self.location = None
        self.started = False
        self.finished = False
        self.group = None
        self.sent_children = {}
        self.sent_windows = {}
        self.sent_files = {}
        self.sent_main = {}
        self.sent_restore_totals = None
        self.sent_intervals = {}
        self.alerted = set()
        self.quiet_key = None
        self.last_activity = 0
        self.last_quiet = None
        self.eta_state = None
        self.eta_at = float("-inf")
        self.banner = None

    def write(self, seconds, event, message, *, level="info", stream=None, **fields):
        stamp = (self.origin + timedelta(seconds=self.now)).isoformat(timespec="milliseconds")
        record = {"time": stamp.replace("+00:00", "Z"), "level": level,
                  "elapsed_s": self.now, "event": event}
        if seconds != self.now:
            record["observed_s"] = seconds
        record.update((key, value) for key, value in fields.items() if value is not None)
        record["msg"] = message
        print(" ".join(f"{key}={logfmt_value(value)}" for key, value in record.items()),
              file=stream, flush=True)

    def begin(self, model):
        self.write(0, "observer_start", "pg_upgrade timeline; read-only log observer",
                   schema=1, mode=self.mode, time_basis=self.time_basis, source=self.source)
        self.write(0, "notice", "Times are observations, not SQL timings. Starts != completions; "
                   "per-database completion is unreported.")
        self.write(0, "notice", "Silence does NOT prove a stall or health. "
                   "Ctrl-C stops only this observer.")
        if not self.references and not (model.banner or model.failure or model.exit_code is not None):
            self.write(0, "eta", "ETA unavailable: no comparable reference supplied.",
                       available=False, reason="no_reference")
        if not model.timing_complete:
            self.write(0, "attached", "LATE ATTACH: earlier timings unknown; "
                       "later fully observed phases remain timed.",
                       historical_phases=len(model.phases), timing="unknown")
            if model.active:
                self.stage(0, model.groups[-1])
                self.write(0, "phase_active", model.active["name"],
                           phase=len(model.phases), timing="unknown")
        self.started = True

    def stage(self, seconds, group):
        if group != self.group and group is not None:
            following = GROUPS[group + 1] if group + 1 < len(GROUPS) else None
            remaining = len(GROUPS) - group - 1
            self.write(seconds, "stage", GROUPS[group], stage=group + 1,
                       remaining_groups=remaining, conditional=True, next=following)
            self.group = group

    def phase_events(self, model):
        events, model.events = model.events, []
        index = 0
        while index < len(events):
            seconds, kind, number = events[index]
            phase = model.phases[number]
            group = model.groups[number]
            self.stage(seconds, group)
            if kind == "START" and index + 1 < len(events) and events[index + 1][1:] == ("END", number):
                index += 1
                seconds, kind, _ = events[index]
            if kind == "START":
                self.write(seconds, "phase_start", phase["name"], phase=number + 1,
                           timing="observed" if phase["timing_known"] else "unknown")
            else:
                known = phase["timing_known"]
                timing = ("observed" if phase["duration_seconds"] else "coalesced") if known else "unknown"
                fields = {"phase": number + 1, "status": phase["status"], "timing": timing}
                if known:
                    fields["start_s"] = phase["start_seconds"]
                    if phase["duration_seconds"]:
                        fields["duration_s"] = phase["duration_seconds"]
                if number in model.file_activity:
                    fields["file_messages"] = model.file_activity[number]["count"]
                level = ("info" if phase["status"] == "ok" else
                         "warning" if phase["status"] in ("notice", "warning") else "error")
                self.write(seconds, "phase_end", phase["name"], level=level, **fields)
            if group is None:
                self.write(seconds, "warning", "Unclassified phase: expected stage sequence may be incomplete.",
                           level="warning", phase=number + 1)
            index += 1

    def activity(self, model, now, *, final=False):
        pending = []
        for oid, child in model.children.items():
            for program, observations in child["programs"].items():
                name = RESTORE if program == "pg_restore" else DUMP
                number = next((i + 1 for i in reversed(range(len(model.phases)))
                               if model.phases[i]["name"] == name), None)
                ended = number is not None and "end_seconds" in model.phases[number - 1]
                force = final or ended or model.failure or model.banner or model.exit_code is not None
                key = (oid, program, number)
                previous = self.sent_children.get(key)
                revision = sum(window["messages"] for window in observations.values())
                if previous and previous[0] == revision:
                    continue
                changed = any((key, label) not in self.sent_windows for label in observations)
                if force or changed or previous is None or now - previous[1] >= self.activity_interval:
                    last = max(window["seen"] for window in observations.values())
                    pending.append((not force, previous[1] if previous else float("-inf"),
                                    -last, oid, program, number, observations, revision))
        pending.sort(key=lambda item: item[:5])
        deferred = 0
        emitted = 0
        for limited, _, _, oid, program, number, observations, revision in pending:
            if limited and emitted >= self.max_children:
                deferred += 1
                continue
            emitted += 1
            child = model.children[oid]
            key = (oid, program, number)
            for label, window in observations.items():
                if self.sent_windows.get((key, label)) == window["messages"]:
                    continue
                detail = window["message"]
                if len(detail) > 160:
                    detail = detail[:157] + "..."
                counts = counts_fields(child["counts"]) if program == "pg_restore" else {}
                self.write(window["seen"], "db_activity", detail, phase=number,
                           db=child["name"], db_oid=int(oid), program=program,
                           source=child["source"], activity=label,
                           activity_messages=window["messages"],
                           activity_unit=("starts" if program == "pg_restore"
                                          and label.startswith("creating ") else "messages"),
                           timing="observed" if window["first"] is not None else "unknown",
                           first_s=window["first"], last_s=window["last"], **counts)
                self.sent_windows[(key, label)] = window["messages"]
            self.sent_children[key] = (revision, now)
        if deferred:
            self.write(now, "activity_deferred", "Additional changed database logs; "
                       "details deferred, errors are never hidden.",
                       databases=deferred)
        for number, detail in model.main_activity.items():
            previous = self.sent_main.get(number)
            ended = "end_seconds" in model.phases[number - 1]
            force = ended or final or model.failure or model.banner or model.exit_code is not None
            if previous and (previous[0] == detail["count"] or
                             not force and now - previous[1] < self.activity_interval):
                continue
            self.write(detail["last"], "main_activity", detail["message"][:160],
                       phase=number, source="main", messages=detail["count"])
            self.sent_main[number] = (detail["count"], now)
        number = len(model.phases) - 1
        activity = model.file_activity.get(number) if model.active else None
        if activity:
            previous = self.sent_files.get(number)
            if previous is None or (activity["count"] != previous[0]
                                    and now - previous[1] >= self.activity_interval):
                self.write(activity["last"], "transfer", "File-operation messages, not completed files or bytes",
                           phase=number + 1, file_messages=activity["count"])
                self.sent_files[number] = (activity["count"], now)
        restore = next((i for i in reversed(range(len(model.phases)))
                        if model.phases[i]["name"] == RESTORE), None)
        if restore is not None and "end_seconds" in model.phases[restore]:
            counts = Counter()
            for child in model.children.values():
                counts.update(child["counts"])
            if counts and counts != self.sent_restore_totals:
                self.write(now, "restore_totals", "Object-start messages, not completions",
                           phase=restore + 1, **counts_fields(counts))
                self.sent_restore_totals = counts

    def quiet(self, model, now, children):
        key = (len(model.phases), model.active is not None)
        if model.active:
            boundary = model.active["start_seconds"]
            label = model.active["name"]
        elif model.phases:
            boundary = model.phases[-1]["end_seconds"]
            label = "between phases after " + model.phases[-1]["name"]
        else:
            boundary, label = 0, "waiting for a recognized phase"
        activity = max([boundary, model.parent_activity or 0] +
                       [child["activity_last"] for child in children.values()])
        if key != self.quiet_key:
            self.last_quiet = None
            self.quiet_key = key
        elif activity > self.last_activity:
            if self.last_quiet is not None:
                self.write(activity, "resumed", "Meaningful log activity resumed",
                           phase=len(model.phases) if model.active else None,
                           quiet_s=activity - self.last_activity)
            self.last_quiet = None
        self.last_activity = activity
        if now - activity < self.quiet_after:
            return
        if self.last_quiet is not None and now - self.last_quiet < self.heartbeat:
            return
        raw = max([model.parent_last or 0] + [child["last"] for child in children.values()])
        latest = max(children.items(), key=lambda item: item[1]["activity_last"]) if children else None
        detail = {}
        if latest and latest[1]["activity_last"] >= (model.parent_activity or boundary):
            oid, child = latest
            detail = {"db": child["name"], "db_oid": int(oid), "program": child["program"],
                      "last_message": child["message"].split(": ", 1)[-1][:160],
                      "source": child["source"]}
        elif model.last_detail and model.last_detail_s == model.parent_activity:
            detail = {"last_message": model.last_detail[:160], "source": "main"}
        self.write(now, "quiet", label, quiet_s=now - activity,
                   phase=len(model.phases) if model.active else None,
                   bytes_ago_s=now - raw if raw > activity else None, **detail)
        if self.last_quiet is None:
            if children:
                oid, child = max(children.items(), key=lambda item: item[1]["activity_last"])
                self.write(child["activity_last"], "last_activity", child["message"][:160],
                           phase=len(model.phases), db=child["name"], db_oid=int(oid))
            self.write(now, "context", quiet_context(model, children),
                       phase=len(model.phases) if model.active else None)
        self.last_quiet = now

    def correlations(self, model, now, final):
        closed, model.intervals = model.intervals, []
        current = [child["interval"] for child in model.children.values()
                   if child["interval"] is not None]
        for interval in closed + current:
            key = (interval["source"], interval["interval_id"])
            signature = (interval["messages"], interval["boundary"])
            previous = self.sent_intervals.get(key)
            number = interval["phase"]
            ended = number is not None and "end_seconds" in model.phases[number - 1]
            force = (interval["boundary"] != "unreported" or final or ended
                     or model.failure or model.banner or model.exit_code is not None)
            if previous and (previous[0] == signature or
                             not force and now - previous[1] < self.activity_interval):
                continue
            first = interval["first_s"]
            stop = interval.get("to_s") if interval["boundary"] == "next_category" else interval["last_s"]
            elapsed = None if first is None or stop is None else stop - first
            timing = ("unknown" if elapsed is None else
                      "observed" if round(elapsed, 3) > 0 else "coalesced")
            fields = dict(interval, timing=timing, basis="log_observations")
            fields["activity_unit"] = ("starts" if interval["program"] == "pg_restore"
                                       and interval["from_activity"].startswith("creating ")
                                       else "messages")
            if timing == "observed":
                fields["interval_s" if interval["boundary"] == "next_category"
                       else "message_span_s"] = elapsed
            child = model.children[str(interval["db_oid"])]
            label = observation_label(interval["from_activity"])
            if interval["boundary"] == "next_category":
                message = "First " + label + " -> next " + observation_label(interval["to_activity"])
            else:
                message = "First -> last " + label
            self.write(stop if stop is not None else now, "db_interval", message,
                       db=child["name"], **fields)
            if interval["boundary"] == "unreported":
                self.sent_intervals[key] = (signature, now)
            else:
                self.sent_intervals.pop(key, None)

    def outcome(self, model, now, final):
        result = model.result()
        level = ("error" if result == "failed" else
                 "warning" if result in ("incomplete", "missing_banner", "exit_unverified") else "info")
        self.write(now, "outcome", model.outcome(), level=level, result=result,
                   exit_code=model.exit_code, final=final)

    def summary(self, model, now):
        self.outcome(model, now, final=True)
        if model.active:
            self.write(now, "phase_unfinished", model.active["name"], phase=len(model.phases))
        for index, name in enumerate(GROUPS):
            phases = [p for p, group in zip(model.phases, model.groups) if group == index]
            if not phases:
                continue
            complete = [p for p in phases if p.get("status") in ("ok", "notice", "warning")]
            timed = [p for p in complete if p.get("timing_known", True)]
            seconds = sum(p["duration_seconds"] for p in timed)
            timing = ("observed" if seconds else "coalesced") if timed else "unknown"
            warnings = sum(p.get("status") in ("notice", "warning") for p in phases)
            self.write(now, "stage_summary", name, stage=index + 1,
                       complete=len(complete), phases=len(phases), timing=timing,
                       labelled_s=seconds if seconds else None,
                       untimed=len(complete) - len(timed), warnings=warnings or None)
        self.finished = True

    def update(self, model, now, *, final=False):
        if self.finished:
            return
        self.now = now
        if not self.started:
            self.begin(model)
        if model.child_location and model.child_location != self.location:
            self.write(now, "child_logs", model.child_location)
            self.location = model.child_location
        self.phase_events(model)
        self.activity(model, now, final=final)
        self.correlations(model, now, final)
        alerts, model.alerts = model.alerts, []
        if model.failure:
            alerts.append((now, "error", model.failure, {}))
        alerts.extend((now, "warning", issue, {"component": "parser"}) for issue in model.issues)
        for seconds, level, alert, fields in alerts:
            if (level, alert) not in self.alerted:
                self.write(seconds, level, alert, level=level, **fields)
                self.alerted.add((level, alert))
        if final:
            if model.active and not (model.failure or model.banner or model.exit_code is not None):
                children = relevant_children(model)
                self.quiet(model, now, children)
            self.summary(model, now)
            return
        if model.banner or model.failure or model.exit_code is not None:
            state = model.outcome()
            if state != self.banner:
                self.outcome(model, now, final=False)
                self.banner = state
            return
        children = relevant_children(model)
        self.quiet(model, now, children)
        if self.references:
            eta, fields = estimate_details(model, now, self.references)
            state = (fields["available"], fields.get("reason"))
            if state != self.eta_state or now - self.eta_at >= self.heartbeat:
                self.write(now, "eta", eta, **fields)
                self.eta_state, self.eta_at = state, now


class LogFile:
    def __init__(self, path, source):
        self.path, self.source = path, source
        self.stream = None
        self.identity = None
        self.offset = 0
        self.decoder = codecs.getincrementaldecoder("utf-8")("backslashreplace")

    def read(self, model, seconds):
        try:
            current = self.path.stat()
        except FileNotFoundError:
            if self.identity is not None:
                raise RuntimeError(f"Observed log disappeared (use --retain): {self.path}")
            return
        if not stat.S_ISREG(current.st_mode):
            raise ValueError(f"Expected a regular log file: {self.path}")
        if (self.identity is not None and not os.path.samestat(self.identity, current)
                or current.st_size < self.offset):
            raise RuntimeError(f"Log replaced or truncated: {self.path}")
        if self.identity is not None and current.st_size == self.offset:
            return
        if self.stream is None:
            self.stream = self.path.open("rb", buffering=0)
        opened = os.fstat(self.stream.fileno())
        if not os.path.samestat(opened, current):
            raise RuntimeError(f"Log replaced or truncated: {self.path}")
        self.identity = current
        self.stream.seek(self.offset)
        # Bound each scan to the size seen at its start, even with a busy producer.
        remaining = current.st_size - self.offset
        while remaining:
            raw = self.stream.read(min(65536, remaining))
            if not raw:
                raise RuntimeError(f"Log shortened during read: {self.path}")
            self.offset += len(raw)
            remaining -= len(raw)
            model.observe({"source": self.source, "seconds": seconds,
                           "text": self.decoder.decode(raw)})

    def close(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None


def run_directories(root):
    try:
        return {path for path in root.iterdir() if path.is_dir()}
    except FileNotFoundError:
        return set()


class Logs:
    def __init__(self, main, directory, root=None):
        self.main = LogFile(main, "main")
        self.directory = directory
        self.root = root
        self.existing = run_directories(root) if root is not None else set()
        self.children = {}

    def scan(self, model, seconds):
        self.main.read(model, seconds)
        if self.root is not None:
            new = run_directories(self.root) - self.existing
            if len(new) > 1:
                raise ValueError("Multiple new pg_upgrade runs; select one with --log-dir")
            if new:
                selected = next(iter(new)) / "log"
                if self.directory is not None and self.directory != selected:
                    raise ValueError("A different pg_upgrade run appeared during observation")
                self.directory = selected
        paths = []
        if self.directory is not None:
            model.child_location = str(self.directory)
            try:
                paths = list(self.directory.iterdir())
            except FileNotFoundError:
                pass  # The explicitly selected run may not have created its log directory yet.
        else:
            model.child_location = "Waiting for a NEW run directory under " + str(self.root)
        for path in sorted(paths):
            if CHILD_NAME.fullmatch(path.name):
                self.children.setdefault(path, LogFile(path, path.name))
        for child in self.children.values():
            try:
                child.read(model, seconds)
            finally:
                child.close()

    def close(self):
        self.main.close()
        for child in self.children.values():
            child.close()


def exit_status(path):
    if path is None:
        return None
    try:
        text = path.read_text().strip()
    except FileNotFoundError:
        return None
    if not re.fullmatch(r"[0-9]{1,3}", text) or not 0 <= int(text) <= 255:
        raise ValueError(f"Invalid exit status; publish this file atomically: {path}")
    return int(text)


def replay(args, model, reporter):
    last, next_display = 0.0, 0.0
    offsets = {}
    first = True
    with args.replay.open() as stream:
        for line in stream:
            event = json.loads(line)
            seconds = event["seconds"]
            if (not isinstance(seconds, (int, float)) or not math.isfinite(seconds)
                    or seconds < last):
                raise ValueError("Replay timestamps must be finite, nonnegative and monotonic")
            if first:
                if event.get("utc") is not None:
                    if not isinstance(event["utc"], str):
                        raise ValueError("Replay UTC timestamp must be a string with a timezone")
                    stamp = datetime.fromisoformat(event["utc"].replace("Z", "+00:00"))
                    if stamp.utcoffset() is None:
                        raise ValueError("Replay UTC timestamp must include a timezone")
                    reporter.origin = stamp.astimezone(timezone.utc) - timedelta(seconds=seconds)
                    reporter.time_basis = "capture"
                first = False
            if seconds > (datetime.max.replace(tzinfo=timezone.utc) - reporter.origin).total_seconds():
                raise ValueError("Replay clock exceeds timestamp range")
            source = event.get("source", "main")
            if "offset" in event:
                if event["offset"] != offsets.get(source, 0):
                    raise ValueError(f"Discontinuous replay bytes: {source}")
                offsets[source] = event["offset"] + event["bytes"]
            while next_display < seconds:
                if args.speed:
                    time.sleep((next_display - last) / args.speed)
                reporter.update(model, next_display)
                last = next_display
                next_display += args.interval
            if args.speed:
                time.sleep((seconds - last) / args.speed)
            model.observe(event)
            last = seconds
    total = last
    code = None
    if args.metadata:
        metadata = json.loads(args.metadata.read_text())
        total = metadata.get("total_seconds", metadata.get("elapsed_seconds"))
        code = metadata.get("upstream_exit_code", metadata.get("exit_code"))
        if not isinstance(code, int) or not 0 <= code <= 255:
            raise ValueError("Replay metadata must contain an exit code in [0, 255]")
        if not isinstance(total, (int, float)) or not math.isfinite(total) or total < last:
            raise ValueError("Replay total must be finite and at least the last event timestamp")
        if total > (datetime.max.replace(tzinfo=timezone.utc) - reporter.origin).total_seconds():
            raise ValueError("Replay clock exceeds timestamp range")
    while next_display < total:
        if args.speed:
            time.sleep((next_display - last) / args.speed)
        reporter.update(model, next_display)
        last = next_display
        next_display += args.interval
    if args.speed:
        time.sleep((total - last) / args.speed)
    model.exit_code = code
    model.finished_at = total
    reporter.update(model, total, final=True)


def watch(args, model, reporter):
    logs = Logs(args.main_log, args.log_dir, args.log_root)
    if args.main_log.exists() and args.main_log.stat().st_size:
        model.timing_complete = False
        if args.log_root is not None:
            raise ValueError("Main log already has output; attach with --log-dir for that exact run")
    reporter.origin = datetime.now(timezone.utc)
    start = reporter.clock_start = time.monotonic()
    model.reading_history = not model.timing_complete
    try:
        while True:
            now = time.monotonic() - start
            logs.scan(model, now)
            model.reading_history = False
            model.exit_code = exit_status(args.exit_file)
            if model.exit_code is not None:
                # The exit file must be published only after the producer closes its logs.
                logs.scan(model, now)
            final = (args.once or model.exit_code is not None
                     or (args.exit_file is None and (model.banner or model.failure)))
            reporter.update(model, now, final=final)
            if final:
                model.finished_at = now
                return
            time.sleep(args.interval)
    finally:
        logs.close()


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Output: one logfmt event per line; readable directly or with lnav.\n"
            "  lnav watcher.log   Recognizes time, level and msg without a profile.\n"
            "  time/elapsed_s describe observer reports, not SQL execution times.\n"
            "  Runtime errors on stderr are also logfmt; --help/usage are plain text.\n"
            "\n"
            "pg_upgrade tips for best results (not watcher flags):\n"
            "  --retain   Keep generated logs after success; their deletion can\n"
            "             interrupt the watcher. --verbose does not imply --retain.\n"
            "  --verbose  Optional: adds dump and file-transfer detail. Phase messages\n"
            "             and verbose pg_restore child logs still work without it.\n"
            "  LC_ALL=C   Ensure English messages (environment setting, not a flag).\n"
            "  Logs       Read pg_upgrade_internal.log or redirected output, not a\n"
            "             terminal transcript. Select the exact run's log directory.\n"
            "\n"
            "For verified success, use the watcher's --exit-file option. Publish the\n"
            "actual pg_upgrade exit code atomically only after pg_upgrade exits.\n"
            "Without it, a completion banner leaves the process exit unverified.\n"
            "The watcher only reads logs; it never launches or controls pg_upgrade."
        ))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--main-log", type=Path,
                      help="Redirected main output or pg_upgrade_internal.log (may not exist yet)")
    mode.add_argument("--replay", type=Path, help="Replay existing main/child JSONL captures")
    children = parser.add_mutually_exclusive_group()
    children.add_argument("--log-dir", type=Path, help="Exact current run's timestamp/log directory")
    children.add_argument("--log-root", type=Path,
                          help="pg_upgrade_output.d; discover one NEW run, starting before pg_upgrade")
    parser.add_argument("--exit-file", type=Path, help="Atomically published pg_upgrade exit code")
    parser.add_argument("--once", action="store_true", help="Read current state once, then exit")
    parser.add_argument("--metadata", type=Path, help="Replay command JSON or .phases.json")
    parser.add_argument("--speed", type=positive, help="Replay clock multiplier; default: no delays")
    parser.add_argument("--interval", type=positive, default=1.0, help="Log polling seconds (default: 1)")
    parser.add_argument("--plain", action="store_true",
                        help="Compatibility no-op: output is always plain ASCII logfmt")
    parser.add_argument("--activity-interval", type=positive, default=5,
                        help="Seconds between routine per-database/file updates (default: 5)")
    parser.add_argument("--quiet-after", type=positive, default=3,
                        help="Explain a phase after this many seconds without activity (default: 3)")
    parser.add_argument("--heartbeat", type=positive, default=30,
                        help="Seconds between continued-quiet reminders / ETA updates (default: 30)")
    parser.add_argument("--max-children", type=int, default=4,
                        help="Changed database details per scan; errors are unlimited (default: 4)")
    parser.add_argument("--reference", type=Path, action="append", default=[],
                        help="Comparable successful phase summary for a heuristic ETA; repeatable")
    parser.add_argument("--summary", type=Path, help="Write observed phases to a NEW JSON file")
    args = parser.parse_args(argv)
    if args.main_log and (not (args.log_dir or args.log_root) or args.metadata or args.speed):
        parser.error("--main-log requires --log-dir or --log-root; --metadata/--speed are replay-only")
    if args.replay and (args.log_dir or args.log_root or args.exit_file or args.once):
        parser.error("--log-dir, --log-root, --exit-file and --once are live-only")
    if args.max_children < 1:
        parser.error("--max-children must be positive")
    model = Timeline()
    reporter = EventReporter(activity_interval=args.activity_interval,
                             quiet_after=args.quiet_after, heartbeat=args.heartbeat,
                             max_children=args.max_children,
                             source=str(args.main_log or args.replay),
                             mode="replay" if args.replay else "live")
    try:
        if args.summary and (args.summary.exists() or args.summary.is_symlink()):
            raise ValueError(f"Refusing to overwrite {args.summary}")
        reporter.references = [load_reference(path) for path in args.reference]
        if args.replay:
            replay(args, model, reporter)
        else:
            watch(args, model, reporter)
        if args.summary:
            # Do not turn an observation stop into a fictitious phase completion.
            summary = {
                "format": 1, "total_seconds": model.finished_at,
                "total_basis": "watcher observations, not internal execution timestamps",
                "upstream_exit_code": model.exit_code, "phases": model.phases,
                "issues": model.issues,
                "watcher_timing_complete": model.timing_complete,
                "watcher_outcome": model.outcome(),
            }
            descriptor = os.open(args.summary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as stream:
                json.dump(summary, stream, indent=2)
                stream.write("\n")
        return model.returncode()
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, OverflowError) as error:
        if not args.replay:
            reporter.now = time.monotonic() - reporter.clock_start
        reporter.write(reporter.now, "observer_error", str(error), level="error",
                       stream=sys.stderr, observer_exit_code=2)
        return 2
    except KeyboardInterrupt:
        if not args.replay:
            reporter.now = time.monotonic() - reporter.clock_start
        reporter.write(reporter.now, "observer_stop", "Observer stopped; pg_upgrade was not signalled.",
                       level="warning", stream=sys.stderr, observer_exit_code=130)
        return 130


if __name__ == "__main__":
    sys.exit(main())
