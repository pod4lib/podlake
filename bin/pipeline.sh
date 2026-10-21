#!/usr/bin/env bash
#
# The scheduled pipeline: bring the lake up to date, then republish the public
# dashboard from it. Written for cron; safe to run by hand.
#
#     podlake/bin/pipeline.sh
#
# The ordering is the whole reason this is one script rather than separate cron
# entries. `sync-all` applies each POD resource in its own transaction, so a
# reader never sees a torn write — but it can absolutely see a lake where one
# institution's latest dump has landed and another's has not. Every headline
# figure on the dashboard compares institutions to each other, so publishing from
# a part-way-synced lake produces numbers that are wrong in a way that looks
# entirely plausible. Hence one script, `set -e`, and a failed sync publishing
# nothing: stale beats half-synced. A time gap between two cron entries could not
# express the ordering either, since a sync with months of catch-up to do runs
# for hours.

set -euo pipefail

# Defaults assume the sibling layout podlake-web already requires — its
# pyproject.toml depends on podlake by path (`../podlake`) — so an ordinary pair
# of checkouts needs no configuration at all:
#
#     <pod root>/podlake/        this checkout
#     <pod root>/podlake-web/    the dashboard
#     <pod root>/logs/           written here
#
# Override any of these in the environment (cron: set them above the entry).
PODLAKE_DIR=${PODLAKE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
POD_ROOT=${POD_ROOT:-$(dirname "$PODLAKE_DIR")}
WEB_DIR=${WEB_DIR:-$POD_ROOT/podlake-web}
LOG_DIR=${LOG_DIR:-$POD_ROOT/logs}
LOCK_FILE=${LOCK_FILE:-$POD_ROOT/pipeline.lock}

# The lake, as an absolute path, because podlake-web's extract takes it as an
# explicit argument. Keep this in step with PODLAKE_CATALOG if you have set that
# in podlake's .env — this script does not read that file.
CATALOG=${CATALOG:-$PODLAKE_DIR/podlake.ducklake}

# The branch the refresh pushes. main is what deploys the live site; any other
# name stages the change for a human to merge instead.
PUBLISH_BRANCH=${PUBLISH_BRANCH:-main}

# uv installs to ~/.local/bin, which is not on cron's near-empty PATH. This is
# the classic reason a job that works by hand fails on a schedule.
export PATH="$HOME/.local/bin:$PATH"

# The fetch in step 0 and the push inside `refresh` both need a GitHub credential,
# and cron has no SSH agent to hold one — so the key must be a passphrase-less
# file that ssh reads directly. Setting DEPLOY_KEY names it here instead of
# relying on whatever ~/.ssh/config the account happens to have.
#
# IdentitiesOnly is the part that is easy to get wrong: without it ssh offers
# every key it can find, GitHub accepts the first that authenticates, and a
# deploy key for a *different* repo authenticates fine — then the push fails as
# "repository not found", which reads like a permissions problem and isn't.
if [ -n "${DEPLOY_KEY:-}" ]; then
  export GIT_SSH_COMMAND="ssh -i $DEPLOY_KEY -o IdentitiesOnly=yes"
fi

mkdir -p "$LOG_DIR"

# cron decides whether to mail you by whether the job wrote anything — the exit
# status has nothing to do with it. Everything below goes to the log file, so
# without the fd juggling here a failure would be completely silent: no output, no
# mail, a stale dashboard and nobody told. So keep the real stderr open on fd 8,
# which is what cron collects, and report there on any nonzero exit.
exec 8>&2
exec >> "$LOG_DIR/pipeline.log" 2>&1

# The log tail rides along so the mail is actionable without logging into the box.
# Only nonzero exits report, which is what leaves the two deliberate `exit 0`
# paths — nothing to publish, and a run already in progress — quiet.
report_failure() {
  # First statement, so this is still the status that triggered the trap.
  local status=$?
  if [ "$status" -ne 0 ]; then
    {
      echo "podlake pipeline FAILED (exit $status) on $(hostname -s)"
      echo "log: $LOG_DIR/pipeline.log"
      echo "--- last 20 lines"
      tail -n 20 "$LOG_DIR/pipeline.log"
    } >&8
  fi
}
trap report_failure EXIT

echo "=== pipeline $(date -u +%FT%TZ) on $(hostname -s)"

# A sync with months of catch-up can outrun its own schedule. Skip rather than
# pile up: flock is released by the kernel however this process dies, so a killed
# run cannot wedge every run after it. Its absence is checked separately because
# macOS has no flock(1), and a bare failure below would look exactly like
# "another run holds the lock" — that is, like a successful no-op.
if ! command -v flock > /dev/null; then
  echo "flock not found — this script expects a Linux host"
  exit 1
fi
exec 9> "$LOCK_FILE"
if ! flock -n 9; then
  echo "a previous pipeline is still running; skipping this window"
  exit 0
fi

# Both halves of this pipeline spill large DuckDB operators to disk, and both pick
# where from Python's tempfile.gettempdir(). That function *probes* TMPDIR and
# silently falls back to /tmp when it cannot write there — so a TMPDIR naming a
# directory that does not exist does not fail, it quietly stops applying. The only
# evidence is a "spilling to /tmp" line in this log, and the consequence is tens of
# GiB landing on whatever volume holds /tmp: possibly small, possibly a tmpfs, in
# which case it is spilling memory into memory and the spill was pointless.
#
# Created here rather than assumed, and fatal if it cannot be. Setting TMPDIR and
# having it silently ignored is the exact failure this prevents, which is not worth
# trading for a run that "succeeds" while spilling somewhere else.
if [ -n "${TMPDIR:-}" ]; then
  mkdir -p "$TMPDIR"
  echo "spill directory: $TMPDIR"
fi

# --- 0. is the dashboard code current? ---------------------------------------
# A check, deliberately not a `git pull`: updating podlake-web's code is a deploy,
# and a deploy should be somebody's decision rather than a side effect of the
# schedule. So this only reports — and it gates step 3 alone. Syncing the lake is
# useful work regardless of whether the dashboard can be published from it.
#
# It runs up front, rather than beside the refresh it guards, because the whole
# point is to say so early — and because the failure it replaces is a bad one.
# `refresh` pushes HEAD to the publish branch, so a stale checkout has its push
# rejected *after* the extract, leaving an unpushed commit behind. The run after
# that finds a clean tree, re-extracts, sees no change against its own local
# commit, and exits 0 — reporting success while nothing has published since.
echo "--- checking podlake-web"
cd "$WEB_DIR"
publish_blocked=""
if ! git fetch --quiet; then
  # Can't tell, so don't guess: carry on and let the push be the judge. A blip now
  # says little about a push some hours from now, and skipping a publish that
  # would have worked is the worse error.
  echo "could not fetch from origin — proceeding unverified"
else
  # Compared against the tracked branch rather than the publish branch, because
  # the question is whether the code about to run is current.
  upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2> /dev/null || true)
  if [ -z "$upstream" ]; then
    publish_blocked="not tracking a remote branch"
  elif ! git merge-base --is-ancestor "$upstream" HEAD; then
    publish_blocked="behind $upstream"
  else
    echo "up to date with $upstream"
  fi
fi
if [ -n "$publish_blocked" ]; then
  echo "$WEB_DIR is $publish_blocked"
  echo "the lake will still sync, but the dashboard will NOT be published"
fi

# --- 1. bring the lake up to date --------------------------------------------
# --log is podlake's unattended mode: timestamped per-resource progress and the
# end-of-run totals, with no progress bars. It goes to pipeline.log itself, so
# one file tells the whole run in order — and the failure mail's tail shows the
# step a sync died in, not just the `time` line after it. podlake's log handler
# appends, like the redirect above, so the two writers interleave by line
# rather than overwriting each other.
echo "--- podlake sync-all"
# cd, so podlake reads its own .env and resolves its relative data path.
cd "$PODLAKE_DIR"
time uv run podlake sync-all --log "$LOG_DIR/pipeline.log"

# --- 2. reclaim disk and apply the delete backlog ----------------------------
# Sync clears only as much backlog as its own writes need; this is what reclaims
# the space. It also leaves the data files with their deletes physically applied,
# so the extract below doesn't pay to merge tombstones on every query it runs.
echo "--- podlake compact"
time uv run podlake compact --log "$LOG_DIR/pipeline.log"

# --- 3. republish the dashboard's aggregates ---------------------------------
# The long pole: about 65 minutes against the full 13-institution lake, and it
# grows with the corpus. Rebuilds site/src/data/*.json from the lake, then commits
# and pushes ONLY if the numbers actually moved (a re-run always changes
# generated_at, which is not news). The push is what publishes: podlake-web's
# GitHub Actions workflow builds the site from those committed artifacts and
# deploys it to Pages. Nothing here touches Pages directly.
if [ -n "$publish_blocked" ]; then
  # Nonzero, so the EXIT trap above reports it. The lake is current and that is
  # worth having, but a dashboard that quietly stops updating is exactly what this
  # script exists to prevent.
  echo "--- NOT publishing: $WEB_DIR is $publish_blocked"
  echo "    to publish, update it and re-run: git -C $WEB_DIR pull --ff-only"
  echo "=== lake synced, dashboard not published $(date -u +%FT%TZ)"
  exit 1
fi

echo "--- podlake-web refresh"
cd "$WEB_DIR"
time uv run podlake-web refresh --catalog "$CATALOG" --branch "$PUBLISH_BRANCH"

echo "=== pipeline done $(date -u +%FT%TZ)"
