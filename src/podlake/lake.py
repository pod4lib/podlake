import logging
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from podlake.config import LAKE_ALIAS, Config, get_config
from podlake.storage import Storage

logger = logging.getLogger(__name__)

RECORDS_TABLE = "records"
META_TABLE = "record_meta"
STATE_TABLE = "harvest_state"


def connect(
    read_only: bool = False, config: Config | None = None
) -> duckdb.DuckDBPyConnection:
    """
    Connect to the DuckLake catalog for the active profile and return an open
    DuckDB connection with the lake attached and selected.

    Pass read_only=True for consumer-style access that cannot modify the lake.
    """
    config = config or get_config()

    con = duckdb.connect()
    _load_extensions(con, config)
    _configure_storage(con, config)

    logger.info("attaching ducklake (%s, read_only=%s)", config.profile, read_only)
    con.execute(config.attach_sql(read_only=read_only))
    con.execute(f"USE {LAKE_ALIAS}")

    # Spill to the same temp location as downloads (honors $TMPDIR), and cap
    # DuckDB's memory if configured, so a big merge overflows to disk instead of
    # ballooning until the OOM killer steps in.
    spill = tempfile.gettempdir().replace("'", "''")
    con.execute(f"SET temp_directory = '{spill}'")
    if config.memory_limit:
        limit = config.memory_limit.replace("'", "''")
        con.execute(f"SET memory_limit = '{limit}'")

    # Show DuckDB's progress bar for long-running statements — notably the
    # delete+insert upsert that `apply_resource` runs after conversion — so a
    # busy apply step isn't mistaken for a hang. Only in an interactive
    # terminal, to keep non-interactive runs (cron/`sync-all`) quiet.
    if sys.stdout.isatty():
        con.execute("SET enable_progress_bar = true")

    return con


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the tall `records` (EAV) table and the per-record `record_meta`
    table if they don't exist yet, each partitioned by org. The schema is fixed
    (see convert.RECORDS_SCHEMA / META_SCHEMA), so no column derivation is
    needed.
    """
    if _table_exists(con, RECORDS_TABLE):
        return

    con.execute(
        f"CREATE TABLE {RECORDS_TABLE} ("
        "org VARCHAR, pod_record_id VARCHAR, field_tag VARCHAR, field_seq INTEGER, "
        "ind1 VARCHAR, ind2 VARCHAR, subfield_code VARCHAR, subfield_seq INTEGER, "
        "value VARCHAR)"
    )
    con.execute(f"ALTER TABLE {RECORDS_TABLE} SET PARTITIONED BY (org)")

    con.execute(
        f"CREATE TABLE {META_TABLE} "
        "(org VARCHAR, pod_record_id VARCHAR, goldrush_key VARCHAR)"
    )
    con.execute(f"ALTER TABLE {META_TABLE} SET PARTITIONED BY (org)")
    logger.info("created %s + %s tables partitioned by org", RECORDS_TABLE, META_TABLE)


def load_pair(
    con: duckdb.DuckDBPyConnection,
    org: str,
    records_parquet: Path,
    meta_parquet: Path,
) -> int:
    """
    Load one organization's records + meta Parquet pair, replacing any existing
    rows for that org (idempotent). Returns the number of records loaded.
    """
    ensure_schema(con)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"DELETE FROM {RECORDS_TABLE} WHERE org = ?", [org])
        con.execute(f"DELETE FROM {META_TABLE} WHERE org = ?", [org])
        con.execute(
            f"INSERT INTO {RECORDS_TABLE} SELECT * FROM read_parquet(?)",
            [str(records_parquet)],
        )
        con.execute(
            f"INSERT INTO {META_TABLE} SELECT * FROM read_parquet(?)",
            [str(meta_parquet)],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    row = con.execute(
        f"SELECT count(*) FROM {META_TABLE} WHERE org = ?", [org]
    ).fetchone()
    loaded = row[0] if row else 0
    logger.info("loaded %s records for org=%s", loaded, org)
    return loaded


def ensure_state_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the `harvest_state` table if needed. It records the lastmod of the
    last ResourceSync resource processed per org (the sync cursor), so the next
    sync only processes newer resources.
    """
    # Stored as a naive UTC TIMESTAMP (not TIMESTAMPTZ, which would make DuckDB
    # require pytz on read); UTC is re-attached in get_cursor.
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {STATE_TABLE} "
        "(org VARCHAR, last_modified TIMESTAMP)"
    )


def get_cursor(con: duckdb.DuckDBPyConnection, org: str) -> datetime | None:
    """
    Return the lastmod (UTC) of the last resource processed for this org, or
    None if it has never been synced (so the next sync starts from the full
    dump).
    """
    ensure_state_table(con)
    row = con.execute(
        f"SELECT last_modified FROM {STATE_TABLE} WHERE org = ?", [org]
    ).fetchone()
    if not row or row[0] is None:
        return None
    return row[0].replace(tzinfo=UTC)


def all_cursors(con: duckdb.DuckDBPyConnection) -> dict[str, datetime]:
    """
    Return {org: last processed lastmod (UTC)} from harvest_state, or {} if the
    lake has never been synced. Unlike get_cursor this never creates the table,
    so it is safe to call on a read-only connection (used by `podlake status`).
    """
    if not _table_exists(con, STATE_TABLE):
        return {}
    rows = con.execute(f"SELECT org, last_modified FROM {STATE_TABLE}").fetchall()
    return {org: ts.replace(tzinfo=UTC) for org, ts in rows if ts is not None}


def _set_cursor(
    con: duckdb.DuckDBPyConnection, org: str, last_modified: datetime
) -> None:
    con.execute(f"DELETE FROM {STATE_TABLE} WHERE org = ?", [org])
    con.execute(
        f"INSERT INTO {STATE_TABLE} VALUES (?, ?)", [org, _naive_utc(last_modified)]
    )


def _naive_utc(dt: datetime) -> datetime:
    """Normalize a datetime to a naive UTC value for the TIMESTAMP column."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC)
    return dt.replace(tzinfo=None)


def _timed(
    con: duckdb.DuckDBPyConnection,
    label: str,
    sql: str,
    params: list | None = None,
) -> None:
    """
    Run one statement, logging a marker before it and the elapsed time after.

    `apply_resource` runs several statements in a single transaction with no
    progress of its own, so at INFO level (`sync --verbose`) these boundaries
    show which step is executing. The last unmatched "→" before a stall or a
    memory spike names the culprit — which is how the delete was first isolated
    from the insert.
    """
    logger.info("→ %s", label)
    start = time.perf_counter()
    if params is None:
        con.execute(sql)
    else:
        con.execute(sql, params)
    logger.info("← %s (%.1fs)", label, time.perf_counter() - start)


def _timed_org_delete(
    con: duckdb.DuckDBPyConnection, org: str, kind: str, table: str
) -> None:
    """
    Delete every row of `org` from `table`, one data file at a time.

    A single `DELETE ... WHERE org = ?` does retire the files whole (no delete
    files are written), but on the way it holds every matching row id in memory,
    outside DuckDB's buffer pool, so `memory_limit` doesn't cap it: measured
    peak RSS grew linearly, ~11 bytes/row with 10M-row files and ~40 with one
    300M-row file. Deleting each file's rowid range instead bounds the peak by
    the largest file (1.4 GB whether the org had 300M or 600M rows), and staying
    inside the caller's transaction keeps the replace atomic.

    DuckLake gives each data file the rowid range [row_id_start, row_id_start +
    record_count). The final org-wide DELETE is the backstop for any rows that
    range misses; when the per-file pass got them all it has nothing to hold.
    """
    files = con.execute(
        "SELECT f.row_id_start, f.record_count "
        f"FROM __ducklake_metadata_{LAKE_ALIAS}.ducklake_data_file f "
        f"JOIN __ducklake_metadata_{LAKE_ALIAS}.ducklake_table t USING (table_id) "
        f"JOIN __ducklake_metadata_{LAKE_ALIAS}.ducklake_file_partition_value p "
        "USING (data_file_id) "
        "WHERE t.table_name = ? AND t.end_snapshot IS NULL "
        "AND f.end_snapshot IS NULL AND f.row_id_start IS NOT NULL "
        "AND p.partition_value = ? "
        "ORDER BY f.row_id_start",
        [table, org],
    ).fetchall()
    label = f"{org} {kind}: delete {table}"
    logger.info("→ %s (%d data files)", label, len(files))
    start = time.perf_counter()
    for row_id_start, record_count in files:
        con.execute(
            f"DELETE FROM {table} WHERE org = ? AND rowid BETWEEN ? AND ?",
            [org, row_id_start, row_id_start + record_count - 1],
        )
    con.execute(f"DELETE FROM {table} WHERE org = ?", [org])
    logger.info("← %s (%.1fs)", label, time.perf_counter() - start)


def apply_resource(
    con: duckdb.DuckDBPyConnection,
    org: str,
    kind: str,
    payload: tuple[Path, Path] | list[str],
    last_modified: datetime,
) -> tuple[int, int]:
    """
    Apply one ResourceSync resource for an org in a single transaction (one
    DuckLake snapshot), then advance the org's sync cursor to `last_modified`:

    - "full": `payload` is a (records_parquet, meta_parquet) pair; replace all
      of the org's rows in both tables with it.
    - "delta": `payload` is a (records_parquet, meta_parquet) pair; upsert the
      records by pod_record_id (delete existing ids from both tables, insert
      the new versions).
    - "deletes": `payload` is a list of pod_record_ids to delete from both tables.

    Returns (changed_count, deleted_count).
    """
    ensure_schema(con)
    ensure_state_table(con)

    changed_count = deleted_count = 0
    con.execute("BEGIN TRANSACTION")
    try:
        if kind in ("full", "delta"):
            assert isinstance(payload, tuple)
            records_pq, meta_pq = payload
            if kind == "full":
                # A full dump is the org's complete record set, so it replaces
                # the org outright rather than upserting: records missing from
                # it are gone. An upsert's `IN (<every id>)` tombstones nearly
                # every row of the org in one transaction — for Stanford's
                # 10M-record dump that was OOM-killed at 31 GB.
                for table in (RECORDS_TABLE, META_TABLE):
                    _timed_org_delete(con, org, kind, table)
            else:
                # Deltas upsert by id, scoped to the org partition: without the
                # `org =` predicate DuckLake can't prune, so the delete scans
                # every org's data to find this resource's ids. Every id in the
                # resource belongs to `org`, so restricting to it is safe.
                ids_subquery = "(SELECT pod_record_id FROM read_parquet(?))"
                for table in (RECORDS_TABLE, META_TABLE):
                    _timed(
                        con,
                        f"{org} {kind}: delete {table}",
                        f"DELETE FROM {table} "
                        f"WHERE org = ? AND pod_record_id IN {ids_subquery}",
                        [org, str(meta_pq)],
                    )
            _timed(
                con,
                f"{org} {kind}: insert records",
                f"INSERT INTO {RECORDS_TABLE} SELECT * FROM read_parquet(?)",
                [str(records_pq)],
            )
            _timed(
                con,
                f"{org} {kind}: insert record_meta",
                f"INSERT INTO {META_TABLE} SELECT * FROM read_parquet(?)",
                [str(meta_pq)],
            )
            row = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(meta_pq)]
            ).fetchone()
            changed_count = row[0] if row else 0
        elif kind == "deletes":
            assert isinstance(payload, list)
            if payload:
                # Semi-join against the ids as a registered relation, like the
                # delta path reads ids from a Parquet subquery. A big deletes
                # file (200k+ ids) would otherwise be a giant literal
                # IN (?, ?, …) list — an enormous SQL string + bind. Register
                # the ids as a (zero-copy) Arrow table and reference it instead.
                con.register("delete_ids", pa.table({"pod_record_id": payload}))
                try:
                    id_subquery = "(SELECT pod_record_id FROM delete_ids)"
                    _timed(
                        con,
                        f"{org} deletes: delete records",
                        f"DELETE FROM {RECORDS_TABLE} "
                        f"WHERE org = ? AND pod_record_id IN {id_subquery}",
                        [org],
                    )
                    _timed(
                        con,
                        f"{org} deletes: delete record_meta",
                        f"DELETE FROM {META_TABLE} "
                        f"WHERE org = ? AND pod_record_id IN {id_subquery}",
                        [org],
                    )
                finally:
                    con.unregister("delete_ids")
                deleted_count = len(payload)
        else:
            raise ValueError(f"unknown resource kind: {kind}")

        _set_cursor(con, org, last_modified)
        _timed(con, f"{org} {kind}: commit", "COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    logger.info(
        "applied %s for org=%s: %s changed, %s deleted",
        kind,
        org,
        changed_count,
        deleted_count,
    )
    return changed_count, deleted_count


def publish(config: Config, dest_uri: str) -> tuple[str, str, int, int]:
    """
    Publish a file-catalog lake to an S3 bucket so read-only consumers can
    attach to it over s3://. Incrementally syncs the Parquet data under
    DATA_PATH (only new/changed files) and then uploads the catalog. Returns
    (catalog_key, data_prefix, uploaded, skipped).

    Data files are uploaded **before** the catalog so the published catalog only
    ever references files already present; DuckLake snapshot isolation keeps
    readers on the prior snapshot until the new catalog lands. Consumers attach
    with OVERRIDE_DATA_PATH because the catalog was written with a local
    DATA_PATH.
    """
    if not config.is_file_catalog:
        raise ValueError("publish requires a file-catalog lake, not a Postgres catalog")

    catalog_path = Path(config.catalog_uri)
    if not catalog_path.is_file():
        raise FileNotFoundError(f"catalog file not found: {catalog_path}")

    data_path = Path(config.data_path)
    data_prefix = data_path.name or "lake-data"
    catalog_key = catalog_path.name

    storage = Storage(dest_uri)
    # data first (immutable, additive; skips what's already uploaded)...
    uploaded, skipped = storage.sync_dir(data_path, data_prefix)
    # ...then the catalog last (it changes every publish).
    storage.upload_file(catalog_path, catalog_key)

    logger.info(
        "published to %s: %s data files uploaded, %s skipped (+ catalog)",
        dest_uri,
        uploaded,
        skipped,
    )
    return catalog_key, data_prefix, uploaded, skipped


def _snapshot_count(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute(
        f"SELECT count(*) FROM ducklake_snapshots('{LAKE_ALIAS}')"
    ).fetchone()
    return row[0] if row else 0


def pending_deletes(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """
    Return (delete_files, deleted_rows) still tombstoned but not yet physically
    removed from their data files. See the README's "delete backlog" section for
    why this number governs how much memory a DELETE needs.

    Errors are deliberately *not* swallowed: reporting an empty backlog when we
    simply failed to read it would silently disable the mid-sync safeguard that
    depends on this. An unbuilt lake is the one legitimate zero.
    """
    if not _table_exists(con, RECORDS_TABLE):
        return (0, 0)
    # DuckLake's bookkeeping lives in a side catalog, not in the lake itself
    # Only delete files on live data files count. Retiring a data file (as a
    # full dump's replace does) leaves its delete files marked live until
    # cleanup, but nothing ever reads them again, so they cost no memory.
    row = con.execute(
        "SELECT count(*), coalesce(sum(d.delete_count), 0) "
        f"FROM __ducklake_metadata_{LAKE_ALIAS}.ducklake_delete_file d "
        f"JOIN __ducklake_metadata_{LAKE_ALIAS}.ducklake_data_file f "
        "USING (data_file_id) "
        "WHERE d.end_snapshot IS NULL AND f.end_snapshot IS NULL"
    ).fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


def compact(
    con: duckdb.DuckDBPyConnection,
    *,
    days: int = 0,
    dry_run: bool = False,
    rewrite_deletes: float | None = None,
    max_files: int | None = None,
    max_passes: int = 10,
) -> dict[str, int]:
    """
    Reclaim disk in the attached lake. DuckLake is merge-on-read, so superseded
    rows — from deltas/deletes and re-imported full dumps — pile up on disk until
    this runs. It expires snapshots older than `days` days (0 = all but the
    current), compacts small Parquet files, then deletes the data files no longer
    referenced by a live snapshot. With dry_run=True nothing changes and the
    counts report what *would* be reclaimed.

    `rewrite_deletes` additionally rewrites data files to physically apply their
    accumulated deletes, for files where the deleted fraction is at least that
    value (0.05 = 5%). This is the only step that clears tombstoned rows out of
    *large* data files: `merge_adjacent_files` skips any file already at the
    target size, and DuckLake's own `rewrite_data_files` defaults to a 0.95
    threshold, so without this a big file's deletes accumulate forever. It
    rewrites real data, so it is much more expensive than the other steps — use
    `max_files` to bound one run and re-run to make progress incrementally.

    A single expire → merge → cleanup pass never fully compacts: merging creates
    new snapshots and freshly-superseded files that only become collectable once
    a *later* expire pass removes the snapshots referencing them. So the cleanup
    cycle repeats until a pass reclaims nothing (or `max_passes`). Returns a
    stats dict, including the number of cleanup `passes` run.
    """
    dry = "true" if dry_run else "false"
    older_than = f"now() - INTERVAL '{int(days)} days'"

    def run(fn: str, measure: str = "count(*)") -> int:
        # Materialize via CTAS so the side-effecting table function runs fully: a
        # bare count() can be optimized away, and fetching the functions'
        # TIMESTAMPTZ columns into Python would require pytz. The temp table keeps
        # the timestamps inside DuckDB.
        con.execute(f"CREATE OR REPLACE TEMP TABLE _compact AS SELECT * FROM {fn}")
        row = con.execute(f"SELECT {measure} FROM _compact").fetchone()
        return int(row[0]) if row else 0

    # the rewrite returns a row per table/compaction group, not per file
    FILES = "coalesce(sum(files_processed), 0)"

    before = _snapshot_count(con)
    delete_files_before, deleted_rows_before = pending_deletes(con)

    # Apply accumulated deletes first, so the cleanup cycle below can collect the
    # data + delete files the rewrite supersedes. No dry-run mode upstream.
    rewritten = 0
    if rewrite_deletes is not None and not dry_run:
        threshold = f"delete_threshold => {float(rewrite_deletes)}"
        logger.info(
            "rewriting data files with >=%s%% deletes (%s delete files, %s rows)",
            float(rewrite_deletes) * 100,
            f"{delete_files_before:,}",
            f"{deleted_rows_before:,}",
        )
        fn = f"ducklake_rewrite_data_files('{LAKE_ALIAS}', {threshold}"
        try:
            bounded = fn + (
                f", max_compacted_files => {int(max_files)})"
                if max_files is not None
                else ")"
            )
            rewritten = run(bounded, FILES)
        except duckdb.Error as e:
            # max_compacted_files was added to ducklake_rewrite_data_files after
            # some released versions; without it the rewrite can't be bounded, so
            # say so rather than silently doing the whole backlog in one run.
            if max_files is None or "max_compacted_files" not in str(e):
                raise
            logger.warning(
                "this DuckLake version does not support max_compacted_files — "
                "rewriting the whole backlog in one pass (this may take a long time)"
            )
            rewritten = run(fn + ")", FILES)

    totals = {"expired": 0, "merged": 0, "cleaned": 0, "orphaned": 0, "passes": 0}
    while totals["passes"] < max_passes:
        expired = run(
            f"ducklake_expire_snapshots('{LAKE_ALIAS}', "
            f"dry_run => {dry}, older_than => {older_than})"
        )
        # merge has no dry-run mode, so only compact for real runs
        merged = 0 if dry_run else run(f"ducklake_merge_adjacent_files('{LAKE_ALIAS}')")
        cleaned = run(
            f"ducklake_cleanup_old_files('{LAKE_ALIAS}', "
            f"dry_run => {dry}, cleanup_all => true)"
        )
        orphaned = run(
            f"ducklake_delete_orphaned_files('{LAKE_ALIAS}', "
            f"dry_run => {dry}, cleanup_all => true)"
        )
        totals["expired"] += expired
        totals["merged"] += merged
        totals["cleaned"] += cleaned
        totals["orphaned"] += orphaned
        totals["passes"] += 1
        # dry runs never mutate (so looping is pointless); real runs stop once a
        # pass leaves everything in place — the fixed point.
        if dry_run or expired + merged + cleaned + orphaned == 0:
            break

    con.execute("DROP TABLE IF EXISTS _compact")
    delete_files_after, deleted_rows_after = pending_deletes(con)

    stats = {
        **totals,
        "snapshots_before": before,
        "snapshots_after": _snapshot_count(con),
        "rewritten": rewritten,
        "delete_files_before": delete_files_before,
        "delete_files_after": delete_files_after,
        "deleted_rows_before": deleted_rows_before,
        "deleted_rows_after": deleted_rows_after,
    }
    logger.info("compact: %s", stats)
    return stats


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def _load_extensions(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    extensions = ["ducklake"]
    if config.is_postgres:
        extensions += ["postgres", "httpfs", "aws"]
    for ext in extensions:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")


def _configure_storage(con: duckdb.DuckDBPyConnection, config: Config) -> None:
    """
    With the postgres profile the DATA_PATH lives in S3, so register an S3
    secret that resolves credentials via DuckDB's credential_chain (standard
    AWS_* env vars, shared config, or an assumed role).
    """
    if config.is_postgres and config.data_path.startswith("s3://"):
        con.execute(
            "CREATE SECRET IF NOT EXISTS pod_s3 (TYPE s3, PROVIDER credential_chain)"
        )
