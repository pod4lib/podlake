import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Annotated

import duckdb
import humanize
import typer
from rich import print
from tqdm import tqdm

from podlake import lake, resourcesync
from podlake.config import Config, get_config
from podlake.convert import dump_to_parquet

app = typer.Typer()

MaxPendingDeletes = Annotated[
    int,
    typer.Option(
        "--max-pending-deletes",
        help="Apply accumulated deletes mid-sync whenever the backlog exceeds this "
        "many tombstoned rows (0 disables). Raise it on a machine with more RAM.",
    ),
]
Verbose = Annotated[
    bool,
    typer.Option(
        "--verbose",
        "-v",
        help="Log each apply step (delete/insert/commit) with timings, so a pause "
        "or a memory spike can be pinned to a specific statement.",
    ),
]
LogFile = Annotated[
    Path | None,
    typer.Option(
        "--log",
        help="Write progress to this file instead of the terminal, and turn off "
        "progress bars. Console stays quiet, so a cron run only mails you on "
        "failure.",
    ),
]


logger = logging.getLogger(__name__)

# Set by --log: progress goes to the log file and the console stays quiet, so a
# cron run only mails you when something actually fails.
_LOGGING_TO_FILE = False


def _setup_logging(verbose: bool, log: Path | None = None) -> None:
    """Route INFO logs to a file (--log) or the console (--verbose). force=True
    because basicConfig is a no-op if the root logger is already configured,
    which would silently swallow the output."""
    global _LOGGING_TO_FILE
    _LOGGING_TO_FILE = log is not None
    fmt = "%(asctime)s  %(message)s"
    if log is not None:
        logging.basicConfig(
            level=logging.INFO, format=fmt, filename=str(log), force=True
        )
    elif verbose:
        logging.basicConfig(level=logging.INFO, format=fmt, force=True)
    # INFO on the root logger otherwise pulls in httpx's line per request, which
    # buries our own progress under hundreds of "HTTP Request: GET ..." lines.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _connect_writable(config: Config | None = None) -> duckdb.DuckDBPyConnection:
    """Open a write connection, silencing DuckDB's progress bar when --log is in
    effect — connect() turns it on for any TTY, which would otherwise leave the
    console noisy despite the log file."""
    con = lake.connect(read_only=False, config=config)
    if _LOGGING_TO_FILE:
        con.execute("SET enable_progress_bar = false")
    return con


def _download_bar_hidden() -> bool:
    """Whether the download's own progress bar will be invisible — because we're
    logging to a file, or stderr isn't a terminal (cron, or piped through tee)."""
    return _LOGGING_TO_FILE or not sys.stderr.isatty()


def _progress(msg: str) -> None:
    """A progress line: into the log file with --log, otherwise onto stderr."""
    if _LOGGING_TO_FILE:
        logger.info(msg)
    else:
        typer.echo(msg, err=True)


def _elapsed(started: float) -> str:
    """Time since `started` (a perf_counter reading), e.g. "2 hours and 13 minutes"."""
    return humanize.precisedelta(
        time.perf_counter() - started, minimum_unit="seconds", format="%0.0f"
    )


def _summary(plain: str, rich_markup: str) -> None:
    """An end-of-command summary: logged with --log, printed otherwise."""
    if _LOGGING_TO_FILE:
        logger.info(plain)
    else:
        print(rich_markup)


# Backlog thresholds for the delete-rewrite. See the README's "delete backlog"
# section for why these matter; the numbers come from a 32GB box, where a 0.05
# rewrite bottomed out near 76M rows and a 1.1M-record delta died at 153M. The
# safe level scales with RAM and with how large the incoming resources are.
DEFAULT_MAX_PENDING_DELETES = 100_000_000
MID_SYNC_DELETE_THRESHOLD = 0.05
MIN_DELETE_THRESHOLD = 0.01


def _threshold_ladder() -> list[float]:
    """Rewrite thresholds to try in one pass, cheapest first, halving down to the
    minimum. A fixed list rather than a loop condition: the escalation is then
    bounded by construction, and cannot spin if a comparison is ever wrong."""
    ladder = []
    t = MID_SYNC_DELETE_THRESHOLD
    while t > MIN_DELETE_THRESHOLD and len(ladder) < 8:
        ladder.append(t)
        t /= 2
    ladder.append(MIN_DELETE_THRESHOLD)
    return ladder


THRESHOLD_LADDER = _threshold_ladder()

# Resources at least this large are held to a stricter backlog limit.
BIG_RESOURCE_BYTES = 100 * 1024 * 1024

# Backlog big enough that an unbounded rewrite is worth warning about first.
BIG_DELETE_BACKLOG = 500_000_000


@app.command()
def config():
    """
    Show the resolved configuration for the active profile (set with the
    PODLAKE_PROFILE environment variable) and verify that the DuckLake catalog
    can be attached.
    """
    cfg = get_config()

    print(f"[bold]podlake configuration[/bold] ({cfg.profile})")
    for key, value in cfg.describe().items():
        print(f"  {key} = {value}")

    try:
        con = lake.connect(read_only=True, config=cfg)
        has_table = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            f"WHERE table_name = '{lake.RECORDS_TABLE}'"
        ).fetchone()
        if has_table and has_table[0]:
            row = con.execute(f"SELECT count(*) FROM {lake.RECORDS_TABLE}").fetchone()
            count = row[0] if row else 0
            print(f"[green]✓ connected to the DuckLake ({count} records)[/green]")
        else:
            print(
                "[green]✓ connected to the DuckLake[/green] "
                "[yellow](no records yet — run `podlake sync`)[/yellow]"
            )
        con.close()
    except duckdb.Error as e:
        # A missing catalog just means the lake hasn't been built yet, which is
        # a normal state, not a configuration error.
        print(
            "[yellow]! could not attach to the DuckLake — it may not be built "
            f"yet (run `podlake sync`): {e}[/yellow]"
        )


@app.command()
def streams(
    org_name: Annotated[
        str | None, typer.Argument(help="Limit to one organization")
    ] = None,
):
    """
    List POD organizations (ResourceSync streams) and how many resources each
    has (full dump + deltas + deletes), with total download size.
    """
    get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    for name, url in sorted(found.items()):
        resources = resourcesync.get_resources(url)
        total = sum(r.length for r in resources)
        print(
            f"- [bold]{name}[/bold]: {len(resources)} resources, "
            f"{humanize.naturalsize(total)}"
        )


@app.command()
def status(
    org_name: Annotated[
        str | None, typer.Argument(help="Limit to one organization")
    ] = None,
    list_files: Annotated[
        bool,
        typer.Option("--list", help="List each resource with its processed status"),
    ] = False,
):
    """
    Show, per organization, which ResourceSync resources have already been
    synced and which are still pending (newer than the org's cursor). Name a
    single organization, or pass --list, to see each resource individually.
    """
    config = get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    cursors = _load_cursors(config)
    total_done = total_pending = done_bytes = pending_bytes = 0
    for name, url in sorted(found.items()):
        cursor = cursors.get(name)
        resources = resourcesync.get_resources(url)
        pending = [r for r in resources if cursor is None or r.lastmod > cursor]
        done = len(resources) - len(pending)
        org_all_bytes = sum(r.length for r in resources)
        org_pending_bytes = sum(r.length for r in pending)

        total_done += done
        total_pending += len(pending)
        done_bytes += org_all_bytes - org_pending_bytes
        pending_bytes += org_pending_bytes

        when = cursor.date().isoformat() if cursor else "never synced"
        print(
            f"- [bold]{name}[/bold]: {done} processed, "
            f"{len(pending)} pending ({humanize.naturalsize(org_pending_bytes)}), "
            f"{_pct(org_all_bytes - org_pending_bytes, org_all_bytes)} by size; "
            f"cursor {when}"
        )
        if list_files or org_name:
            for r in resources:
                processed = cursor is not None and r.lastmod <= cursor
                mark = "[green]✓[/green]" if processed else "[yellow]→[/yellow]"
                name_only = r.url.rstrip("/").split("/")[-1]
                print(
                    f"    {mark} {r.lastmod.date().isoformat()}  {r.kind:<7} "
                    f"{humanize.naturalsize(r.length):>10}  {name_only}"
                )

    if len(found) > 1:
        print(
            f"[bold]total[/bold]: {total_done} processed, "
            f"{total_pending} pending ({humanize.naturalsize(pending_bytes)}), "
            f"{_pct(done_bytes, done_bytes + pending_bytes)} by size"
        )


def _pct(done_bytes: int, total_bytes: int) -> str:
    """Percent of bytes processed, floored so it reads 100% only when truly done."""
    if total_bytes == 0:
        return "100%"
    return f"{int(100 * done_bytes / total_bytes)}%"


def _load_cursors(config: Config) -> dict:
    """
    Per-org sync cursors, read read-only.

    Returns {} when the lake genuinely hasn't been built yet, so `status` shows
    everything as pending (which is correct). If the lake *does* exist but can't
    be read — e.g. it's locked by a running sync — this errors out rather than
    silently reporting every resource as never-synced.
    """
    if config.is_file_catalog and not Path(config.catalog_uri).exists():
        return {}
    try:
        con = lake.connect(read_only=True, config=config)
    except duckdb.Error as e:
        typer.echo(
            f"could not read the lake (it may be locked by a running sync): {e}",
            err=True,
        )
        raise typer.Exit(code=1) from e
    try:
        return lake.all_cursors(con)
    finally:
        con.close()


@app.command()
def compact(
    older_than_days: Annotated[
        int,
        typer.Option(
            "--older-than-days",
            help="Expire snapshots older than this many days (0 = all but the current).",
        ),
    ] = 0,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Report what would be reclaimed without changing anything.",
        ),
    ] = False,
    delete_threshold: Annotated[
        float,
        typer.Option(
            "--delete-threshold",
            help="Rewrite data files that are at least this fraction deleted, to "
            "physically apply their accumulated deletes (0.1 = 10%). Lower reclaims "
            "more but costs much more per row reclaimed.",
            min=0.0,
            max=1.0,
        ),
    ] = 0.1,
    no_rewrite_deletes: Annotated[
        bool,
        typer.Option(
            "--no-rewrite-deletes",
            help="Skip applying accumulated deletes, for a quick disk-only pass. "
            "Note this leaves tombstoned rows in place, which makes later DELETEs "
            "progressively more expensive.",
        ),
    ] = False,
    max_files: Annotated[
        int | None,
        typer.Option(
            "--max-files",
            help="Cap how many files the delete-rewrite handles in one run, so a "
            "huge backlog can be worked through a chunk at a time. (Requires a "
            "DuckLake version that supports max_compacted_files.)",
        ),
    ] = None,
    log: LogFile = None,
):
    """
    Reclaim disk space and keep writes fast. DuckLake is merge-on-read, so
    superseded rows from deltas/deletes and re-imported full dumps accumulate on
    disk until this runs: it expires old snapshots, compacts small Parquet files,
    applies accumulated deletes, and removes data files no longer referenced by a
    live snapshot.

    Applying deletes matters for more than disk. Every DELETE loads the full
    delete history of each data file it opens, so an unapplied backlog makes
    deltas progressively slower and more memory-hungry until they fail outright.
    Merging small files does not clear it — that is what the delete-rewrite is
    for, and why it is on by default.
    """
    _setup_logging(False, log)
    config = get_config()
    con = _connect_writable(config)
    try:
        if not no_rewrite_deletes and not dry_run and max_files is None:
            _, pending_rows = lake.pending_deletes(con)
            if pending_rows > BIG_DELETE_BACKLOG:
                _summary(
                    f"{pending_rows:,} tombstoned rows to apply — this may take a "
                    "long time and cannot be interrupted for partial credit",
                    f"[yellow]{pending_rows:,} tombstoned rows to apply — this may "
                    "take a long time, and cannot be interrupted for partial credit. "
                    "Consider --max-files, or a higher --delete-threshold first.[/yellow]",
                )
        s = lake.compact(
            con,
            days=older_than_days,
            dry_run=dry_run,
            rewrite_deletes=None if no_rewrite_deletes else delete_threshold,
            max_files=max_files,
        )
    finally:
        con.close()

    if dry_run:
        line = (
            f"dry run — would expire {s['expired']} snapshot(s) and delete "
            f"{s['cleaned']} old + {s['orphaned']} orphaned data files "
            f"(snapshots stay at {s['snapshots_before']})"
        )
    else:
        line = (
            f"snapshots {s['snapshots_before']} → {s['snapshots_after']} "
            f"in {s['passes']} pass(es); "
            f"expired {s['expired']}, merged {s['merged']}, "
            f"deleted {s['cleaned']} old + {s['orphaned']} orphaned data files"
        )
    _summary(line, line)
    backlog = (
        f"pending deletes: {s['delete_files_before']:,} files / "
        f"{s['deleted_rows_before']:,} rows → "
        f"{s['delete_files_after']:,} files / {s['deleted_rows_after']:,} rows"
        + (f" ({s['rewritten']} file(s) rewritten)" if s["rewritten"] else "")
    )
    _summary(backlog, backlog)


@app.command()
def sync(
    org_name: Annotated[str, typer.Argument(help="Organization name")],
    batch_size: Annotated[
        int,
        typer.Option(
            help="Records buffered per Parquet row group. Lower it to reduce "
            "peak memory on constrained machines."
        ),
    ] = 100_000,
    limit: Annotated[
        int | None,
        typer.Option(help="Limit records per resource (useful for testing)"),
    ] = None,
    max_pending_deletes: MaxPendingDeletes = DEFAULT_MAX_PENDING_DELETES,
    verbose: Verbose = False,
    log: LogFile = None,
):
    """
    Sync one organization from POD ResourceSync into the DuckLake. Processes
    every resource (full dump + deltas + deletes) newer than the org's cursor:
    the first run does a full initial load, later runs apply only new deltas.
    """
    _setup_logging(verbose, log)
    get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    con = _connect_writable()
    try:
        for name, url in found.items():
            changed, deleted, n = _sync_org(
                con, name, url, batch_size, limit, max_pending_deletes
            )
            _summary(
                f"{name}: {n} resources processed, {changed} changed, {deleted} deleted",
                f"[bold]{name}[/bold]: {n} resources processed, "
                f"{changed} changed, {deleted} deleted",
            )
    finally:
        con.close()


@app.command()
def sync_all(
    batch_size: Annotated[
        int,
        typer.Option(
            help="Records buffered per Parquet row group. Lower it to reduce "
            "peak memory on constrained machines."
        ),
    ] = 100_000,
    max_pending_deletes: MaxPendingDeletes = DEFAULT_MAX_PENDING_DELETES,
    verbose: Verbose = False,
    log: LogFile = None,
):
    """
    Sync every organization from POD ResourceSync into the DuckLake, one at a
    time. Each resource is its own transaction (DuckLake snapshot), so an
    interrupted run resumes cleanly from where it left off.
    """
    _setup_logging(verbose, log)
    get_config()

    started = time.monotonic()
    orgs = total_resources = total_changed = total_deleted = 0

    con = _connect_writable()
    try:
        for name, url in sorted(resourcesync.get_streams().items()):
            changed, deleted, n = _sync_org(
                con, name, url, batch_size, None, max_pending_deletes
            )
            _summary(
                f"{name}: {n} resources processed, {changed} changed, {deleted} deleted",
                f"[bold]{name}[/bold]: {n} resources processed, "
                f"{changed} changed, {deleted} deleted",
            )
            if n:
                orgs += 1
            total_resources += n
            total_changed += changed
            total_deleted += deleted
    finally:
        con.close()

    # A one-line verdict for the whole run, so a cron log (or --log file) can be
    # skimmed without reading every per-org line.
    elapsed = humanize.naturaldelta(time.monotonic() - started)
    line = (
        f"total: {total_resources:,} resources from {orgs} "
        f"organization{'' if orgs == 1 else 's'}, "
        f"{total_changed:,} records changed, {total_deleted:,} deleted, in {elapsed}"
    )
    _summary(line, f"[bold]{line}[/bold]")


def _delete_limit_for(resource_bytes: int, max_pending: int, floor: int) -> int:
    """
    The backlog we're willing to carry into applying a resource of this size.

    What kills a sync is a *large* resource meeting a moderate backlog: the
    delete pays for the tombstones it creates *and* every tombstone already on
    the files it opens. Small deltas survive a backlog a big one cannot, so hold
    big ones tighter. Sizes come from the ResourceSync manifest, so this is known
    before anything is downloaded.

    Never demand less than `floor`, the backlog a rewrite has already proven it
    cannot get below: asking for the impossible just burns compaction cycles.
    """
    limit = max_pending
    if resource_bytes >= BIG_RESOURCE_BYTES:
        limit = max(1, max_pending // 2)
    return max(limit, floor)


def _maybe_apply_deletes(
    con: duckdb.DuckDBPyConnection,
    org: str,
    pos: str,
    resource_bytes: int,
    max_pending: int,
    state: dict,
) -> None:
    """
    Apply the accumulated delete backlog before it makes the next apply too
    expensive. See the README's "delete backlog" section for the why.

    Each rewrite threshold has a floor — it only touches files past that fraction
    deleted — so when one pass comes up short we rewrite *harder* rather than
    accept a bigger backlog, which would only defer the failure to a larger
    resource. Once the lowest threshold can't help either, the backlog is
    genuinely irreducible: record it so later resources stop paying for
    compactions that cannot work.
    """
    if not max_pending:
        return
    limit = _delete_limit_for(resource_bytes, max_pending, state.get("floor", 0))
    _, pending = lake.pending_deletes(con)
    if pending <= limit:
        return

    if pending > BIG_DELETE_BACKLOG:
        _progress(
            f"  {pos} {org}: {pending:,} tombstoned rows to apply — this may take a "
            "long time and cannot be interrupted for partial credit"
        )

    for threshold in THRESHOLD_LADDER:
        _progress(
            f"  {pos} {org}: {pending:,} tombstoned rows pending (> {limit:,}), "
            f"applying them (threshold {threshold})…"
        )
        s = lake.compact(con, days=0, rewrite_deletes=threshold)
        pending = s["deleted_rows_after"]
        _progress(
            f"  {pos} {org}: backlog {s['deleted_rows_before']:,} → {pending:,} rows "
            f"({s['rewritten']} file(s) rewritten)"
        )
        if pending <= limit:
            return

    state["floor"] = pending
    _progress(
        f"  {pos} {org}: {pending:,} tombstoned rows could not be reclaimed even at "
        f"threshold {MIN_DELETE_THRESHOLD}; continuing, and not retrying below this "
        "level"
    )


def _sync_org(
    con: duckdb.DuckDBPyConnection,
    org: str,
    resourcelist_url: str,
    batch_size: int,
    limit: int | None,
    max_pending_deletes: int = DEFAULT_MAX_PENDING_DELETES,
) -> tuple[int, int, int]:
    cursor = lake.get_cursor(con, org)
    resources = resourcesync.get_resources(resourcelist_url)
    pending = [r for r in resources if cursor is None or r.lastmod > cursor]

    total = len(pending)
    if total:
        size = humanize.naturalsize(sum(r.length for r in pending))
        _progress(
            f"{org}: {total} resource{'' if total == 1 else 's'} to sync ({size})"
        )

    total_changed = total_deleted = 0
    delete_state: dict = {}
    for i, resource in enumerate(pending, 1):
        pos = f"[{i}/{total}]"
        # Clear the backlog *before* applying, while we still know how big this
        # resource is — a large one gets a stricter limit than a small one.
        _maybe_apply_deletes(
            con, org, pos, resource.length, max_pending_deletes, delete_state
        )
        with tempfile.TemporaryDirectory() as tmp:
            if _download_bar_hidden():
                _progress(
                    f"  {pos} {org} {resource.kind}: downloading "
                    f"{humanize.naturalsize(resource.length)}…"
                )
            if resource.kind == "deletes":
                del_path = Path(tmp) / "deletes.txt"
                resourcesync.download(
                    resource.url,
                    del_path,
                    fixity=resource.fixity,
                    desc=f"{pos} {org} {resource.kind}: downloading",
                    quiet=_LOGGING_TO_FILE,
                )
                ids = [f"{org}:{rid}" for rid in resourcesync.read_delete_ids(del_path)]
                _progress(
                    f"  {pos} {org} deletes: removing {len(ids):,} records from the lake…"
                )
                _, deleted = lake.apply_resource(
                    con, org, "deletes", ids, resource.lastmod
                )
                total_deleted += deleted
            else:
                suffix = ".xml.gz" if resource.url.endswith(".gz") else ".xml"
                dl_path = Path(tmp) / f"resource{suffix}"
                started = time.perf_counter()
                resourcesync.download(
                    resource.url,
                    dl_path,
                    fixity=resource.fixity,
                    desc=f"{pos} {org} {resource.kind}: downloading",
                    quiet=_LOGGING_TO_FILE,
                )
                # Timed separately because the two are easy to confuse in the
                # log: a 6.4 GB full dump downloads in about a minute and then
                # converts for two hours.
                if _download_bar_hidden():
                    _progress(
                        f"  {pos} {org} {resource.kind}: downloaded in "
                        f"{_elapsed(started)}, converting to Parquet…"
                    )
                started = time.perf_counter()
                records_pq = Path(tmp) / "records.parquet"
                meta_pq = Path(tmp) / "meta.parquet"
                # Track the count ourselves rather than reading progress.n:
                # update() is a no-op on a bar disabled by --log, so n would
                # stay 0 in exactly the runs whose log we go back and read.
                seen = 0
                with tqdm(
                    desc=f"{pos} {org} {resource.kind}",
                    unit=" records",
                    smoothing=0.01,
                    disable=_LOGGING_TO_FILE,
                ) as progress:

                    def on_record(count: int, bar: tqdm = progress) -> None:
                        nonlocal seen
                        seen = count
                        bar.update(1)

                    dump_to_parquet(
                        org,
                        dl_path,
                        records_pq,
                        meta_pq,
                        batch_size=batch_size,
                        on_record=on_record,
                        limit=limit,
                    )
                # apply_resource runs a delete+insert upsert with no progress of
                # its own; announce it so the wait isn't mistaken for a stall.
                _progress(
                    f"  {pos} {org} {resource.kind}: converted in "
                    f"{_elapsed(started)}; updating the lake with {seen:,} records…"
                )
                changed, _ = lake.apply_resource(
                    con, org, resource.kind, (records_pq, meta_pq), resource.lastmod
                )
                total_changed += changed
    return total_changed, total_deleted, total


@app.command()
def fetch(
    org_name: Annotated[str, typer.Argument(help="Organization name")],
    output_dir: Annotated[
        Path,
        typer.Argument(help="Directory to write Parquet files", file_okay=False),
    ],
    full_only: Annotated[
        bool,
        typer.Option(help="Fetch only the base full dump (skip deltas and deletes)"),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(
            help="Records buffered per Parquet row group. Lower it to reduce "
            "peak memory on constrained machines."
        ),
    ] = 100_000,
    limit: Annotated[
        int | None,
        typer.Option(help="Limit records per resource (useful for testing)"),
    ] = None,
):
    """
    Download and convert an organization's ResourceSync dumps to Parquet files
    on disk, WITHOUT loading them into the DuckLake. Useful for inspecting the
    raw converted data (schema, column sparsity, sizes) without paying for a
    load. Use --full-only for just the base full dump.
    """
    get_config()
    found = resourcesync.get_streams(org_name)
    if not found:
        typer.echo(f"No ResourceSync stream found for {org_name}", err=True)
        raise typer.Exit(code=1)

    output_dir.mkdir(parents=True, exist_ok=True)
    for name, url in found.items():
        written, total = _fetch_org(name, url, output_dir, full_only, batch_size, limit)
        print(
            f"[bold]{name}[/bold]: wrote {written} files "
            f"({humanize.naturalsize(total)}) to {output_dir}"
        )


def _fetch_org(
    org: str,
    resourcelist_url: str,
    output_dir: Path,
    full_only: bool,
    batch_size: int,
    limit: int | None,
) -> tuple[int, int]:
    resources = resourcesync.get_resources(resourcelist_url)
    if full_only:
        resources = [r for r in resources if r.kind == "full"]

    written = total_bytes = 0
    for resource in resources:
        name = resource.url.rstrip("/").split("/")[-1]
        if resource.kind == "deletes":
            dest = output_dir / name
            resourcesync.download(resource.url, dest, fixity=resource.fixity)
            total_bytes += dest.stat().st_size
            written += 1
            continue

        stem = name.removesuffix(".gz").removesuffix(".xml")
        records_out = output_dir / f"{stem}.records.parquet"
        meta_out = output_dir / f"{stem}.meta.parquet"
        with tempfile.TemporaryDirectory() as tmp:
            suffix = ".xml.gz" if resource.url.endswith(".gz") else ".xml"
            dl_path = Path(tmp) / f"resource{suffix}"
            resourcesync.download(resource.url, dl_path, fixity=resource.fixity)
            with tqdm(
                desc=f"{org} {resource.kind}", unit=" records", smoothing=0.01
            ) as progress:
                dump_to_parquet(
                    org,
                    dl_path,
                    records_out,
                    meta_out,
                    batch_size=batch_size,
                    on_record=lambda _: progress.update(1),
                    limit=limit,
                )
        total_bytes += records_out.stat().st_size + meta_out.stat().st_size
        written += 2

    return written, total_bytes


@app.command()
def load(
    records_parquet: Annotated[
        Path,
        typer.Argument(
            help="A `*.records.parquet` file produced by `fetch`", exists=True
        ),
    ],
    meta_parquet: Annotated[
        Path | None,
        typer.Option(
            help="The matching meta Parquet (defaults to the sibling `*.meta.parquet`)"
        ),
    ] = None,
    org: Annotated[
        str | None,
        typer.Option(help="Organization name (defaults to the filename prefix)"),
    ] = None,
):
    """
    Load a records + meta Parquet pair produced by `fetch` into the DuckLake,
    replacing that org's rows (whole-org replace, so safe to re-run). Most
    ingestion should use `sync`; this is a manual escape hatch for a fetched
    full dump.
    """
    get_config()

    name = records_parquet.name
    if ".records.parquet" not in name:
        typer.echo("expected a `*.records.parquet` file", err=True)
        raise typer.Exit(code=1)
    meta = meta_parquet or records_parquet.with_name(
        name.replace(".records.parquet", ".meta.parquet")
    )
    if not meta.is_file():
        typer.echo(f"meta Parquet not found: {meta}", err=True)
        raise typer.Exit(code=1)
    org_name = org or name.split("-")[0]

    con = lake.connect(read_only=False)
    try:
        count = lake.load_pair(con, org_name, records_parquet, meta)
        print(f"loaded [bold]{count}[/bold] records for {org_name}")
    finally:
        con.close()


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="A read-only SQL query to run")],
):
    """
    Run a read-only SQL query against the DuckLake and print the results. Useful
    for quick checks; the unified table is named `records`.
    """
    get_config()

    con = lake.connect(read_only=True)
    try:
        con.sql(sql).show()
    finally:
        con.close()


@app.command()
def publish(
    dest: Annotated[
        str | None,
        typer.Argument(
            help="s3://bucket/prefix to publish to "
            "(defaults to the PODLAKE_PUBLISH_URL environment variable)"
        ),
    ] = None,
):
    """
    Publish a file-catalog lake to an S3 bucket so read-only consumers can
    attach to it over s3://. Incrementally syncs new/changed Parquet data (skips
    files already in the bucket) and uploads the catalog.
    """
    cfg = get_config()

    target = dest or cfg.publish_url
    if not target:
        typer.echo(
            "No publish destination: pass an s3:// URL or set PODLAKE_PUBLISH_URL",
            err=True,
        )
        raise typer.Exit(code=1)
    if not cfg.is_file_catalog:
        typer.echo(
            "publish requires a file-catalog lake, not a Postgres catalog", err=True
        )
        raise typer.Exit(code=1)

    catalog_key, data_prefix, uploaded, skipped = lake.publish(cfg, target)
    print(
        f"published to {target}: [bold]{uploaded}[/bold] data files uploaded, "
        f"{skipped} unchanged skipped (+ catalog)"
    )

    base = target.rstrip("/")
    print("\nConsumers can attach read-only with:")
    print("  INSTALL ducklake; INSTALL httpfs;")
    print(f"  ATTACH 'ducklake:{base}/{catalog_key}' AS podlake")
    print(
        f"    (DATA_PATH '{base}/{data_prefix}/', READ_ONLY, OVERRIDE_DATA_PATH true);"
    )


if __name__ == "__main__":
    app()
