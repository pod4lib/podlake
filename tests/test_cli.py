import datetime
import gzip

import duckdb
from typer.testing import CliRunner

from podlake import lake, resourcesync
from podlake.cli import app
from podlake.config import get_config
from podlake.resourcesync import Resource

runner = CliRunner()

COLLECTION = (
    '<collection xmlns="http://www.loc.gov/MARC21/slim">'
    "<record><leader>00000nam a2200000 a 4500</leader>"
    '<controlfield tag="001">a1</controlfield>'
    '<datafield tag="245" ind1="0" ind2="0">'
    '<subfield code="a">One</subfield></datafield></record>'
    "<record><leader>00000nam a2200000 a 4500</leader>"
    '<controlfield tag="001">a2</controlfield>'
    '<datafield tag="245" ind1="0" ind2="0">'
    '<subfield code="a">Two</subfield></datafield></record>'
    "</collection>"
)
BASE = "https://pod.stanford.edu/file"


def _resources():
    ts = datetime.datetime(2026, 2, 11, tzinfo=datetime.UTC)
    return [
        Resource(
            f"{BASE}/1/brown-2026-02-11-full-marcxml.xml.gz",
            "application/gzip",
            1,
            "",
            ts,
            "full",
        ),
        Resource(
            f"{BASE}/2/brown-2026-02-12-delta-marcxml.xml.gz",
            "application/gzip",
            1,
            "",
            ts,
            "delta",
        ),
        Resource(
            f"{BASE}/3/brown-2026-02-12-delta-deletes.del.txt",
            "text/plain",
            1,
            "",
            ts,
            "deletes",
        ),
    ]


def _fake_download(url, path, fixity=None, desc=None, quiet=False):
    if url.endswith(".del.txt"):
        path.write_text("a2\n")  # delete record a2
    else:
        with gzip.open(path, "wb") as fh:
            fh.write(COLLECTION.encode())
    return path


def _patch(monkeypatch):
    monkeypatch.setattr(
        resourcesync, "get_streams", lambda name=None: {"brown": f"{BASE}/rl"}
    )
    monkeypatch.setattr(resourcesync, "get_resources", lambda url: _resources())
    monkeypatch.setattr(resourcesync, "download", _fake_download)


def test_fetch_writes_records_and_meta(tmp_path, monkeypatch):
    _patch(monkeypatch)
    out = tmp_path / "out"

    result = runner.invoke(app, ["fetch", "brown", str(out)])
    assert result.exit_code == 0, result.output

    assert (out / "brown-2026-02-11-full-marcxml.records.parquet").is_file()
    assert (out / "brown-2026-02-11-full-marcxml.meta.parquet").is_file()
    assert (out / "brown-2026-02-12-delta-marcxml.records.parquet").is_file()
    assert (out / "brown-2026-02-12-delta-deletes.del.txt").is_file()

    con = duckdb.connect()
    meta = out / "brown-2026-02-11-full-marcxml.meta.parquet"
    ids = con.execute(
        f"SELECT pod_record_id FROM read_parquet('{meta}') ORDER BY pod_record_id"
    ).fetchall()
    assert [i[0] for i in ids] == ["brown:a1", "brown:a2"]

    # fetch never builds a lake
    assert not list(tmp_path.rglob("*.ducklake"))


def test_fetch_full_only(tmp_path, monkeypatch):
    _patch(monkeypatch)
    out = tmp_path / "out"

    result = runner.invoke(app, ["fetch", "brown", str(out), "--full-only"])
    assert result.exit_code == 0, result.output

    names = sorted(p.name for p in out.iterdir())
    assert names == [
        "brown-2026-02-11-full-marcxml.meta.parquet",
        "brown-2026-02-11-full-marcxml.records.parquet",
    ]


def test_status_reports_pending_then_processed(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    # before any sync: lake not built, so everything is pending
    before = runner.invoke(app, ["status", "brown"])
    assert before.exit_code == 0, before.output
    assert "0 processed" in before.output
    assert "3 pending" in before.output
    assert "never synced" in before.output

    # after syncing all three resources, they all count as processed
    assert runner.invoke(app, ["sync", "brown"]).exit_code == 0
    after = runner.invoke(app, ["status", "brown"])
    assert after.exit_code == 0, after.output
    assert "3 processed" in after.output
    assert "0 pending" in after.output


def test_status_totals_and_percent_across_orgs(tmp_path, monkeypatch):
    monkeypatch.setattr(
        resourcesync,
        "get_streams",
        lambda name=None: {"brown": f"{BASE}/rl", "cornell": f"{BASE}/rl"},
    )
    monkeypatch.setattr(resourcesync, "get_resources", lambda url: _resources())
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0, result.output
    # 2 orgs x 3 resources, none synced -> total 6 pending, 0% done by size
    assert "total" in result.output
    assert "6 pending" in result.output
    assert "0% by size" in result.output


def test_status_errors_when_lake_exists_but_unreadable(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    # a catalog file that exists but isn't a valid DuckLake -> attach fails
    catalog = tmp_path / "podlake.ducklake"
    catalog.write_text("not a real ducklake catalog")
    monkeypatch.setenv("PODLAKE_CATALOG", str(catalog))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["status", "brown"])
    # errors out instead of silently reporting everything as "never synced"
    assert result.exit_code == 1
    assert "never synced" not in result.output


def test_compact_dry_run_command(tmp_path, monkeypatch):
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["compact", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "dry run" in result.output


def test_compact_applies_deletes_by_default(tmp_path, monkeypatch):
    """The delete-rewrite is on by default: leaving it opt-in is how a backlog
    silently accumulates until DELETEs stop fitting in memory."""
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    config = get_config()
    con = lake.connect(read_only=False, config=config)
    lake.ensure_schema(con)
    con.execute(
        "INSERT INTO records SELECT 'x', 'x:' || i, '245', 1, '1', '0', 'a', 0, "
        "'t' || i FROM range(50000) s(i)"
    )
    con.execute(
        "DELETE FROM records WHERE org = 'x' "
        "AND (CAST(substr(pod_record_id, 3) AS INTEGER) % 5) = 0"
    )
    assert lake.pending_deletes(con)[1] > 0
    con.close()

    # --no-rewrite-deletes leaves the backlog in place
    result = runner.invoke(app, ["compact", "--no-rewrite-deletes"])
    assert result.exit_code == 0, result.output
    con = lake.connect(read_only=True, config=config)
    assert lake.pending_deletes(con)[1] > 0
    con.close()

    # the default run applies it
    result = runner.invoke(app, ["compact"])
    assert result.exit_code == 0, result.output
    assert "pending deletes" in result.output
    con = lake.connect(read_only=True, config=config)
    assert lake.pending_deletes(con) == (0, 0)
    assert con.execute("SELECT count(*) FROM records").fetchone() == (40000,)
    con.close()


def test_sync_applies_deletes_when_backlog_exceeds_threshold(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    # a threshold of 1 row makes every resource trip the mid-sync check; the sync
    # must still finish and leave the lake correct
    result = runner.invoke(app, ["sync", "brown", "--max-pending-deletes", "1"])
    assert result.exit_code == 0, result.output

    con = lake.connect(read_only=True, config=get_config())
    ids = [
        r[0]
        for r in con.execute(
            "SELECT pod_record_id FROM record_meta ORDER BY pod_record_id"
        ).fetchall()
    ]
    assert ids == ["brown:a1"]
    con.close()


def test_sync_loads_into_lake(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["sync", "brown"])
    assert result.exit_code == 0, result.output

    con = lake.connect(read_only=True, config=get_config())
    # full loaded a1,a2; the deletes resource removed a2 -> only a1 remains
    ids = [
        r[0]
        for r in con.execute(
            "SELECT pod_record_id FROM record_meta ORDER BY pod_record_id"
        ).fetchall()
    ]
    assert ids == ["brown:a1"]
    con.close()


def test_first_sync_starts_at_latest_full_dump(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    def at(day):
        return datetime.datetime(2026, 2, day, tzinfo=datetime.UTC)

    # an old full + its deletes, then a newer full (a1 + a2) + a delta (a1, a2)
    resources = [
        Resource(f"{BASE}/1/brown-2026-02-01-full-marcxml.xml.gz",
                 "application/gzip", 1, "", at(1), "full"),
        Resource(f"{BASE}/2/brown-2026-02-02-delta-deletes.del.txt",
                 "text/plain", 1, "", at(2), "deletes"),
        Resource(f"{BASE}/3/brown-2026-02-10-full-marcxml.xml.gz",
                 "application/gzip", 1, "", at(10), "full"),
        Resource(f"{BASE}/4/brown-2026-02-11-delta-marcxml.xml.gz",
                 "application/gzip", 1, "", at(11), "delta"),
    ]  # fmt: skip
    monkeypatch.setattr(resourcesync, "get_resources", lambda url: resources)
    downloaded = []

    def download(url, path, fixity=None, desc=None, quiet=False):
        downloaded.append(url)
        return _fake_download(url, path, fixity, desc, quiet)

    monkeypatch.setattr(resourcesync, "download", download)

    result = runner.invoke(app, ["sync", "brown"])
    assert result.exit_code == 0, result.output
    # nothing older than the latest full dump is fetched, and none is skipped
    assert downloaded == [resources[2].url, resources[3].url]
    assert "skipping" not in result.output

    con = lake.connect(read_only=False, config=get_config())
    ids = con.execute("SELECT pod_record_id FROM record_meta ORDER BY 1").fetchall()
    assert ids == [("brown:a1",), ("brown:a2",)]
    assert lake.get_cursor(con, "brown") == at(11)
    con.close()


def test_sync_skips_full_dump_once_org_is_loaded(tmp_path, monkeypatch):
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")
    assert runner.invoke(app, ["sync", "brown"]).exit_code == 0  # leaves a1

    # POD re-issues a full dump (a1 + a2), then a deletes file removing a1.
    # Applied, the full would bring a2 back; skipped, nothing is left.
    full_ts = datetime.datetime(2026, 2, 20, tzinfo=datetime.UTC)
    del_ts = datetime.datetime(2026, 2, 21, tzinfo=datetime.UTC)
    later = [
        Resource(f"{BASE}/4/brown-2026-02-20-full-marcxml.xml.gz",
                 "application/gzip", 6_400_000_000, "", full_ts, "full"),
        Resource(f"{BASE}/5/brown-2026-02-21-delta-deletes.del.txt",
                 "text/plain", 1, "", del_ts, "deletes"),
    ]  # fmt: skip
    monkeypatch.setattr(resourcesync, "get_resources", lambda url: _resources() + later)
    downloaded = []

    def download(url, path, fixity=None, desc=None, quiet=False):
        downloaded.append(url)
        path.write_text("a1\n")
        return path

    monkeypatch.setattr(resourcesync, "download", download)

    result = runner.invoke(app, ["sync", "brown"])
    assert result.exit_code == 0, result.output
    assert "brown: skipping full dump of 2026-02-20 (6.4 GB)" in result.output
    assert downloaded == [later[1].url]  # the skipped dump is never fetched

    con = lake.connect(read_only=False, config=get_config())
    assert con.execute("SELECT count(*) FROM record_meta").fetchone() == (0,)
    assert lake.get_cursor(con, "brown") == del_ts
    con.close()


def test_sync_verbose_logs_apply_steps(tmp_path, monkeypatch):
    """--verbose must actually surface the per-statement markers: they are what
    pins a stall or memory spike to a specific step (delete vs insert)."""
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["sync", "brown", "--verbose"])
    assert result.exit_code == 0, result.output
    assert "brown full: delete records" in result.output
    assert "brown full: insert records" in result.output
    assert "brown full: commit" in result.output


def test_sync_log_file_quiets_console(tmp_path, monkeypatch):
    """--log sends progress to a file and leaves stdout/stderr quiet, so a cron
    run only mails on failure."""
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")
    log = tmp_path / "sync.log"

    result = runner.invoke(app, ["sync", "brown", "--log", str(log)])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == ""

    written = log.read_text()
    assert "downloading" in written
    # download and conversion are timed separately: conversion is the slow part
    assert "downloaded in " in written
    assert "converted in " in written
    assert "resources processed" in written
    # The record count must survive the bar being disabled: tqdm.update() is a
    # no-op on a disabled bar, so reading progress.n logged "0 records" here
    assert "updating the lake with 2 records" in written
    # httpx's per-request chatter would bury our progress lines
    assert "HTTP Request" not in written


def test_delete_limit_never_below_known_floor():
    """Demanding a backlog the rewrite has already failed to reach just burns
    compaction cycles, so the floor wins over the big-resource limit."""
    from podlake.cli import BIG_RESOURCE_BYTES, _delete_limit_for

    small, big = 1, BIG_RESOURCE_BYTES
    assert _delete_limit_for(small, 100_000_000, 0) == 100_000_000
    assert _delete_limit_for(big, 100_000_000, 0) == 50_000_000
    # a proven floor above the strict limit raises it rather than chasing 50M
    assert _delete_limit_for(big, 100_000_000, 76_000_000) == 76_000_000


def test_log_silences_duckdb_progress_bar(tmp_path, monkeypatch):
    """--log must also turn off DuckDB's own progress bar; connect() enables it
    for any TTY, so the log file alone would not keep an interactive console
    quiet. CliRunner is not a TTY, so assert the setting directly."""
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    from podlake import cli

    cli._setup_logging(False, tmp_path / "x.log")
    try:
        con = cli._connect_writable(get_config())
        assert con.execute(
            "SELECT current_setting('enable_progress_bar')"
        ).fetchone() == (False,)
        con.close()
    finally:
        cli._setup_logging(False, None)


def test_compact_supports_log(tmp_path, monkeypatch):
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")
    log = tmp_path / "compact.log"

    result = runner.invoke(app, ["compact", "--log", str(log)])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == ""
    assert "pending deletes" in log.read_text()


def _con():
    """A throwaway connection; the lake calls under test are all monkeypatched."""
    return duckdb.connect()


def _fake_backlog(monkeypatch, pending, after):
    """Drive _maybe_apply_deletes with a scripted backlog. Returns the list of
    thresholds the rewrite was actually asked for."""
    from podlake import cli

    tried = []
    monkeypatch.setattr(cli.lake, "pending_deletes", lambda con: (1, pending))

    def fake_compact(con, *, days, rewrite_deletes):
        tried.append(rewrite_deletes)
        return {
            "deleted_rows_before": pending,
            "deleted_rows_after": after(rewrite_deletes),
            "rewritten": 1,
        }

    monkeypatch.setattr(cli.lake, "compact", fake_compact)
    return tried


def test_backlog_escalates_then_records_floor(monkeypatch):
    """When no threshold can get under the limit, halve down to the minimum and
    remember the floor — rather than give up early or retry forever."""
    from podlake import cli

    tried = _fake_backlog(monkeypatch, 200_000_000, lambda t: 200_000_000)
    state: dict = {}
    cli._maybe_apply_deletes(_con(), "x", "[1/1]", 1, 100_000_000, state)

    assert tried == [0.05, 0.025, 0.0125, 0.01]  # stops at the minimum
    assert state["floor"] == 200_000_000

    # a later resource at or below the floor must not pay for the ladder again
    tried.clear()
    cli._maybe_apply_deletes(_con(), "x", "[2/2]", 1, 100_000_000, state)
    assert tried == []


def test_backlog_stops_as_soon_as_it_is_under_the_limit(monkeypatch):
    """The cheap threshold suffices, so don't escalate or record a floor."""
    from podlake import cli

    tried = _fake_backlog(monkeypatch, 200_000_000, lambda t: 50_000_000)
    state: dict = {}
    cli._maybe_apply_deletes(_con(), "x", "[1/1]", 1, 100_000_000, state)

    assert tried == [0.05]
    assert "floor" not in state


def test_backlog_check_disabled_by_zero(monkeypatch):
    from podlake import cli

    tried = _fake_backlog(monkeypatch, 900_000_000, lambda t: 900_000_000)
    cli._maybe_apply_deletes(_con(), "x", "[1/1]", 1, 0, {})
    assert tried == []


def test_sync_all_reports_a_run_total(tmp_path, monkeypatch):
    """A cron log wants a one-line verdict for the run, not just per-org lines."""
    _patch(monkeypatch)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.setenv("PODLAKE_CATALOG", str(tmp_path / "podlake.ducklake"))
    monkeypatch.setenv("PODLAKE_DATA_PATH", str(tmp_path / "data") + "/")

    result = runner.invoke(app, ["sync-all"])
    assert result.exit_code == 0, result.output
    # brown fixture: full (a1,a2) then delta (a1,a2) = 4 changed, then 1 deleted
    assert "total: 3 resources from 1 organization" in result.output
    assert "4 records changed" in result.output
    assert "1 deleted" in result.output
