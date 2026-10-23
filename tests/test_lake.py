import os
from datetime import UTC, datetime
from pathlib import Path

import boto3
import duckdb
import moto
import pytest

from podlake import lake
from podlake.config import Config, get_config


def _dev_config(tmp_path: Path) -> Config:
    return Config(
        profile="file",
        data_path=str(tmp_path / "data") + "/",
        catalog_uri=str(tmp_path / "podlake.ducklake"),
    )


def _records_parquet(path: Path, rows: list[tuple]) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t (org VARCHAR, pod_record_id VARCHAR, field_tag VARCHAR, "
        "field_seq INTEGER, ind1 VARCHAR, ind2 VARCHAR, subfield_code VARCHAR, "
        "subfield_seq INTEGER, value VARCHAR)"
    )
    if rows:
        con.executemany("INSERT INTO t VALUES (?,?,?,?,?,?,?,?,?)", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
    con.close()


def _meta_parquet(path: Path, rows: list[tuple]) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE m (org VARCHAR, pod_record_id VARCHAR, goldrush_key VARCHAR)"
    )
    if rows:
        con.executemany("INSERT INTO m VALUES (?,?,?)", rows)
    con.execute(f"COPY m TO '{path}' (FORMAT parquet)")
    con.close()


def _record(org, rid, title, gr):
    """Return (eav_rows, meta_row) for one simple record: LDR + 001 + 245$a."""
    pid = f"{org}:{rid}"
    rows = [
        (org, pid, "LDR", 0, None, None, None, None, "00000nam a2200000 a 4500"),
        (org, pid, "001", 1, None, None, None, None, rid),
        (org, pid, "245", 2, "1", "0", "a", 0, title),
    ]
    return rows, (org, pid, gr)


def _write_org(tmp_path, name, records):
    """Write a records+meta parquet pair for a list of _record() results."""
    rec_rows = [r for rec, _ in records for r in rec]
    meta_rows = [meta for _, meta in records]
    rpq = tmp_path / f"{name}.records.parquet"
    mpq = tmp_path / f"{name}.meta.parquet"
    _records_parquet(rpq, rec_rows)
    _meta_parquet(mpq, meta_rows)
    return rpq, mpq


def test_default_data_path_lake_reads_from_another_directory(tmp_path, monkeypatch):
    """
    podlake-web reads the lake from its own checkout, so data file paths must not
    depend on the working directory podlake wrote them from.
    """
    writer, reader = tmp_path / "podlake", tmp_path / "podlake-web"
    writer.mkdir()
    reader.mkdir()
    monkeypatch.chdir(writer)
    monkeypatch.setenv("PODLAKE_PROFILE", "file")
    monkeypatch.delenv("PODLAKE_CATALOG", raising=False)
    monkeypatch.delenv("PODLAKE_DATA_PATH", raising=False)

    config = get_config()
    assert config.data_path == str(writer / "lake-data") + "/"

    con = lake.connect(config=config)
    lake.ensure_schema(con)
    # enough rows to be written to a Parquet file rather than inlined in the catalog
    con.execute(
        "INSERT INTO records SELECT 'mit', 'mit:' || i, '245', 1, '1', '0', 'a', 0, "
        "'title ' || i FROM range(50000) r(i)"
    )
    con.close()

    monkeypatch.chdir(reader)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute(
        f"ATTACH 'ducklake:{writer}/podlake.ducklake' AS l "
        f"(DATA_PATH '{writer}/lake-data/', OVERRIDE_DATA_PATH true, READ_ONLY)"
    )
    # count(*), min() and max() can come from catalog statistics; this has to
    # open the Parquet file
    total = con.execute("SELECT sum(length(value)) FROM l.records").fetchone()
    assert total == (sum(len(f"title {i}") for i in range(50000)),)


def test_load_pair_and_overlap(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))

    srpq, smpq = _write_org(
        tmp_path,
        "stanford",
        [
            _record("stanford", "a1", "Symphony", "shared"),
            _record("stanford", "a2", "Sonata", "stanford_only"),
        ],
    )
    hrpq, hmpq = _write_org(
        tmp_path, "harvard", [_record("harvard", "b1", "Symphony", "shared")]
    )

    assert lake.load_pair(con, "stanford", srpq, smpq) == 2
    assert lake.load_pair(con, "harvard", hrpq, hmpq) == 1

    # records partitioned by org
    cols = [r[0] for r in con.execute("DESCRIBE records").fetchall()]
    assert cols[0] == "org"

    counts = dict(
        con.execute("SELECT org, count(*) FROM record_meta GROUP BY org").fetchall()
    )
    assert counts == {"stanford": 2, "harvard": 1}

    # consortial overlap via record_meta
    overlap = con.execute(
        "SELECT goldrush_key, count(DISTINCT org) AS orgs FROM record_meta "
        "GROUP BY goldrush_key HAVING orgs > 1"
    ).fetchall()
    assert overlap == [("shared", 2)]
    con.close()


def test_apply_resource_upsert_and_delete(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    srpq, smpq = _write_org(
        tmp_path,
        "stanford",
        [
            _record("stanford", "a1", "Symphony", "k1"),
            _record("stanford", "a2", "Sonata", "k2"),
        ],
    )
    lake.load_pair(con, "stanford", srpq, smpq)

    # delta: a1 retitled, a3 new
    drpq, dmpq = _write_org(
        tmp_path,
        "stanford-delta",
        [
            _record("stanford", "a1", "Symphony (rev)", "k1"),
            _record("stanford", "a3", "Concerto", "k3"),
        ],
    )
    ts1 = datetime(2026, 2, 12, tzinfo=UTC)
    changed, _ = lake.apply_resource(con, "stanford", "delta", (drpq, dmpq), ts1)
    assert changed == 2

    ts2 = datetime(2026, 2, 13, tzinfo=UTC)
    _, deleted = lake.apply_resource(con, "stanford", "deletes", ["stanford:a2"], ts2)
    assert deleted == 1

    ids = [
        r[0]
        for r in con.execute(
            "SELECT pod_record_id FROM record_meta ORDER BY pod_record_id"
        ).fetchall()
    ]
    assert ids == ["stanford:a1", "stanford:a3"]

    title = con.execute(
        "SELECT value FROM records WHERE pod_record_id='stanford:a1' "
        "AND field_tag='245' AND subfield_code='a'"
    ).fetchone()
    assert title == ("Symphony (rev)",)

    assert lake.get_cursor(con, "stanford") == ts2
    con.close()


def test_apply_resource_empty_delta(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    srpq, smpq = _write_org(
        tmp_path, "stanford", [_record("stanford", "a1", "Symphony", "k1")]
    )
    lake.load_pair(con, "stanford", srpq, smpq)

    # an empty delta (0 records) is a no-op that still advances the cursor
    erpq, empq = _write_org(tmp_path, "empty", [])
    ts = datetime(2026, 3, 1, tzinfo=UTC)
    changed, deleted = lake.apply_resource(con, "stanford", "delta", (erpq, empq), ts)
    assert (changed, deleted) == (0, 0)
    assert con.execute("SELECT count(*) FROM record_meta").fetchone() == (1,)
    assert lake.get_cursor(con, "stanford") == ts
    con.close()


def test_eav_query_shapes(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    rows = [
        ("stanford", "stanford:a1", "LDR", 0, None, None, None, None, "00000nam"),
        ("stanford", "stanford:a1", "001", 1, None, None, None, None, "a1"),
        ("stanford", "stanford:a1", "245", 2, "1", "0", "a", 0, "Symphony"),
        ("stanford", "stanford:a1", "245", 2, "1", "0", "b", 1, "in D"),
        ("stanford", "stanford:a1", "100", 3, "1", " ", "a", 0, "Beethoven"),
        ("stanford", "stanford:a1", "650", 4, " ", "0", "a", 0, "Music"),
        ("stanford", "stanford:a1", "650", 5, " ", "0", "a", 0, "History"),
        ("stanford", "stanford:a2", "LDR", 0, None, None, None, None, "00000nam"),
        ("stanford", "stanford:a2", "001", 1, None, None, None, None, "a2"),
        ("stanford", "stanford:a2", "245", 2, "1", "0", "a", 0, "Sonata"),
        ("stanford", "stanford:a2", "100", 3, "1", " ", "a", 0, "Mozart"),
    ]
    rpq = tmp_path / "s.records.parquet"
    mpq = tmp_path / "s.meta.parquet"
    _records_parquet(rpq, rows)
    _meta_parquet(
        mpq, [("stanford", "stanford:a1", "k1"), ("stanford", "stanford:a2", "k2")]
    )
    lake.load_pair(con, "stanford", rpq, mpq)

    # whole 245 field via string_agg (subfields joined in subfield order)
    whole = con.execute(
        "SELECT string_agg(value, ' ' ORDER BY subfield_seq) FROM records "
        "WHERE pod_record_id='stanford:a1' AND field_tag='245'"
    ).fetchone()
    assert whole == ("Symphony in D",)

    # self-join: title <-> author
    sj = con.execute(
        "SELECT t.value, a.value FROM records t JOIN records a USING (pod_record_id) "
        "WHERE t.field_tag='245' AND t.subfield_code='a' "
        "AND a.field_tag='100' AND a.subfield_code='a' "
        "AND t.pod_record_id='stanford:a1'"
    ).fetchone()
    assert sj == ("Symphony", "Beethoven")

    # FILTER-agg pivot: title per record
    pivot = dict(
        con.execute(
            "SELECT pod_record_id, "
            "max(value) FILTER (WHERE field_tag='245' AND subfield_code='a') "
            "FROM records GROUP BY pod_record_id"
        ).fetchall()
    )
    assert pivot == {"stanford:a1": "Symphony", "stanford:a2": "Sonata"}

    # repeated 650 preserved as two rows with distinct field_seq
    counts = con.execute(
        "SELECT count(*), count(DISTINCT field_seq) FROM records "
        "WHERE pod_record_id='stanford:a1' AND field_tag='650'"
    ).fetchone()
    assert counts == (2, 2)
    con.close()


def test_get_cursor_absent(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    assert lake.get_cursor(con, "stanford") is None
    con.close()


def test_connect_caps_memory_and_sets_spill_dir(tmp_path):
    config = _dev_config(tmp_path)
    config.memory_limit = "512MiB"
    con = lake.connect(read_only=False, config=config)
    try:
        # spilling is enabled (DuckDB has a temp dir, derived from $TMPDIR)
        spill = con.execute("SELECT current_setting('temp_directory')").fetchone()
        assert spill and spill[0]
        # and the memory cap is applied (DuckDB reports it as "512.0 MiB")
        limit = con.execute("SELECT current_setting('memory_limit')").fetchone()
        assert limit and "512" in limit[0]
    finally:
        con.close()


def test_read_only_cannot_write(tmp_path):
    config = _dev_config(tmp_path)
    rpq, mpq = _write_org(tmp_path, "stanford", [_record("stanford", "a1", "T", "k")])

    writer = lake.connect(read_only=False, config=config)
    lake.load_pair(writer, "stanford", rpq, mpq)
    writer.close()

    reader = lake.connect(read_only=True, config=config)
    assert reader.execute("SELECT count(*) FROM record_meta").fetchone() == (1,)
    with pytest.raises(duckdb.Error):
        reader.execute("INSERT INTO record_meta VALUES ('x','x:1','k')")
    reader.close()


def test_publish_incremental(tmp_path):
    config = _dev_config(tmp_path)
    rpq, mpq = _write_org(tmp_path, "stanford", [_record("stanford", "a1", "T", "k")])
    con = lake.connect(read_only=False, config=config)
    lake.load_pair(con, "stanford", rpq, mpq)
    con.close()

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    with moto.mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket="pod-public")

        # first publish uploads the catalog (+ any non-inlined data files; a
        # tiny lake gets inlined into the catalog, so there may be none)
        catalog_key, _, uploaded, _ = lake.publish(config, "s3://pod-public/lake")
        keys = [
            o["Key"]
            for o in s3.list_objects_v2(Bucket="pod-public").get("Contents", [])
        ]
        assert f"lake/{catalog_key}" in keys

        # re-publish is idempotent: no new uploads, prior data skipped, catalog
        # re-uploaded (file-level skip mechanics are covered in test_storage)
        catalog_key2, _, uploaded2, skipped2 = lake.publish(
            config, "s3://pod-public/lake"
        )
        assert uploaded2 == 0
        assert skipped2 == uploaded
        assert f"lake/{catalog_key2}" in [
            o["Key"]
            for o in s3.list_objects_v2(Bucket="pod-public").get("Contents", [])
        ]


def test_publish_rejects_postgres_catalog(tmp_path):
    pg_config = Config(
        profile="postgres",
        data_path="s3://x/lake/",
        catalog_uri="postgres:dbname=podlake host=db",
    )
    with pytest.raises(ValueError):
        lake.publish(pg_config, "s3://pod-public/lake")


def test_compact_reclaims_superseded_data(tmp_path):
    config = _dev_config(tmp_path)
    con = lake.connect(read_only=False, config=config)
    try:

        def build(name: str, title: str) -> tuple[Path, Path]:
            # enough rows to write real Parquet files (not inlined into the catalog)
            src = duckdb.connect()
            src.execute(
                "CREATE TABLE t (org VARCHAR, pod_record_id VARCHAR, field_tag VARCHAR, "
                "field_seq INTEGER, ind1 VARCHAR, ind2 VARCHAR, subfield_code VARCHAR, "
                "subfield_seq INTEGER, value VARCHAR)"
            )
            src.execute(
                "INSERT INTO t SELECT 'stanford', 'stanford:' || i, '245', 1, '1', '0', "
                f"'a', 0, '{title} ' || i FROM range(20000) tbl(i)"
            )
            src.execute(
                "CREATE TABLE m (org VARCHAR, pod_record_id VARCHAR, goldrush_key VARCHAR)"
            )
            src.execute(
                "INSERT INTO m SELECT 'stanford', 'stanford:' || i, 'k' || i "
                "FROM range(20000) tbl(i)"
            )
            rpq = tmp_path / f"{name}.records.parquet"
            mpq = tmp_path / f"{name}.meta.parquet"
            src.execute(f"COPY t TO '{rpq}' (FORMAT parquet)")
            src.execute(f"COPY m TO '{mpq}' (FORMAT parquet)")
            src.close()
            return rpq, mpq

        r, m = build("full", "One")
        lake.load_pair(con, "stanford", r, m)
        # a full re-import supersedes every record -> the first copy is now dead
        r, m = build("reimport", "Two")
        lake.apply_resource(
            con, "stanford", "delta", (r, m), datetime(2026, 3, 1, tzinfo=UTC)
        )

        data_dir = tmp_path / "data"

        def data_size() -> int:
            return sum(f.stat().st_size for f in data_dir.rglob("*") if f.is_file())

        before = data_size()
        stats = lake.compact(con, days=0)

        assert stats["snapshots_after"] < stats["snapshots_before"]
        assert stats["cleaned"] >= 1  # the superseded copy's files were deleted
        assert data_size() < before  # disk actually reclaimed
        # the live data is the superseding version, still complete
        assert con.execute("SELECT count(*) FROM record_meta").fetchone() == (20000,)
        assert con.execute(
            "SELECT value FROM records WHERE pod_record_id = 'stanford:0'"
        ).fetchone() == ("Two 0",)
    finally:
        con.close()


def test_compact_dry_run_changes_nothing(tmp_path):
    config = _dev_config(tmp_path)
    con = lake.connect(read_only=False, config=config)
    try:
        rpq, mpq = _write_org(
            tmp_path, "stanford", [_record("stanford", "a1", "T", "k1")]
        )
        lake.load_pair(con, "stanford", rpq, mpq)
        stats = lake.compact(con, days=0, dry_run=True)
        assert stats["snapshots_after"] == stats["snapshots_before"]
        assert stats["merged"] == 0
        assert stats["passes"] == 1  # dry run never loops
    finally:
        con.close()


def test_rewrite_deletes_clears_tombstoned_rows(tmp_path):
    con = lake.connect(read_only=False, config=_dev_config(tmp_path))
    try:
        lake.ensure_schema(con)
        # enough rows to write a real data file rather than inlining into the catalog
        con.execute(
            "INSERT INTO records SELECT 'x', 'x:' || i, '245', 1, '1', '0', 'a', 0, "
            "'t' || i FROM range(50000) s(i)"
        )
        # delete ~20% of the rows: a plain compact leaves these tombstoned, because
        # merge_adjacent_files skips files that are already at the target size
        con.execute(
            "DELETE FROM records WHERE org = 'x' "
            "AND (CAST(substr(pod_record_id, 3) AS INTEGER) % 5) = 0"
        )

        files, rows = lake.pending_deletes(con)
        assert files >= 1 and rows > 0

        lake.compact(con, days=0)
        _, still_pending = lake.pending_deletes(con)
        assert still_pending == rows  # plain compact does not apply them

        stats = lake.compact(con, days=0, rewrite_deletes=0.05)
        assert stats["rewritten"] >= 1
        assert stats["deleted_rows_before"] == rows
        assert stats["deleted_rows_after"] == 0  # tombstones physically applied
        assert lake.pending_deletes(con) == (0, 0)

        # and the surviving data is intact and correct
        assert con.execute(
            "SELECT count(*) FROM records WHERE org = 'x'"
        ).fetchone() == (40000,)
    finally:
        con.close()
