# podlake

[![Tests](https://github.com/sul-dlss/podlake/actions/workflows/test.yml/badge.svg)](https://github.com/sul-dlss/podlake/actions/workflows/test.yml)

podlake harvests MARC from [POD]'s [ResourceSync] service, converts it to
Parquet, and loads it into a [DuckLake] lakehouse so it can be queried with
DuckDB. The lake is only ever as current as POD's most recently published delta.

## The DuckLake schema

[DuckLake] is a DuckDB-centric table format: a catalog plus ordinary Parquet
data files. DuckLake handles record updates and deletes for you so you will
always get the most recent record. You query it with DuckDB (the `ducklake`
extension), instead of reading the Parquet files directly.

Once you connect to the DuckLake you will see two tables, both partitioned by
`org` (the contributing institution).

**`record_meta`: one row per record**

| column | type | notes |
| --- | --- | --- |
| `org` | VARCHAR | contributing institution (partition key) |
| `pod_record_id` | VARCHAR | stable record id, `org:localid` |
| `goldrush_key` | VARCHAR | Gold Rush match key; groups records into distinct titles |

**`records`: one row per subfield (tall / EAV)**

| column | type | notes |
| --- | --- | --- |
| `org` | VARCHAR | partition key |
| `pod_record_id` | VARCHAR | joins to `record_meta` |
| `field_tag` | VARCHAR | MARC tag; the leader is `'LDR'` |
| `field_seq` | INTEGER | field order within the record |
| `ind1`, `ind2` | VARCHAR | indicators (NULL for control fields) |
| `subfield_code` | VARCHAR | subfield code; NULL for control fields / leader |
| `subfield_seq` | INTEGER | subfield order within the field |
| `value` | VARCHAR | the subfield text (or the control-field / leader string) |

**harvest_state: one row per lake update**

| column | type | notes |
| --- | --- | --- |
| `org` | VARCHAR | organization name |
| `last_modified` | TIMESTRAMP | the last time the organization data was refreshed |

Notes for querying:

- The leader is `field_tag = 'LDR'`; control fields (00X) hold their data in
  `value` with a NULL `subfield_code`.
- Field and subfield order is preserved by `field_seq` / `subfield_seq`, so a
  record round-trips exactly.
- Handy 008 slices (1-indexed): publication year = `substr(value, 8, 4)`, place
  of publication = chars 16–18, language = 36–38; leader type-of-record = char 7.

This tall, one-row-per-subfield layout is deliberate, and follows the
MARC-in-Parquet format research in [`dchud/mrrc`][mrrc]: it loads into DuckLake
orders of magnitude faster and cheaper than a wide one-column-per-field table,
every field/subfield/indicator is uniformly queryable with a plain `WHERE`, and
it's lossless. As that evaluation observes, columnar storage pays off for very
large analytic collections, which is exactly podlake's case.

### Gold Rush keys

Every `record_meta` row carries a `goldrush_key`: a normalized key produced by
the Colorado Alliance's [Gold Rush match key][goldrush] algorithm. Records that
produce the same key describe the same title, so the key is how you work at the
*title* level across the consortium. `GROUP BY goldrush_key` collapses many
institutions' records for one title into a single group, and `count(DISTINCT
org)` per key is how many institutions hold it (the basis for overlap, rarity
/ "last copies", and deduplication). It is roughly *manifestation* (FRBR) or
*instance* (BIBFRAME) level: because it captures edition and carrier, a print
book and its e-book edition get distinct keys and won't group together, unless you 
work with a portion of the key with a [text function].

### Example queries

```sql
-- records vs. distinct titles per organization
SELECT org, count(*) AS records, count(DISTINCT goldrush_key) AS titles
FROM record_meta GROUP BY org;

-- consortial overlap: titles held by more than one institution
SELECT goldrush_key, count(DISTINCT org) AS orgs
FROM record_meta GROUP BY goldrush_key HAVING orgs > 1;

-- all titles (245 $a)
SELECT value FROM records WHERE field_tag = '245' AND subfield_code = 'a';

-- pull several fields per record as columns (conditional aggregation)
SELECT pod_record_id,
  max(value) FILTER (WHERE field_tag = '245' AND subfield_code = 'a') AS title,
  max(value) FILTER (WHERE field_tag = '100' AND subfield_code = 'a') AS author
FROM records WHERE field_tag IN ('245', '100') GROUP BY pod_record_id;

-- reconstruct a record in order (leader first, then fields/subfields)
SELECT field_tag, ind1, ind2, subfield_code, value
FROM records WHERE pod_record_id = 'stanford:a1'
ORDER BY field_seq, subfield_seq;
```

You can see more elaborate queries in the [podlake-web] project.

### Joining fields and subfields

Because each subfield is its own row, you relate them with a **self-join** on
`records`. Join on `pod_record_id` to combine different fields of a record; join
on `(pod_record_id, field_seq)` to combine subfields of the *same* field
occurrence: something `FILTER`-aggregation can't distinguish when a field
repeats:

```sql
-- title ($a) paired with the remainder-of-title ($b) from the same 245
SELECT a.pod_record_id, a.value AS title, b.value AS remainder
FROM records a JOIN records b USING (pod_record_id, field_seq)
WHERE a.field_tag = '245' AND a.subfield_code = 'a' AND b.subfield_code = 'b';
```

The `FILTER (WHERE …)` form shown above is simpler when you just want one value
per field per record; reach for a self-join when a field can repeat (multiple
650s, 856s, …) or when you need to correlate subfields within a single field.

## Building the lake

Install [uv], then run podlake with `uvx`:

```
$ uvx podlake --help
```

Configure it with environment variables (read from a `.env` file or the
environment): put your POD token in `PODBUCKET_POD_TOKEN` and pick a profile with
`PODLAKE_PROFILE`. The default **`file`** profile uses a local catalog file and
local Parquet — ideal for building a lake locally and then publishing it:

```sh
PODBUCKET_POD_TOKEN=your-pod-token
PODLAKE_PROFILE=file
PODLAKE_CATALOG=podlake.ducklake         # local catalog file (default)
PODLAKE_DATA_PATH=./lake-data/           # where Parquet data files live (default)
PODLAKE_PUBLISH_URL=s3://your-bucket/pod # optional default target for `publish`
```

In production you run that same `file` profile on a server, `sync`, and
`publish` the file-catalog lake to S3. POD members then attach to the
bucket read-only, with no database to run or expose.

**Sync** downloads POD's ResourceSync dumps (a base full dump plus a chain of
daily delta and delete files), converts them to Parquet, and upserts them into
the lake:

```
$ uvx podlake streams            # list organizations + their resource counts/sizes
$ uvx podlake sync stanford      # one organization
$ uvx podlake sync-all           # every organization, one at a time
```

The first run does the full initial load, and later runs apply only new deltas. 
You use the same command for both. Each resource is applied in its own transaction (one
DuckLake snapshot) and advances the org's cursor, so an interrupted sync resumes
cleanly. Run `sync-all` on a schedule (e.g. cron) to keep the lake current. 
Lower `--batch-size` (default 100000) on memory-constrained machines.

The spill for large operations, and each resource's download/conversion, goes to
`$TMPDIR` — point that at a roomy volume if your default temp dir is small. Set
`PODLAKE_MEMORY_LIMIT` (e.g. `10GB` on a 16GB box) to cap DuckDB's buffer pool so
it spills to disk instead of growing. Note this bounds the *buffer pool* only;
the delete backlog described next is separate and is not covered by it.

## Query the lake

For a quick check, query through podlake (it connects read-only):

```
$ uvx podlake query "SELECT org, count(*) FROM record_meta GROUP BY org"
```

Analysts usually attach directly with DuckDB, **read-only** so the connection
can never modify the lake:

```sql
-- a published lake in a bucket (what most consumers use)
INSTALL ducklake; INSTALL httpfs;
ATTACH 'ducklake:s3://your-bucket/pod/podlake.ducklake' AS podlake
  (DATA_PATH 's3://your-bucket/pod/lake-data/', READ_ONLY, OVERRIDE_DATA_PATH true);
USE podlake;

-- a local file-catalog lake
INSTALL ducklake;
ATTACH 'ducklake:podlake.ducklake' AS podlake (DATA_PATH './lake-data/', READ_ONLY);
USE podlake;
```

`OVERRIDE_DATA_PATH true` re-roots the published catalog at the bucket. A public
bucket needs no credentials; for a private one, consumers supply read-only AWS
credentials via `CREATE SECRET (TYPE s3, ...)`. Thanks to snapshot isolation the
maintainer can republish while analysts keep querying, and a reader can pin a
version with `FROM records AT (VERSION => N)`. See the schema section above for
query patterns.

## Running the whole pipeline on a schedule

[`bin/pipeline.sh`](bin/pipeline.sh) is the maintenance cycle above plus the
dashboard, as one cron job: `sync-all`, `compact`, then [podlake-web]'s
`refresh`, which recompiles the public aggregate JSON from the lake and pushes
it.

Everything the script needs is derived from its own location; `PODLAKE_DIR`,
`POD_ROOT`, `WEB_DIR`, `LOG_DIR`, `LOCK_FILE`, `CATALOG`, `PUBLISH_BRANCH` and
`DEPLOY_KEY` override the pieces. Two things it can't do for you:

- **podlake's `.env`**, with `PODBUCKET_POD_TOKEN`, in the podlake checkout.
- **A GitHub credential for podlake-web**, since publishing is a push.

To create a Github credential to push the new podlake-web artifacts:

```sh
# as the account that runs cron
$ ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519_podlake_web
$ cat ~/.ssh/id_ed25519_podlake_web.pub
```

Add that public half under podlake-web's **Settings → Deploy keys → Add**, with
**Allow write access** checked — without it the fetch works and only the push
fails. Then, once, by hand:

```sh
$ ssh -T git@github.com     # records github.com in known_hosts; prints which repo the key is for
$ git -C ../podlake-web remote -v    # origin must be the git@github.com: form, not https
```

That first `ssh -T` is not optional housekeeping: an unknown host key fails the
fetch outright, and under cron there's no one to answer the prompt. Point the
script at the key with `DEPLOY_KEY`, which pins the identity explicitly:

```sh
DEPLOY_KEY=$HOME/.ssh/id_ed25519_podlake_web /opt/app/pod/podlake/bin/pipeline.sh
```

## Develop

```
$ uv run pytest
```

Tests run entirely locally (no network or `PODBUCKET_POD_TOKEN` needed):
ResourceSync manifest parsing with fixtures, MARCXML conversion with small
in-test dumps, and the lake/publish paths against a temporary file-profile lake
(S3 is mocked with moto).

[POD]: https://pod.stanford.edu/
[ResourceSync]: https://www.openarchives.org/rs/toc
[uv]: https://docs.astral.sh/uv/
[goldrush]: https://gitlab.com/pymarc/goldrush
[DuckLake]: https://ducklake.select/
[mrrc]: https://github.com/dchud/mrrc/blob/main/docs/history/format-research/EVALUATION_PARQUET.md
[podlake-web]: https://github.com/pod4lib/podlake-web
[podlake-web-site]: https://pod4lib.github.io/podlake-web/
[text function]: https://duckdb.org/docs/lts/sql/functions/text
