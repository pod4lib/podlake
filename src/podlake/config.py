import os
from dataclasses import dataclass, field
from pathlib import Path

import dotenv

# The alias the DuckLake catalog is attached as; used throughout the codebase.
LAKE_ALIAS = "podlake"


@dataclass
class Config:
    """
    Resolved podlake configuration for the active profile.

    Two profiles are supported, selected with the PODLAKE_PROFILE environment
    variable and named for the catalog backend each uses:

    - "file" (default): a local DuckDB catalog file and a local Parquet
      DATA_PATH. No external services required. This is also the profile you
      run in production and then `publish` to S3 for read-only consumers.
    - "postgres": a Postgres catalog and an S3 DATA_PATH, for a shared lake with
      concurrent writers. AWS credentials are resolved by DuckDB's
      credential_chain (standard AWS_* env vars, shared config, or an assumed
      role).

    PODLAKE_MEMORY_LIMIT, if set (e.g. "10GB"), caps DuckDB's memory; past the
    limit it spills to disk (in $TMPDIR, same as downloads) instead of risking
    the OOM killer.
    """

    profile: str
    data_path: str
    catalog_uri: str
    pg: dict[str, str] = field(default_factory=dict)
    publish_url: str | None = None
    memory_limit: str | None = None

    @property
    def is_postgres(self) -> bool:
        return self.profile == "postgres"

    @property
    def is_file_catalog(self) -> bool:
        """
        True when the catalog is a local DuckDB/SQLite file (as opposed to a
        Postgres catalog). Only a file-catalog lake can be published to S3.
        """
        return not self.catalog_uri.startswith("postgres:")

    def attach_sql(self, read_only: bool = False) -> str:
        """
        Build the ATTACH statement for this profile's DuckLake catalog. The
        catalog URI (which may embed a Postgres password) and data path can't be
        passed as bind parameters, so they are inlined as single-quoted SQL
        literals with embedded quotes escaped.
        """
        options = [f"DATA_PATH '{_sql_literal(self.data_path)}'"]
        if read_only:
            options.append("READ_ONLY")
        target = _sql_literal(f"ducklake:{self.catalog_uri}")
        return f"ATTACH '{target}' AS {LAKE_ALIAS} ({', '.join(options)})"

    def describe(self) -> dict[str, str]:
        """
        A human-readable, secret-masked view of the resolved configuration,
        suitable for printing in the `config` command.
        """
        info = {
            "PODLAKE_PROFILE": self.profile,
            "data_path": self.data_path,
            "memory_limit": self.memory_limit or "(DuckDB default)",
        }
        if self.is_postgres:
            info["catalog"] = "postgres"
            info["postgres_host"] = self.pg.get("host", "")
            info["postgres_dbname"] = self.pg.get("dbname", "")
            info["postgres_user"] = self.pg.get("user", "")
            info["postgres_password"] = _mask(self.pg.get("password", ""))
        else:
            info["catalog"] = self.catalog_uri
        return info


def get_config() -> Config:
    """
    Load environment variables from a .env file (if present) and resolve the
    configuration for the active profile.
    """
    dotenv.load_dotenv()

    profile = os.environ.get("PODLAKE_PROFILE", "file").lower()
    if profile not in ("file", "postgres"):
        raise ValueError(
            f"PODLAKE_PROFILE must be 'file' or 'postgres', not {profile!r}"
        )

    if profile == "postgres":
        return _postgres_config()
    return _file_config()


def _file_config() -> Config:
    catalog = os.environ.get("PODLAKE_CATALOG", "podlake.ducklake")
    data_path = os.environ.get("PODLAKE_DATA_PATH", "./lake-data/")
    # DuckLake under DuckDB 2.0 records each data file under a relative DATA_PATH
    # as a plain path relative to the writer's working directory (for example
    # "lake-data/main/records/org=mit/…", not relative to the data path). Readers
    # resolve that against their own working directory, where OVERRIDE_DATA_PATH
    # doesn't reach: podlake-web's refresh, run from its own checkout, failed with
    # "Cannot open file". An absolute DATA_PATH gets file paths relative to it.
    if not data_path.startswith("s3://"):
        data_path = str(Path(data_path).resolve()) + "/"
    return Config(
        profile="file",
        data_path=data_path,
        catalog_uri=catalog,
        publish_url=os.environ.get("PODLAKE_PUBLISH_URL"),
        memory_limit=os.environ.get("PODLAKE_MEMORY_LIMIT"),
    )


def _postgres_config() -> Config:
    data_path = _require("PODLAKE_DATA_PATH")

    dsn = os.environ.get("PODLAKE_PG_DSN")
    if dsn:
        pg = _parse_dsn(dsn)
    else:
        pg = {
            "host": _require("PODLAKE_PG_HOST"),
            "port": os.environ.get("PODLAKE_PG_PORT", "5432"),
            "dbname": _require("PODLAKE_PG_DBNAME"),
            "user": _require("PODLAKE_PG_USER"),
            "password": _require("PODLAKE_PG_PASSWORD"),
        }

    # DuckLake accepts a Postgres connection string after the "postgres:" prefix.
    catalog_uri = "postgres:" + " ".join(f"{k}={v}" for k, v in pg.items())

    return Config(
        profile="postgres",
        data_path=data_path,
        catalog_uri=catalog_uri,
        pg=pg,
        publish_url=os.environ.get("PODLAKE_PUBLISH_URL"),
        memory_limit=os.environ.get("PODLAKE_MEMORY_LIMIT"),
    )


def _parse_dsn(dsn: str) -> dict[str, str]:
    """
    Parse a libpq-style "key=value key=value" DSN into a dict.
    """
    pg = {}
    for part in dsn.split():
        if "=" in part:
            key, value = part.split("=", 1)
            pg[key] = value
    return pg


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} environment variable must be set for the postgres profile"
        )
    return value


def _mask(secret: str) -> str:
    if not secret:
        return ""
    return "*" * len(secret)


def _sql_literal(value: str) -> str:
    """Escape a value for use inside a single-quoted SQL string literal."""
    return value.replace("'", "''")
