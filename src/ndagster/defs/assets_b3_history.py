import contextlib
import tempfile
from pathlib import Path

import dagster as dg
import duckdb
import httpx2
import polars as pl

COTAHIST_QUERY = r"""
SELECT
    regexp_extract(line, '^(\d{2})', 1) as tipreg,
    regexp_replace(line, '^.{2}(\d{4})(\d{2})(\d{2}).*', '\1-\2-\3')::date as dtpreg,
    regexp_extract(line, '^.{10}(.{2})', 1) as codbdi,
    trim(regexp_extract(line, '^.{12}(.{12})', 1)) as codneg,
    regexp_extract(line, '^.{24}(.{3})', 1) as tpmerc,
    coalesce(nullif(trim(regexp_extract(line, '^.{27}(.{12})', 1)), ''), codneg) as nomres,
    trim(regexp_extract(line, '^.{39}(.{10})', 1)) as especi,
    trim(regexp_extract(line, '^.{49}(.{3})', 1)) as prazot,
    trim(regexp_extract(line, '^.{52}(.{4})', 1)) as modref,
    regexp_replace(line, '^.{56}(\d{11})(\d{2}).*', '\1.\2')::double as preabe,
    regexp_replace(line, '^.{69}(\d{11})(\d{2}).*', '\1.\2')::double as premax,
    regexp_replace(line, '^.{82}(\d{11})(\d{2}).*', '\1.\2')::double as premin,
    regexp_replace(line, '^.{95}(\d{11})(\d{2}).*', '\1.\2')::double as premed,
    regexp_replace(line, '^.{108}(\d{11})(\d{2}).*', '\1.\2')::double as preult,
    regexp_replace(line, '^.{121}(\d{11})(\d{2}).*', '\1.\2')::double as preofc,
    regexp_replace(line, '^.{134}(\d{11})(\d{2}).*', '\1.\2')::double as preofv,
    regexp_extract(line, '^.{147}(\d{5})', 1)::bigint as totneg,
    regexp_extract(line, '^.{152}(\d{18})', 1)::bigint as quatot,
    regexp_replace(line, '^.{170}(\d{16})(\d{2}).*', '\1.\2')::double as voltot,
    regexp_replace(line, '^.{188}(\d{11})(\d{2}).*', '\1.\2')::double as preexe,
    regexp_extract(line, '^.{201}(.)', 1) as indopc,
    regexp_replace(line, '^.{202}(\d{4})(\d{2})(\d{2}).*', '\1-\2-\3')::date as datven,
    regexp_extract(line, '^.{210}(\d{7})', 1) as fatcot,
    regexp_replace(line, '^.{217}(\d{11})(\d{2}).*', '\1.\2')::double as ptoexe,
    regexp_extract(line, '^.{230}(.{12})', 1) as codisi,
    regexp_extract(line, '^.{242}(\d{3})', 1) as dismes
FROM read_csv(
    $source,
    columns = {'line': 'varchar'},
    header = false,
    delim = '\t',
    quote = '',
    escape = '',
    encoding = 'latin-1',
    auto_detect = false
)
WHERE 
    regexp_matches(line, '^01')
ORDER BY 
    dtpreg,
    nomres,
    codneg,
    tipreg,
    codisi
"""


@contextlib.contextmanager
def database():
    with duckdb.connect() as conn:
        conn.install_extension("zipfs")
        conn.install_extension("httpfs")
        conn.load_extension("zipfs")
        conn.load_extension("httpfs")
        yield conn


def to_markdown(df: pl.DataFrame, **config) -> str:
    with pl.Config(
        tbl_formatting="MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
        tbl_cols=-1,
        tbl_width_chars=-1,
        fmt_str_lengths=100,
        **config,
    ):
        return str(df)


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    top_volume = (
        df.group_by("codneg", "nomres")
        .agg(pl.col("voltot").sum())
        .top_k(10, by="voltot")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "avg_price": dg.MetadataValue.float(float(df["premed"].mean() or 0)),
        "total_trades": dg.MetadataValue.int(int(df["totneg"].sum())),
        "total_volume": dg.MetadataValue.float(float(df["voltot"].sum())),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "top_volume": dg.MetadataValue.md(
            to_markdown(top_volume, thousands_separator=True, float_precision=2)
        ),
    }


@dg.asset(
    key=["b3_history", "daily", "download"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 * * 1-5",
        fmt=r"%Y-%m-%d",
    ),
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=600,
        max_retries=6,
    ),
)
def daily_download(context: dg.AssetExecutionContext) -> bytes:
    date = context.partition_time_window.start.strftime(r"%d%m%Y")
    url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{date}.zip"

    with httpx2.Client(follow_redirects=True, timeout=60) as client:
        response = client.get(url)
        response.raise_for_status()

    context.add_output_metadata(
        {
            "url": dg.MetadataValue.url(url),
            "size_bytes": dg.MetadataValue.int(len(response.content)),
            "response_content_type": response.headers.get("content-type", ""),
        }
    )
    return response.content


@dg.asset(
    key=["b3_history", "daily", "parse"],
    ins={"daily_download": dg.AssetIn(key=["b3_history", "daily", "download"])},
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 * * 1-5",
        fmt=r"%Y-%m-%d",
    ),
    io_manager_key="io_manager_parquet",
    automation_condition=(
        dg.AutomationCondition.any_deps_updated().since_last_handled()
        & ~dg.AutomationCondition.any_deps_in_progress()
        & ~dg.AutomationCondition.in_progress()
    ),
)
def daily_parse(
    context: dg.AssetExecutionContext, daily_download: bytes
) -> pl.DataFrame:
    date = context.partition_time_window.start.strftime(r"%d%m%Y")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"COTAHIST_D{date}.zip"
        path.write_bytes(daily_download)

        with database() as conn:
            source = f"zip://{path}/COTAHIST_D{date}.TXT"
            df = conn.sql(COTAHIST_QUERY, params={"source": source}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df


@dg.asset(
    key=["b3_history", "monthly", "download"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 * *",
        fmt=r"%Y-%m",
    ),
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=3600,
        max_retries=6,
    ),
)
def monthly_download(context: dg.AssetExecutionContext) -> bytes:
    date = context.partition_time_window.start.strftime(r"%m%Y")
    url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M{date}.zip"

    with httpx2.Client(follow_redirects=True, timeout=60) as client:
        response = client.get(url)
        response.raise_for_status()

    context.add_output_metadata(
        {
            "url": dg.MetadataValue.url(url),
            "size_bytes": dg.MetadataValue.int(len(response.content)),
            "response_content_type": response.headers.get("content-type", ""),
        }
    )
    return response.content


@dg.asset(
    key=["b3_history", "monthly", "parse"],
    ins={"monthly_download": dg.AssetIn(key=["b3_history", "monthly", "download"])},
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 * *",
        fmt=r"%Y-%m",
    ),
    io_manager_key="io_manager_parquet",
    automation_condition=(
        dg.AutomationCondition.any_deps_updated().since_last_handled()
        & ~dg.AutomationCondition.any_deps_in_progress()
        & ~dg.AutomationCondition.in_progress()
    ),
)
def monthly_parse(
    context: dg.AssetExecutionContext, monthly_download: bytes
) -> pl.DataFrame:
    date = context.partition_time_window.start.strftime(r"%m%Y")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"COTAHIST_M{date}.zip"
        path.write_bytes(monthly_download)

        with database() as conn:
            source = f"zip://{path}/COTAHIST_M{date}.TXT"
            df = conn.sql(COTAHIST_QUERY, params={"source": source}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df


@dg.asset(
    key=["b3_history", "yearly", "download"],
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 1 *",
        fmt=r"%Y",
    ),
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=3600,
        max_retries=6,
    ),
)
def yearly_download(context: dg.AssetExecutionContext) -> bytes:
    date = context.partition_time_window.start.strftime(r"%Y")
    url = f"https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{date}.zip"

    with httpx2.Client(follow_redirects=True, timeout=60) as client:
        response = client.get(url)
        response.raise_for_status()

    context.add_output_metadata(
        {
            "url": dg.MetadataValue.url(url),
            "size_bytes": dg.MetadataValue.int(len(response.content)),
            "response_content_type": response.headers.get("content-type", ""),
        }
    )
    return response.content


@dg.asset(
    key=["b3_history", "yearly", "parse"],
    ins={"yearly_download": dg.AssetIn(key=["b3_history", "yearly", "download"])},
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 1 *",
        fmt=r"%Y",
    ),
    io_manager_key="io_manager_parquet",
    automation_condition=(
        dg.AutomationCondition.any_deps_updated().since_last_handled()
        & ~dg.AutomationCondition.any_deps_in_progress()
        & ~dg.AutomationCondition.in_progress()
    ),
)
def yearly_parse(
    context: dg.AssetExecutionContext, yearly_download: bytes
) -> pl.DataFrame:
    date = context.partition_time_window.start.strftime(r"%Y")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"COTAHIST_A{date}.zip"
        path.write_bytes(yearly_download)

        with database() as conn:
            source = f"zip://{path}/COTAHIST_A{date}.TXT"
            df = conn.sql(COTAHIST_QUERY, params={"source": source}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df
