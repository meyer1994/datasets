import io
import tempfile
import zipfile
from pathlib import Path

import dagster as dg
import duckdb
import httpx2
import polars as pl

from ndagster.defs.assets_b3_bulletin_price_report import latest_report
from ndagster.defs.assets_b3_history import to_markdown

DAILY_LIQUIDITY_LIMITS_OTHER_QUERY = r"""
WITH raw AS (
    SELECT *
    FROM read_csv(
        $source,
        auto_detect := false,
        delim := ';',
        header := false,
        quote := '',
        null_padding := true,
        columns := {
            'field1': 'VARCHAR',
            'field2': 'VARCHAR',
            'field3': 'VARCHAR',
            'field4': 'VARCHAR',
            'field5': 'VARCHAR',
            'field6': 'VARCHAR'
        }
    )
),
header AS (
    SELECT strptime(field1, '%d/%m/%Y')::DATE AS trad_dt
    FROM raw
    WHERE field2 IS NULL
)
SELECT
    header.trad_dt                                     AS trad_dt,
    raw.field1                                         AS clearing_house,
    raw.field2                                         AS instrument_id_source,
    raw.field3::BIGINT                                 AS instrument_id,
    raw.field4                                         AS ticker,
    raw.field5::DECIMAL(18, 4)                         AS liquidity_limit,
    raw.field6::DECIMAL(18, 4)                         AS other_limit
FROM raw
CROSS JOIN header
WHERE raw.field2 IS NOT NULL
ORDER BY
    trad_dt,
    ticker
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    limited = df.filter(pl.col("other_limit") > 0)
    largest_limits = limited.select("ticker", "liquidity_limit", "other_limit").top_k(
        10, by="other_limit"
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "instruments_with_other_limit": dg.MetadataValue.int(limited.height),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "largest_limits": dg.MetadataValue.md(
            to_markdown(largest_limits, thousands_separator=True)
        ),
    }


@dg.asset(
    key_prefix=["b3_daily_liquidity_limits_other"],
    name="daily_download",
    group_name="b3_daily_liquidity_limits_other",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 * * 1-5",
        fmt=r"%Y-%m-%d",
    ),
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=60,
        max_retries=3,
    ),
)
def daily_download(context: dg.AssetExecutionContext) -> bytes:
    date = context.partition_time_window.start.strftime(r"%y%m%d")
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=LA{date}.zip"

    with httpx2.Client(follow_redirects=True, timeout=60) as client:
        response = client.get(url)
        response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        if not archive.namelist():
            raise dg.Failure(
                f"Empty archive for {date}: no session or not published yet"
            )

    context.add_output_metadata(
        {
            "url": dg.MetadataValue.url(url),
            "size_bytes": dg.MetadataValue.int(len(response.content)),
            "response_content_type": response.headers.get("content-type", ""),
        }
    )
    return response.content


@dg.asset(
    key_prefix=["b3_daily_liquidity_limits_other"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(
            key=["b3_daily_liquidity_limits_other", "daily_download"]
        )
    },
    group_name="b3_daily_liquidity_limits_other",
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
    name, txt = latest_report(daily_download, "LLDAdicional")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(txt)

        with duckdb.connect() as conn:
            df = conn.sql(
                DAILY_LIQUIDITY_LIMITS_OTHER_QUERY, params={"source": str(path)}
            ).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
