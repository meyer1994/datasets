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

STANDARDIZED_INSTRUMENT_GROUPS_QUERY = r"""
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
            'record_type': 'VARCHAR',
            'field2': 'VARCHAR',
            'field3': 'VARCHAR',
            'field4': 'VARCHAR',
            'field5': 'VARCHAR'
        }
    )
),
header AS (
    SELECT strptime(field2, '%d/%m/%Y')::DATE AS trad_dt
    FROM raw
    WHERE record_type = '01'
)
SELECT
    header.trad_dt                                     AS trad_dt,
    raw.field2::BIGINT                                 AS group_id,
    raw.field3                                         AS market,
    raw.field4::BIGINT                                 AS instrument_id,
    raw.field5                                         AS security_id_source
FROM raw
CROSS JOIN header
WHERE raw.record_type = '02'
ORDER BY
    trad_dt,
    group_id,
    instrument_id
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    largest_groups = (
        df.group_by("group_id")
        .agg(pl.len().alias("instruments"))
        .top_k(10, by="instruments")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "groups": dg.MetadataValue.int(df["group_id"].n_unique()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "largest_groups": dg.MetadataValue.md(to_markdown(largest_groups)),
    }


@dg.asset(
    key_prefix=["b3_standardized_instrument_groups"],
    name="daily_download",
    group_name="b3_standardized_instrument_groups",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=AI{date}.zip"

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
    key_prefix=["b3_standardized_instrument_groups"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(
            key=["b3_standardized_instrument_groups", "daily_download"]
        )
    },
    group_name="b3_standardized_instrument_groups",
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
    name, txt = latest_report(daily_download, "AgrupamentoInstrumentosPadronizados")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(txt)

        with duckdb.connect() as conn:
            df = conn.sql(
                STANDARDIZED_INSTRUMENT_GROUPS_QUERY, params={"source": str(path)}
            ).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
