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

PRIMITIVE_RISK_FACTORS_QUERY = r"""
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
            'field5': 'VARCHAR',
            'field6': 'VARCHAR',
            'field7': 'VARCHAR',
            'field8': 'VARCHAR',
            'field9': 'VARCHAR',
            'field10': 'VARCHAR',
            'field11': 'VARCHAR'
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
    raw.field2::BIGINT                                 AS fpr_id,
    raw.field3                                         AS fpr_name,
    CASE raw.field4
        WHEN '1' THEN 'Percentage'
        WHEN '2' THEN 'Basis points'
        ELSE raw.field4
    END                                                AS variation_format,
    raw.field5::BIGINT                                 AS fpr_group_id,
    raw.field6                                         AS clearing_house,
    raw.field7::BIGINT                                 AS instrument_id,
    raw.field8                                         AS security_id_source,
    nullif(raw.field9::BIGINT, 0)                      AS day_count_base,
    nullif(raw.field10::BIGINT, 0)                     AS interpolation_base,
    CASE raw.field11
        WHEN '0' THEN 'Price'
        WHEN '1' THEN 'Linear'
        WHEN '2' THEN 'Exponential'
        ELSE raw.field11
    END                                                AS capitalization
FROM raw
CROSS JOIN header
WHERE raw.record_type = '02'
ORDER BY
    trad_dt,
    fpr_id,
    instrument_id
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    by_format = (
        df.group_by("variation_format", "capitalization")
        .agg(
            pl.col("fpr_id").n_unique().alias("fprs"),
            pl.col("instrument_id").n_unique().alias("instruments"),
        )
        .sort("fprs", descending=True)
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "fprs": dg.MetadataValue.int(df["fpr_id"].n_unique()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "fprs_by_format": dg.MetadataValue.md(
            to_markdown(by_format, thousands_separator=True)
        ),
    }


@dg.asset(
    key_prefix=["b3_primitive_risk_factors"],
    name="daily_download",
    group_name="b3_primitive_risk_factors",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=FP{date}.zip"

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
    key_prefix=["b3_primitive_risk_factors"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(
            key=["b3_primitive_risk_factors", "daily_download"]
        )
    },
    group_name="b3_primitive_risk_factors",
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
    name, txt = latest_report(daily_download, "FatoresPrimitivosRisco")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(txt)

        with duckdb.connect() as conn:
            df = conn.sql(
                PRIMITIVE_RISK_FACTORS_QUERY, params={"source": str(path)}
            ).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
