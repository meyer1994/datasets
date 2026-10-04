from datetime import timedelta

import dagster as dg
import polars as pl

from ndagster.defs.assets_b3_history import to_markdown
from ndagster.defs.bacen_sgs import download_series, parse_series

# Share of the total credit portfolio overdue for more than 90 days (%);
# published with a ~2 month lag
SGS_CODE = 21082

DATASET_URL = (
    "https://dadosabertos.bcb.gov.br/dataset/"
    "21082-inadimplencia-da-carteira-de-credito---total"
)

PARTITIONS = dg.TimeWindowPartitionsDefinition(
    start="2020-01",
    timezone="America/Sao_Paulo",
    cron_schedule="0 0 1 * *",
    fmt=r"%Y-%m",
)


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    latest = df.sort("ref_dt").tail(1)

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "ref_dt": dg.MetadataValue.text(
            str(latest["ref_dt"].item()) if latest.height else ""
        ),
        "value": dg.MetadataValue.float(
            float(latest["value"].item()) if latest.height else 0.0
        ),
        "blocked_rows": dg.MetadataValue.int(int(df["blocked"].sum())),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10), float_precision=2)),
    }


@dg.asset(
    key_prefix=["bacen_credit_default"],
    name="monthly_download",
    group_name="bacen_credit_default",
    partitions_def=PARTITIONS,
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=60,
        max_retries=3,
    ),
)
def monthly_download(context: dg.AssetExecutionContext) -> bytes:
    window = context.partition_time_window
    start = window.start.date()
    end = (window.end - timedelta(days=1)).date()

    name, archive = download_series(SGS_CODE, start, end)

    context.add_output_metadata(
        {
            "url": dg.MetadataValue.url(DATASET_URL),
            "source_file": dg.MetadataValue.text(name),
            "size_bytes": dg.MetadataValue.int(len(archive)),
        }
    )
    return archive


@dg.asset(
    key_prefix=["bacen_credit_default"],
    name="monthly_parse",
    ins={
        "monthly_download": dg.AssetIn(key=["bacen_credit_default", "monthly_download"])
    },
    group_name="bacen_credit_default",
    partitions_def=PARTITIONS,
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
    df = parse_series(SGS_CODE, monthly_download)

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df
