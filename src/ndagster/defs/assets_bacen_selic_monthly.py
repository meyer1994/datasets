from datetime import timedelta

import dagster as dg
import polars as pl

from ndagster.defs.assets_b3_history import to_markdown
from ndagster.defs.bacen_sgs import download_series, parse_series

# Taxa de juros - Selic acumulada no mês (% a.m.); the current month is partial
SGS_CODE = 4390
DATASET_URL = "https://dadosabertos.bcb.gov.br/dataset/4390-taxa-de-juros---selic-acumulada-no-mes"


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    latest = df.row(-1, named=True)

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "ref_dt": dg.MetadataValue.text(latest["ref_dt"].strftime(r"%Y-%m")),
        "value": dg.MetadataValue.float(float(latest["value"])),
        "blocked_rows": dg.MetadataValue.int(df["blocked"].sum()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
    }


@dg.asset(
    key_prefix=["bacen_selic_monthly"],
    name="monthly_download",
    group_name="bacen_selic_monthly",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2020-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 * *",
        fmt=r"%Y-%m",
    ),
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
    key_prefix=["bacen_selic_monthly"],
    name="monthly_parse",
    ins={
        "monthly_download": dg.AssetIn(key=["bacen_selic_monthly", "monthly_download"])
    },
    group_name="bacen_selic_monthly",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2020-01",
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
    df = parse_series(SGS_CODE, monthly_download).with_columns(
        rate_factor=(pl.col("value") / 100 + 1).cast(pl.Decimal(24, 10))
    )

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df
