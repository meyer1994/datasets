import dagster as dg
import polars as pl

from ndagster.defs.assets_b3_history import to_markdown
from ndagster.defs.bacen_sgs import download_series, parse_series

SGS_CODE = 11
DATASET_URL = "https://dadosabertos.bcb.gov.br/dataset/11-taxa-de-juros---selic"
BUSINESS_DAYS_PER_YEAR = 252


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    latest = df.sort("ref_dt").tail(1)

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "latest_ref_dt": dg.MetadataValue.text(str(latest["ref_dt"].item())),
        "latest_daily_rate": dg.MetadataValue.float(float(latest["value"].item())),
        "latest_annualized_rate": dg.MetadataValue.float(
            float(latest["annualized_rate"].item())
        ),
        "blocked_rows": dg.MetadataValue.int(int(df["blocked"].sum())),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
    }


@dg.asset(
    key_prefix=["bacen_selic"],
    name="daily_download",
    group_name="bacen_selic",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="1986-06-04",
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
    day = context.partition_time_window.start.date()
    name, archive = download_series(SGS_CODE, day, day)

    context.add_output_metadata(
        {
            "url": dg.MetadataValue.url(DATASET_URL),
            "source_file": dg.MetadataValue.text(name),
            "size_bytes": dg.MetadataValue.int(len(archive)),
        }
    )
    return archive


@dg.asset(
    key_prefix=["bacen_selic"],
    name="daily_parse",
    ins={"daily_download": dg.AssetIn(key=["bacen_selic", "daily_download"])},
    group_name="bacen_selic",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="1986-06-04",
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
    df = parse_series(SGS_CODE, daily_download).with_columns(
        # value is % a.d.; compound over 252 business days to get % a.a.
        (
            ((1 + pl.col("value").cast(pl.Float64) / 100) ** BUSINESS_DAYS_PER_YEAR - 1)
            * 100
        )
        .round(8)
        .cast(pl.Decimal(24, 8))
        .alias("annualized_rate")
    )

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df
