from datetime import timedelta

import dagster as dg
import polars as pl

from ndagster.defs.assets_b3_history import to_markdown
from ndagster.defs.bacen_sgs import download_series, parse_series

SGS_CODE = 25149
DATASET_URL = "https://dadosabertos.bcb.gov.br/dataset/25149-cartoes-de-credito-ativos"


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    latest = df.sort("ref_dt").tail(1)

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "ref_year": dg.MetadataValue.int(latest["ref_dt"].item().year),
        "active_cards_millions": dg.MetadataValue.float(float(latest["value"].item())),
        "blocked_rows": dg.MetadataValue.int(int(df["blocked"].sum())),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
    }


@dg.asset(
    key_prefix=["bacen_credit_cards"],
    name="yearly_download",
    group_name="bacen_credit_cards",
    # The series stopped after 2022; end is exclusive so 2022 is the last partition
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2010",
        end="2023",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 1 *",
        fmt=r"%Y",
    ),
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=60,
        max_retries=3,
    ),
)
def yearly_download(context: dg.AssetExecutionContext) -> bytes:
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
    key_prefix=["bacen_credit_cards"],
    name="yearly_parse",
    ins={"yearly_download": dg.AssetIn(key=["bacen_credit_cards", "yearly_download"])},
    group_name="bacen_credit_cards",
    # The series stopped after 2022; end is exclusive so 2022 is the last partition
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2010",
        end="2023",
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
    df = parse_series(SGS_CODE, yearly_download)

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df
