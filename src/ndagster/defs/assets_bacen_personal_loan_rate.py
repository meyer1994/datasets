from datetime import timedelta

import dagster as dg
import polars as pl

from ndagster.defs.assets_b3_history import to_markdown
from ndagster.defs.bacen_sgs import download_series, parse_series

# Taxa média mensal de juros - recursos livres - Pessoas físicas - Crédito
# pessoal não consignado (% a.m.); published with a ~2 month lag
SGS_CODE = 25464
DATASET_URL = (
    "https://dadosabertos.bcb.gov.br/dataset/"
    "25464-taxa-media-mensal-de-juros-das-operacoes-de-credito-com-recursos-livres"
    "---pessoas-fisicas---c"
)


def annualized(monthly: pl.Expr) -> pl.Expr:
    """((1 + monthly / 100) ^ 12 - 1) * 100, compounded in DECIMAL."""
    factor = (monthly.cast(pl.Decimal(38, 18)) / 100 + 1).cast(pl.Decimal(38, 18))
    compounded = factor
    for _ in range(11):
        compounded = (compounded * factor).cast(pl.Decimal(38, 18))
    return ((compounded - 1) * 100).cast(pl.Decimal(24, 8))


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    latest = df.sort("ref_dt").tail(1)

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "ref_dt": dg.MetadataValue.text(
            latest["ref_dt"].dt.strftime(r"%Y-%m").item() if df.height else ""
        ),
        "value": dg.MetadataValue.float(float(latest["value"].item()))
        if df.height
        else dg.MetadataValue.null(),
        "annualized_value": dg.MetadataValue.float(
            float(latest["annualized_value"].item())
        )
        if df.height
        else dg.MetadataValue.null(),
        "blocked_rows": dg.MetadataValue.int(df["blocked"].sum()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
    }


@dg.asset(
    key_prefix=["bacen_personal_loan_rate"],
    name="monthly_download",
    group_name="bacen_personal_loan_rate",
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
    key_prefix=["bacen_personal_loan_rate"],
    name="monthly_parse",
    ins={
        "monthly_download": dg.AssetIn(
            key=["bacen_personal_loan_rate", "monthly_download"]
        )
    },
    group_name="bacen_personal_loan_rate",
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
        annualized_value=annualized(pl.col("value"))
    )

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(output_metadata(df))

    return df
