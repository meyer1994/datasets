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

FEE_DAILY_UNIT_COST_QUERY = r"""
WITH rpt AS (
    SELECT *
    FROM read_xml(
        $source,
        record_element := 'FeeUnitCostRpt',
        all_varchar := true,
        -- webbed's SAX streaming fails on these files; force DOM parsing
        maximum_file_size := 4294967296
    )
),
fees AS (
    SELECT
        InitlApplDt.Dt                                 AS trad_dt,
        RptParams.RptDtAndTm.Dt                        AS report_dt,
        RptParams.RptNb                                AS report_number,
        unnest(InstrmInf.FeeInstrmInf)                 AS f
    FROM rpt
)
SELECT
    trad_dt::DATE                                      AS trad_dt,
    report_dt::DATE                                    AS report_dt,
    report_number::BIGINT                              AS report_number,
    f.FinInstrmAttrbts.Sgmt                            AS segment,
    f.FinInstrmAttrbts.Mkt                             AS market,
    f.FinInstrmAttrbts.Asst                            AS asset,
    f.FinInstrmAttrbts.XprtnCd                         AS expiration_code,
    f.TradInf.DayTradInd::BOOLEAN                      AS day_trade,
    f.TradInf.TradTxTp                                 AS trade_type,
    f.TradInf.StrtgyLegCd                              AS strategy_leg,
    f.BacenAuctnDtls.BacenAuctnInd::BOOLEAN            AS bacen_auction,
    f.XchgFeeUnitCost.Amt."#text"::DECIMAL(18, 10)     AS exchange_fee,
    f.RegnFeeUnitCost.Amt."#text"::DECIMAL(18, 10)     AS registration_fee,
    coalesce(f.XchgFeeUnitCost.Amt.Ccy, f.RegnFeeUnitCost.Amt.Ccy) AS fee_ccy
FROM fees
ORDER BY
    trad_dt,
    asset,
    expiration_code,
    day_trade,
    trade_type,
    strategy_leg
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    by_asset = (
        df.group_by("asset")
        .agg(
            pl.len().alias("entries"),
            pl.col("expiration_code").n_unique().alias("expirations"),
            pl.col("exchange_fee")
            .cast(pl.Float64)
            .median()
            .alias("median_exchange_fee"),
            pl.col("registration_fee")
            .cast(pl.Float64)
            .median()
            .alias("median_registration_fee"),
        )
        .sort("entries", descending=True)
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "assets": dg.MetadataValue.int(by_asset.height),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "by_asset": dg.MetadataValue.md(
            to_markdown(by_asset, thousands_separator=True, float_precision=4)
        ),
    }


@dg.asset(
    key_prefix=["b3_fee_daily_unit_cost"],
    name="daily_download",
    group_name="b3_fee_daily_unit_cost",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=DI{date}.zip"

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
    key_prefix=["b3_fee_daily_unit_cost"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(key=["b3_fee_daily_unit_cost", "daily_download"])
    },
    group_name="b3_fee_daily_unit_cost",
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
    name, xml = latest_report(daily_download, "BVBG.044.01_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(FEE_DAILY_UNIT_COST_QUERY, params={"source": str(path)}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
