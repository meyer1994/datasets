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

FEE_UNIT_COST_QUERY = r"""
WITH rpt AS (
    SELECT
        RptParams.RptDtAndTm.Dt                        AS rpt_dt,
        RptParams.RptNb                                AS rpt_nb,
        RptParams.Frqcy                                AS frqcy,
        InitlApplDt.Dt                                 AS valid_from,
        FnlApplDt.Dt                                   AS valid_to,
        unnest(InstrmInf.FeeInstrmInf)                 AS r
    FROM read_xml(
        $source,
        record_element := 'FeeUnitCostRpt',
        all_varchar := true,
        -- webbed's SAX streaming fails on these files; force DOM parsing
        maximum_file_size := 4294967296
    )
)
SELECT
    rpt_dt::DATE                                       AS rpt_dt,
    rpt_nb::BIGINT                                     AS report_number,
    frqcy                                              AS frequency,
    valid_from::DATE                                   AS valid_from,
    valid_to::DATE                                     AS valid_to,
    r.FinInstrmAttrbts.Sgmt                            AS segment,
    r.FinInstrmAttrbts.Mkt                             AS market,
    r.FinInstrmAttrbts.Asst                            AS asset,
    r.TradInf.DayTradInd::BOOLEAN                      AS day_trade,
    r.TradInf.TradTxTp                                 AS trade_type,
    r.TradInf.StrtgyLegCd                              AS strategy_leg,
    r.XchgFeeUnitCost.Amt."#text"::DECIMAL(18, 6)      AS exchange_fee,
    r.RegnFeeUnitCost.Amt."#text"::DECIMAL(18, 6)      AS registration_fee,
    coalesce(r.XchgFeeUnitCost.Amt.Ccy, r.RegnFeeUnitCost.Amt.Ccy) AS fee_ccy,
    r.RlvrInitlDt.Dt::DATE                             AS rollover_from,
    r.RlvrFnlDt.Dt::DATE                               AS rollover_to,
    r.RlvrXchgFeeUnitCost.Amt."#text"::DECIMAL(18, 6)  AS rollover_exchange_fee,
    r.RlvrRegnFeeUnitCost.Amt."#text"::DECIMAL(18, 6)  AS rollover_registration_fee
FROM rpt
ORDER BY
    rpt_dt,
    segment,
    market,
    asset,
    day_trade,
    trade_type,
    strategy_leg
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    top_fees = (
        df.filter(~pl.col("day_trade") & pl.col("strategy_leg").is_null())
        .with_columns(total_fee=pl.col("exchange_fee") + pl.col("registration_fee"))
        .select(
            "market",
            "asset",
            "trade_type",
            "exchange_fee",
            "registration_fee",
            "total_fee",
        )
        .top_k(10, by="total_fee")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "assets": dg.MetadataValue.int(df["asset"].n_unique()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "top_fees": dg.MetadataValue.md(
            to_markdown(top_fees, thousands_separator=True, float_precision=2)
        ),
    }


@dg.asset(
    key_prefix=["b3_fee_unit_cost"],
    name="monthly_download",
    group_name="b3_fee_unit_cost",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01",
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
    # Published on the first business day after the report month
    publication_month = context.partition_time_window.end

    with httpx2.Client(follow_redirects=True, timeout=60) as client:
        for day in range(1, 11):
            date = publication_month.replace(day=day)
            if date.weekday() >= 5:
                continue

            url = (
                "https://www.b3.com.br/pesquisapregao/download"
                f"?filelist=UN{date.strftime(r'%y%m%d')}.zip"
            )
            response = client.get(url)
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                if archive.namelist():
                    break
        else:
            raise dg.Failure(
                f"Empty archives for {publication_month.strftime(r'%Y-%m')}: "
                "not published yet"
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
    key_prefix=["b3_fee_unit_cost"],
    name="monthly_parse",
    ins={"monthly_download": dg.AssetIn(key=["b3_fee_unit_cost", "monthly_download"])},
    group_name="b3_fee_unit_cost",
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
    name, xml = latest_report(monthly_download, "BVBG.043.01_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(FEE_UNIT_COST_QUERY, params={"source": str(path)}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
