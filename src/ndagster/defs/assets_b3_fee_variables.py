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

FEE_VARIABLES_QUERY = r"""
WITH rpt AS (
    SELECT
        RptParams.RptDtAndTm.Dt                        AS report_dt,
        RptParams.RptNb                                AS report_number,
        RptParams.Frqcy                                AS frqcy,
        VldtyPrd.FrDtTm                                AS valid_from,
        VldtyPrd.ToDtTm                                AS valid_to,
        unnest(FeeInf)                                 AS f
    FROM read_xml(
        $source,
        record_element := 'FeeVarblInf',
        all_varchar := true,
        -- webbed's SAX streaming fails on these files; force DOM parsing
        maximum_file_size := 4294967296
    )
),
fees AS (
    -- Each fee block repeats the same conversion quote; keep the first present
    SELECT
        *,
        coalesce(
            f.OthrFeeQtnInf, f.PrmcFeeInf.QtnInf, f.SttlmFeeInf.QtnInf
        )                                              AS q
    FROM rpt
)
SELECT
    report_dt::DATE                                    AS report_dt,
    report_number::BIGINT                              AS report_number,
    frqcy                                              AS frequency,
    valid_from::TIMESTAMP::DATE                        AS valid_from,
    valid_to::TIMESTAMP::DATE                          AS valid_to,
    f.FinInstrmAttrbts.Sgmt                            AS segment,
    f.FinInstrmAttrbts.Mkt                             AS market,
    f.FinInstrmAttrbts.Asst                            AS asset,
    q.ConvsInd.OthrId.Id::BIGINT                       AS conversion_instrument_id,
    q.RefDt::DATE                                      AS conversion_ref_dt,
    q.ConvsIndVal::DECIMAL(18, 6)                      AS conversion_rate,
    f.PrmcFeeInf.RdctnFctr::DECIMAL(18, 6)             AS holding_reduction_factor,
    f.PrmcFeeInf.DailyFinVal.Amt."#text"::DECIMAL(18, 6) AS holding_daily_value,
    f.PrmcFeeInf.DailyFinVal.Amt.Ccy                   AS holding_daily_value_ccy,
    f.SttlmFeeInf.CtrctVal.Amt."#text"::DECIMAL(18, 6) AS settlement_fee,
    f.SttlmFeeInf.CtrctVal.Amt.Ccy                     AS settlement_fee_ccy,
    f.PhysDlvrySttlmFeeInf.Rate::DECIMAL(18, 6)        AS physical_delivery_rate
FROM fees
ORDER BY
    report_dt,
    segment,
    market,
    asset
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    by_conversion = (
        df.group_by("conversion_instrument_id", "conversion_ref_dt")
        .agg(
            pl.col("conversion_rate").first().cast(pl.Float64),
            pl.len().alias("assets"),
            pl.col("asset").sort().head(8).str.join(", ").alias("sample_assets"),
        )
        .sort("assets", descending=True)
        .with_columns(pl.col("conversion_instrument_id").cast(pl.String))
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "assets": dg.MetadataValue.int(df["asset"].n_unique()),
        "with_settlement_fee": dg.MetadataValue.int(df["settlement_fee"].count()),
        "with_holding_fee": dg.MetadataValue.int(
            df["holding_reduction_factor"].count()
        ),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "by_conversion": dg.MetadataValue.md(
            to_markdown(by_conversion, thousands_separator=True, float_precision=4)
        ),
    }


@dg.asset(
    key_prefix=["b3_fee_variables"],
    name="monthly_download",
    group_name="b3_fee_variables",
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
                f"?filelist=VA{date.strftime(r'%y%m%d')}.zip"
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
    key_prefix=["b3_fee_variables"],
    name="monthly_parse",
    ins={"monthly_download": dg.AssetIn(key=["b3_fee_variables", "monthly_download"])},
    group_name="b3_fee_variables",
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
    name, xml = latest_report(monthly_download, "BVBG.024.01_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(FEE_VARIABLES_QUERY, params={"source": str(path)}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
