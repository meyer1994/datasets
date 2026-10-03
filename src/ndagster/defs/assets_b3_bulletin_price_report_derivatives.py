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

PRICE_REPORT_DERIVATIVES_QUERY = r"""
SELECT
    TradDt.Dt::DATE                                    AS trad_dt,
    SctyId.TckrSymb                                    AS ticker,
    FinInstrmId.OthrId.Id::BIGINT                      AS instrument_id,
    FinInstrmId.PlcOfListg.MktIdrCd                    AS market,
    a.OpnIntrst::BIGINT                                AS open_interest,
    a.FrstPric."#text"::DECIMAL(18, 6)                 AS first_price,
    a.MinPric."#text"::DECIMAL(18, 6)                  AS min_price,
    a.MaxPric."#text"::DECIMAL(18, 6)                  AS max_price,
    a.TradAvrgPric."#text"::DECIMAL(18, 6)             AS avg_price,
    a.LastPric."#text"::DECIMAL(18, 6)                 AS last_price,
    a.LastPric.Ccy                                     AS price_ccy,
    a.RglrTxsQty::BIGINT                               AS regular_trades,
    a.AdjstdQt."#text"::DECIMAL(18, 6)                 AS adjusted_quote,
    a.AdjstdQt.Ccy                                     AS adjusted_quote_ccy,
    a.AdjstdQtTax."#text"::DECIMAL(18, 6)              AS adjusted_quote_rate,
    a.AdjstdQtStin                                     AS adjusted_quote_status,
    a.PrvsAdjstdQt."#text"::DECIMAL(18, 6)             AS prev_adjusted_quote,
    a.PrvsAdjstdQtTax."#text"::DECIMAL(18, 6)          AS prev_adjusted_quote_rate,
    a.PrvsAdjstdQtStin                                 AS prev_adjusted_quote_status
FROM (
    SELECT *, FinInstrmAttrbts AS a
    FROM read_xml(
        $source,
        record_element := 'PricRpt',
        all_varchar := true,
        -- webbed's SAX streaming fails on these files; force DOM parsing
        maximum_file_size := 4294967296
    )
)
ORDER BY
    trad_dt,
    ticker
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    top_trades = (
        df.filter(pl.col("regular_trades").is_not_null())
        .select("ticker", "regular_trades", "last_price", "adjusted_quote")
        .top_k(10, by="regular_trades")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "traded_instruments": dg.MetadataValue.int(df["regular_trades"].count()),
        "total_trades": dg.MetadataValue.int(int(df["regular_trades"].sum())),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "top_trades": dg.MetadataValue.md(
            to_markdown(top_trades, thousands_separator=True, float_precision=2)
        ),
    }


@dg.asset(
    key_prefix=["b3_bulletin_price_report_derivatives"],
    name="daily_download",
    group_name="b3_bulletin_price_report_derivatives",
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2025-01-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 * * 1-5",
        fmt=r"%Y-%m-%d",
    ),
    io_manager_key="io_manager_raw",
    retry_policy=dg.RetryPolicy(
        delay=600,
        max_retries=6,
    ),
)
def daily_download(context: dg.AssetExecutionContext) -> bytes:
    date = context.partition_time_window.start.strftime(r"%y%m%d")
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRD{date}.zip"

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
    key_prefix=["b3_bulletin_price_report_derivatives"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(
            key=["b3_bulletin_price_report_derivatives", "daily_download"]
        )
    },
    group_name="b3_bulletin_price_report_derivatives",
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
    name, xml = latest_report(daily_download, "BVBG.187.01_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(
                PRICE_REPORT_DERIVATIVES_QUERY, params={"source": str(path)}
            ).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
