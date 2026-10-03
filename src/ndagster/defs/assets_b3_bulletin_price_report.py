import io
import tempfile
import zipfile
from pathlib import Path

import dagster as dg
import duckdb
import httpx2
import polars as pl

from ndagster.defs.assets_b3_history import to_markdown

PRICE_REPORT_QUERY = r"""
SELECT
    TradDt.Dt::DATE                                    AS trad_dt,
    SctyId.TckrSymb                                    AS ticker,
    FinInstrmId.OthrId.Id::BIGINT                      AS instrument_id,
    FinInstrmId.PlcOfListg.MktIdrCd                    AS market,
    TradDtls.TradQty::BIGINT                           AS trades,
    TradDtls.DaysToSttlm::INTEGER                      AS days_to_settlement,
    a.MktDataStrmId                                    AS market_data_stream,
    a.OpnIntrst::BIGINT                                AS open_interest,
    a.FinInstrmQty::BIGINT                             AS instrument_qty,
    a.FrstPric."#text"::DECIMAL(18, 6)                 AS first_price,
    a.MinPric."#text"::DECIMAL(18, 6)                  AS min_price,
    a.MaxPric."#text"::DECIMAL(18, 6)                  AS max_price,
    a.TradAvrgPric."#text"::DECIMAL(18, 6)             AS avg_price,
    a.LastPric."#text"::DECIMAL(18, 6)                 AS last_price,
    coalesce(a.LastPric.Ccy, a.FrstPric.Ccy)           AS price_ccy,
    a.BestBidPric."#text"::DECIMAL(18, 6)              AS best_bid,
    a.BestAskPric."#text"::DECIMAL(18, 6)              AS best_ask,
    coalesce(a.BestBidPric.Ccy, a.BestAskPric.Ccy)     AS quote_ccy,
    a.OscnPctg::DECIMAL(18, 6)                         AS oscillation_pct,
    a.NtlFinVol."#text"::DECIMAL(18, 2)                AS volume_brl,
    a.IntlFinVol."#text"::DECIMAL(18, 2)               AS volume_usd,
    a.RglrTxsQty::BIGINT                               AS regular_trades,
    a.RglrTraddCtrcts::BIGINT                          AS regular_contracts,
    a.NtlRglrVol."#text"::DECIMAL(18, 2)               AS regular_volume_brl,
    a.IntlRglrVol."#text"::DECIMAL(18, 2)              AS regular_volume_usd,
    a.NonRglrTxsQty::BIGINT                            AS non_regular_trades,
    a.NonRglrTraddCtrcts::BIGINT                       AS non_regular_contracts,
    a.NtlNonRglrVol."#text"::DECIMAL(18, 2)            AS non_regular_volume_brl,
    a.IntlNonRglrVol."#text"::DECIMAL(18, 2)           AS non_regular_volume_usd,
    a.AdjstdQt."#text"::DECIMAL(18, 6)                 AS adjusted_quote,
    a.AdjstdQt.Ccy                                     AS adjusted_quote_ccy,
    a.AdjstdQtTax."#text"::DECIMAL(18, 6)              AS adjusted_quote_rate,
    a.AdjstdQtStin                                     AS adjusted_quote_status,
    a.PrvsAdjstdQt."#text"::DECIMAL(18, 6)             AS prev_adjusted_quote,
    a.PrvsAdjstdQtTax."#text"::DECIMAL(18, 6)          AS prev_adjusted_quote_rate,
    a.PrvsAdjstdQtStin                                 AS prev_adjusted_quote_status,
    a.VartnPts."#text"::DECIMAL(18, 6)                 AS variation_points,
    a.AdjstdValCtrct."#text"::DECIMAL(24, 12)          AS adjusted_contract_value,
    a.EqvtVal."#text"::DECIMAL(18, 6)                  AS equivalent_value_brl,
    a.MinTradLmt."#text"::DECIMAL(18, 6)               AS min_trade_limit,
    a.MaxTradLmt."#text"::DECIMAL(18, 6)               AS max_trade_limit,
    a.MaxTradLmt.Ccy                                   AS trade_limit_ccy
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
    ticker,
    days_to_settlement
"""


def latest_report(archive: bytes, prefix: str) -> tuple[str, bytes]:
    """Return the name and content of the most recent report XML.

    B3 bulletin downloads are a zip wrapping e.g. PRyymmdd.zip, which holds
    one XML per revision of the session's report; names sort chronologically.
    """
    with zipfile.ZipFile(io.BytesIO(archive)) as outer:
        (inner_name,) = outer.namelist()
        inner_bytes = outer.read(inner_name)

    with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
        names = sorted(n for n in inner.namelist() if n.startswith(prefix))
        return names[-1], inner.read(names[-1])


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    top_volume = (
        df.filter(pl.col("volume_brl").is_not_null())
        .select("ticker", "trades", "last_price", "oscillation_pct", "volume_brl")
        .top_k(10, by="volume_brl")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "traded_instruments": dg.MetadataValue.int(df["trades"].count()),
        "total_trades": dg.MetadataValue.int(int(df["trades"].sum())),
        "total_volume_brl": dg.MetadataValue.float(float(df["volume_brl"].sum())),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "top_volume": dg.MetadataValue.md(
            to_markdown(top_volume, thousands_separator=True, float_precision=2)
        ),
    }


@dg.asset(
    key_prefix=["b3_bulletin_price_report"],
    name="daily_download",
    group_name="b3_bulletin_price_report",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=PR{date}.zip"

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
    key_prefix=["b3_bulletin_price_report"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(key=["b3_bulletin_price_report", "daily_download"])
    },
    group_name="b3_bulletin_price_report",
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
    name, xml = latest_report(daily_download, "BVBG.086.01_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(PRICE_REPORT_QUERY, params={"source": str(path)}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
