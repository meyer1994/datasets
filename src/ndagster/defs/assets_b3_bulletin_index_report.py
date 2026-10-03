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

INDEX_REPORT_QUERY = r"""
WITH rpt AS (
    SELECT *
    FROM read_xml($source, record_element := 'IndxRpt', all_varchar := true)
),
indexes AS (
    SELECT TradDt.Dt AS trad_dt, 'index' AS record_type, unnest(IndxInf) AS r
    FROM rpt
),
iopvs AS (
    SELECT TradDt.Dt AS trad_dt, 'iopv' AS record_type, unnest(IOPVInf) AS r
    FROM rpt
),
bdrs AS (
    SELECT TradDt.Dt AS trad_dt, 'bdr' AS record_type, unnest(BDRInf) AS r
    FROM rpt
),
records AS (
    SELECT
        trad_dt,
        record_type,
        r.SctyInf.SctyId,
        r.SctyInf.FinInstrmId,
        r.AsstDesc,
        r.SctyInf.OpngPric,
        r.SctyInf.MinPric,
        r.SctyInf.MaxPric,
        r.SctyInf.TradAvrgPric,
        r.SctyInf.PrvsDayClsgPric,
        r.SctyInf.ClsgPric,
        r.SctyInf.IndxVal,
        r.SctyInf.OscnVal,
        r.SttlmVal,
        r.RsngShrsNb,
        r.FlngShrsNb,
        r.StblShrsNb
    FROM indexes
    UNION ALL BY NAME
    SELECT
        trad_dt,
        record_type,
        r.SctyId,
        r.FinInstrmId,
        r.OpngPric,
        r.MinPric,
        r.MaxPric,
        r.TradAvrgPric,
        r.PrvsDayClsgPric,
        r.ClsgPric,
        r.IndxVal,
        r.OscnVal
    FROM iopvs
    UNION ALL BY NAME
    SELECT
        trad_dt,
        record_type,
        r.SctyId,
        r.FinInstrmId,
        r.RefPric
    FROM bdrs
)
SELECT
    trad_dt::DATE                                      AS trad_dt,
    record_type,
    SctyId.TckrSymb                                    AS ticker,
    FinInstrmId.OthrId.Id::BIGINT                      AS instrument_id,
    FinInstrmId.PlcOfListg.MktIdrCd                    AS market,
    AsstDesc                                           AS description,
    OpngPric."#text"::DECIMAL(18, 6)                   AS opening_price,
    MinPric."#text"::DECIMAL(18, 6)                    AS min_price,
    MaxPric."#text"::DECIMAL(18, 6)                    AS max_price,
    TradAvrgPric."#text"::DECIMAL(18, 6)               AS avg_price,
    PrvsDayClsgPric."#text"::DECIMAL(18, 6)            AS prev_closing_price,
    ClsgPric."#text"::DECIMAL(18, 6)                   AS closing_price,
    IndxVal."#text"::DECIMAL(18, 6)                    AS index_value,
    OscnVal::DECIMAL(18, 6)                            AS oscillation,
    SttlmVal."#text"::DECIMAL(18, 6)                   AS settlement_value,
    RsngShrsNb::BIGINT                                 AS rising_shares,
    FlngShrsNb::BIGINT                                 AS falling_shares,
    StblShrsNb::BIGINT                                 AS stable_shares,
    RefPric."#text"::DECIMAL(18, 6)                    AS reference_price,
    coalesce(ClsgPric.Ccy, RefPric.Ccy)                AS price_ccy
FROM records
ORDER BY
    trad_dt,
    record_type,
    ticker
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    indexes = (
        df.filter(pl.col("record_type") == "index")
        .select("ticker", "description", "closing_price", "oscillation")
        .sort("ticker")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "indexes": dg.MetadataValue.int(indexes.height),
        "iopvs": dg.MetadataValue.int(df["record_type"].eq("iopv").sum()),
        "bdrs": dg.MetadataValue.int(df["record_type"].eq("bdr").sum()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "index_values": dg.MetadataValue.md(
            to_markdown(indexes, thousands_separator=True, float_precision=4)
        ),
    }


@dg.asset(
    key_prefix=["b3_bulletin_index_report"],
    name="daily_download",
    group_name="b3_bulletin_index_report",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=IR{date}.zip"

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
    key_prefix=["b3_bulletin_index_report"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(key=["b3_bulletin_index_report", "daily_download"])
    },
    group_name="b3_bulletin_index_report",
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
    name, xml = latest_report(daily_download, "BVBG.087.01_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(INDEX_REPORT_QUERY, params={"source": str(path)}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
