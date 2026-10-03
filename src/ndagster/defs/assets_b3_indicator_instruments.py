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

INDICATOR_INSTRUMENTS_QUERY = r"""
SELECT
    RptParams.RptDtAndTm.Dt::DATE                      AS trad_dt,
    InstrmId.OthrId.Id::BIGINT                         AS instrument_id,
    InstrmId.PlcOfListg.MktIdrCd                       AS market,
    InstrmInf.InstrmNm                                 AS instrument_name,
    InstrmInf."Desc"                                   AS description,
    InstrmInf.Asst                                     AS asset,
    InstrmInf.Sgmt                                     AS segment,
    InstrmInf.Mkt                                      AS market_code,
    InstrmInf.SctyCtgy                                 AS security_category,
    i.EcncIndDesc                                      AS indicator_description,
    i.TpCd                                             AS indicator_type_code,
    i.BaseCd                                           AS base_code,
    i.ValTpCd                                          AS value_type_code,
    i.NtryRefCd                                        AS entry_reference_code,
    i.DcmlPrcsn::BIGINT                                AS decimal_precision,
    i.Mtrty                                            AS maturity_code,
    RptParams.Frqcy                                    AS frequency,
    RptParams.ActvtyInd::BOOLEAN                       AS is_active
FROM (
    SELECT *, InstrmDtls.EcncIndsInf AS i
    FROM read_xml(
        $source,
        record_element := 'Inds',
        all_varchar := true,
        -- webbed's SAX streaming fails on these files; force DOM parsing
        maximum_file_size := 4294967296
    )
)
ORDER BY
    trad_dt,
    asset,
    description
"""


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    by_asset = (
        df.group_by("asset", "instrument_name")
        .agg(pl.len().alias("indicators"))
        .top_k(15, by="indicators")
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "assets": dg.MetadataValue.int(df["asset"].n_unique()),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "indicators_by_asset": dg.MetadataValue.md(
            to_markdown(by_asset, thousands_separator=True)
        ),
    }


@dg.asset(
    key_prefix=["b3_indicator_instruments"],
    name="daily_download",
    group_name="b3_indicator_instruments",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=II{date}.zip"

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
    key_prefix=["b3_indicator_instruments"],
    name="daily_parse",
    ins={
        "daily_download": dg.AssetIn(key=["b3_indicator_instruments", "daily_download"])
    },
    group_name="b3_indicator_instruments",
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
    name, xml = latest_report(daily_download, "BVBG.029.02_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(
                INDICATOR_INSTRUMENTS_QUERY, params={"source": str(path)}
            ).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
