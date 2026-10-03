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

# webbed returns no rows for XML files over ~160 MB (this report is ~450 MB)
CHUNK_SIZE = 20_000_000

INSTRUMENTS_QUERY = r"""
WITH rpt AS (
    SELECT
        RptParams.RptDtAndTm.Dt                        AS trad_dt,
        FinInstrmId,
        FinInstrmAttrCmon                              AS cmon,
        -- each InstrmInf holds a single type-specific child (EqtyInf,
        -- OptnOnEqtsInf, FutrCtrctsInf...); JSON tolerates days where a type
        -- or optional field is absent from the inferred schema
        to_json(InstrmInf)                             AS inf
    FROM read_xml(
        $source,
        record_element := 'Instrm',
        all_varchar := true,
        union_by_name := true,
        -- webbed's SAX streaming fails on these files; force DOM parsing
        maximum_file_size := 4294967296
    )
),
records AS (
    SELECT *, inf -> element AS r
    FROM (SELECT *, unnest(json_keys(inf)) AS element FROM rpt)
    WHERE inf ->> element IS NOT NULL
)
SELECT
    trad_dt::DATE                                      AS trad_dt,
    CASE element
        WHEN 'EqtyInf' THEN 'equity'
        WHEN 'OptnOnEqtsInf' THEN 'option_on_equity'
        WHEN 'ExrcEqtsInf' THEN 'equity_option_exercise'
        WHEN 'EqtyFwdInf' THEN 'equity_forward'
        WHEN 'FutrCtrctsInf' THEN 'future'
        WHEN 'OptnOnSpotAndFutrsInf' THEN 'option_on_spot_and_future'
        WHEN 'DrvsOptnExrcInf' THEN 'derivative_option_exercise'
        WHEN 'StrtgyInf' THEN 'strategy'
        WHEN 'FxdIncmInf' THEN 'fixed_income'
        WHEN 'FxdIncmNonTrdblInf' THEN 'fixed_income_non_tradable'
        WHEN 'NtlBdInf' THEN 'national_bond'
        WHEN 'IntlBdInf' THEN 'international_bond'
        WHEN 'ADRInf' THEN 'adr'
        WHEN 'BTCInf' THEN 'securities_lending'
        WHEN 'FICInf' THEN 'fund'
        WHEN 'CshInf' THEN 'cash'
        WHEN 'OTCInf' THEN 'otc'
        ELSE element
    END                                                AS record_type,
    r ->> 'SctyCtgy'                                   AS security_category,
    FinInstrmId.OthrId.Id::BIGINT                      AS instrument_id,
    r ->> 'TckrSymb'                                   AS ticker,
    r ->> 'ISIN'                                       AS isin,
    cmon.Asst                                          AS asset,
    cmon.AsstDesc                                      AS asset_description,
    trim(cmon."Desc")                                  AS description,
    FinInstrmId.PlcOfListg.MktIdrCd                    AS market,
    cmon.Mkt                                           AS market_code,
    cmon.Sgmt                                          AS segment,
    r ->> 'CFICd'                                      AS cfi_code,
    trim(r ->> 'SpcfctnCd')                            AS specification_code,
    r ->> 'CrpnNm'                                     AS corporate_name,
    r ->> 'GovnInd'                                    AS governance,
    coalesce(r ->> 'TradgCcy', r ->> 'Ccy', r ->> 'CcyCd') AS currency,
    (r ->> 'TradgStartDt')::DATE                       AS trading_start_date,
    (r ->> 'TradgEndDt')::DATE                         AS trading_end_date,
    coalesce(r ->> 'XprtnDt', r ->> 'MtrtyDt')::DATE   AS expiration_date,
    r ->> 'XprtnCd'                                    AS expiration_code,
    r ->> 'OptnTp'                                     AS option_type,
    coalesce(r ->> 'OptnStyle', r ->> 'ExrcStyle')     AS option_style,
    (r -> 'ExrcPric' ->> '#text')::DECIMAL(18, 6)      AS strike_price,
    -- unknown underlyings are published as 'XX'
    TRY_CAST(coalesce(
        r ->> '$.UndrlygInstrmId.OthrId.Id',
        r ->> '$.OptnExrcInstrmId.OthrId.Id',
        r ->> '$.DerivOptnExrcInstrmId.OthrId.Id'
    ) AS BIGINT)                                       AS underlying_instrument_id,
    (r ->> 'AllcnRndLot')::BIGINT                      AS round_lot,
    (r ->> 'PricFctr')::BIGINT                         AS price_factor,
    (r ->> 'CtrctMltplr')::DECIMAL(18, 6)              AS contract_multiplier,
    (r ->> 'DaysToSttlm')::BIGINT                      AS days_to_settlement,
    (r -> 'FrstPric' ->> '#text')::DECIMAL(18, 6)      AS first_price,
    (r -> 'LastPric' ->> '#text')::DECIMAL(18, 6)      AS last_price
FROM records
ORDER BY
    trad_dt,
    record_type,
    ticker
"""


def split_report(xml: bytes, directory: Path, name: str) -> list[str]:
    """Write the report as standalone XML files cut at BizGrp boundaries."""
    marker = b"<BizGrp>"
    start = xml.index(marker)
    end = xml.rindex(b"</Xchg>")
    head, tail = xml[:start], xml[end:]

    paths = []
    while start < end:
        stop = xml.find(marker, start + CHUNK_SIZE, end)
        stop = end if stop == -1 else stop
        path = directory / f"{Path(name).stem}_{len(paths):03d}.xml"
        path.write_bytes(head + xml[start:stop] + tail)
        paths.append(str(path))
        start = stop
    return paths


def output_metadata(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    record_types = (
        df.group_by("record_type")
        .agg(
            pl.len().alias("instruments"),
            pl.col("security_category")
            .unique()
            .sort()
            .str.join(",")
            .alias("categories"),
        )
        .sort("instruments", descending=True)
    )

    return {
        "total_rows": dg.MetadataValue.int(df.height),
        "preview": dg.MetadataValue.md(to_markdown(df.head(10))),
        "record_types": dg.MetadataValue.md(
            to_markdown(record_types, thousands_separator=True)
        ),
    }


@dg.asset(
    key_prefix=["b3_instruments"],
    name="daily_download",
    group_name="b3_instruments",
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
    url = f"https://www.b3.com.br/pesquisapregao/download?filelist=IN{date}.zip"

    with httpx2.Client(follow_redirects=True, timeout=120) as client:
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
    key_prefix=["b3_instruments"],
    name="daily_parse",
    ins={"daily_download": dg.AssetIn(key=["b3_instruments", "daily_download"])},
    group_name="b3_instruments",
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
    name, xml = latest_report(daily_download, "BVBG.028.02_")
    context.log.debug("Parsing %s", name)

    with tempfile.TemporaryDirectory() as tmp:
        paths = split_report(xml, Path(tmp), name)
        del xml
        context.log.debug("Split into %d files", len(paths))

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            df = conn.sql(INSTRUMENTS_QUERY, params={"source": paths}).pl()

    context.log.debug("Loaded %d rows", df.height)
    context.log.debug("Schema: %s", df.schema)
    context.add_output_metadata(
        {"source_file": dg.MetadataValue.text(name), **output_metadata(df)}
    )

    return df
