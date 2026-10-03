import contextlib

import dagster as dg
import duckdb
import pandas as pd

COTAHIST_QUERY = r"""
SELECT
    regexp_extract(line, '^(\d{2})', 1) as tipreg,
    regexp_replace(line, '^.{2}(\d{4})(\d{2})(\d{2}).*', '\1-\2-\3')::date as dtpreg,
    regexp_extract(line, '^.{10}(.{2})', 1) as codbdi,
    trim(regexp_extract(line, '^.{12}(.{12})', 1)) as codneg,
    regexp_extract(line, '^.{24}(.{3})', 1) as tpmerc,
    coalesce(nullif(trim(regexp_extract(line, '^.{27}(.{12})', 1)), ''), codneg) as nomres,
    trim(regexp_extract(line, '^.{39}(.{10})', 1)) as especi,
    trim(regexp_extract(line, '^.{49}(.{3})', 1)) as prazot,
    trim(regexp_extract(line, '^.{52}(.{4})', 1)) as modref,
    regexp_replace(line, '^.{56}(\d{11})(\d{2}).*', '\1.\2')::double as preabe,
    regexp_replace(line, '^.{69}(\d{11})(\d{2}).*', '\1.\2')::double as premax,
    regexp_replace(line, '^.{82}(\d{11})(\d{2}).*', '\1.\2')::double as premin,
    regexp_replace(line, '^.{95}(\d{11})(\d{2}).*', '\1.\2')::double as premed,
    regexp_replace(line, '^.{108}(\d{11})(\d{2}).*', '\1.\2')::double as preult,
    regexp_replace(line, '^.{121}(\d{11})(\d{2}).*', '\1.\2')::double as preofc,
    regexp_replace(line, '^.{134}(\d{11})(\d{2}).*', '\1.\2')::double as preofv,
    regexp_extract(line, '^.{147}(\d{5})', 1)::bigint as totneg,
    regexp_extract(line, '^.{152}(\d{18})', 1)::bigint as quatot,
    regexp_replace(line, '^.{170}(\d{16})(\d{2}).*', '\1.\2')::double as voltot,
    regexp_replace(line, '^.{188}(\d{11})(\d{2}).*', '\1.\2')::double as preexe,
    regexp_extract(line, '^.{201}(.)', 1) as indopc,
    regexp_replace(line, '^.{202}(\d{4})(\d{2})(\d{2}).*', '\1-\2-\3')::date as datven,
    regexp_extract(line, '^.{210}(\d{7})', 1) as fatcot,
    regexp_replace(line, '^.{217}(\d{11})(\d{2}).*', '\1.\2')::double as ptoexe,
    regexp_extract(line, '^.{230}(.{12})', 1) as codisi,
    regexp_extract(line, '^.{242}(\d{3})', 1) as dismes
FROM read_csv(
    $source,
    columns = {'line': 'varchar'},
    header = false,
    delim = '\t',
    quote = '',
    escape = '',
    encoding = 'latin-1',
    auto_detect = false
)
WHERE 
    regexp_matches(line, '^01')
ORDER BY 
    dtpreg,
    nomres,
    codneg,
    tipreg,
    codisi
"""


@contextlib.contextmanager
def database():
    with duckdb.connect() as conn:
        conn.install_extension("zipfs")
        conn.install_extension("httpfs")
        conn.load_extension("zipfs")
        conn.load_extension("httpfs")
        yield conn


def output_metadata(df: pd.DataFrame) -> dict[str, dg.MetadataValue]:
    top_volume = (
        df.groupby(["codneg", "nomres"], as_index=False)["voltot"]
        .sum()
        .nlargest(10, "voltot")
    )

    return {
        "total_rows": dg.MetadataValue.int(len(df)),
        "avg_price": dg.MetadataValue.float(float(df["premed"].mean())),
        "total_trades": dg.MetadataValue.int(int(df["totneg"].sum())),
        "total_volume": dg.MetadataValue.float(float(df["voltot"].sum())),
        "preview": dg.MetadataValue.md(
            df.head(10).to_markdown(index=False, disable_numparse=True)
        ),
        "top_volume": dg.MetadataValue.md(
            top_volume.to_markdown(index=False, floatfmt=",.2f")
        ),
    }


@dg.asset(
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2026-09-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 * * 1-5",
        fmt=r"%Y-%m-%d",
    ),
    retry_policy=dg.RetryPolicy(
        delay=600,
        max_retries=6,
    ),
)
def daily(context: dg.AssetExecutionContext) -> pd.DataFrame:
    date = context.partition_time_window.start.strftime(r"%d%m%Y")
    url = f"zip://https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D{date}.zip/COTAHIST_D{date}.TXT"

    with database() as conn:
        df = conn.sql(COTAHIST_QUERY, params={"source": url}).df()

    context.log.debug("Loaded %d rows", len(df))
    context.log.debug("Columns: %s", df.columns)
    context.log.debug("Dtypes: %s", df.dtypes)
    context.add_output_metadata(output_metadata(df))

    return df


@dg.asset(
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="2026-01",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 * *",
        fmt=r"%Y-%m",
    ),
    retry_policy=dg.RetryPolicy(
        delay=3600,
        max_retries=6,
    ),
)
def monthly(context: dg.AssetExecutionContext) -> pd.DataFrame:
    date = context.partition_time_window.start.strftime(r"%m%Y")
    url = f"zip://https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M{date}.zip/COTAHIST_M{date}.TXT"

    with database() as conn:
        df = conn.sql(COTAHIST_QUERY, params={"source": url}).df()

    context.log.debug("Loaded %d rows", len(df))
    context.log.debug("Columns: %s", df.columns)
    context.log.debug("Dtypes: %s", df.dtypes)
    context.add_output_metadata(output_metadata(df))

    return df


@dg.asset(
    partitions_def=dg.TimeWindowPartitionsDefinition(
        start="1986",
        timezone="America/Sao_Paulo",
        cron_schedule="0 0 1 1 *",
        fmt=r"%Y",
    ),
    retry_policy=dg.RetryPolicy(
        delay=3600,
        max_retries=6,
    ),
)
def yearly(context: dg.AssetExecutionContext) -> pd.DataFrame:
    date = context.partition_time_window.start.strftime(r"%Y")
    url = f"zip://https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{date}.zip/COTAHIST_A{date}.TXT"

    with database() as conn:
        df = conn.sql(COTAHIST_QUERY, params={"source": url}).df()

    context.log.debug("Loaded %d rows", len(df))
    context.log.debug("Columns: %s", df.columns)
    context.log.debug("Dtypes: %s", df.dtypes)
    context.add_output_metadata(output_metadata(df))

    return df
