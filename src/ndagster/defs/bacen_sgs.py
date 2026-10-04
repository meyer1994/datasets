import io
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from xml.etree import ElementTree

import dagster as dg
import duckdb
import httpx2
import polars as pl

# api.bcb.gov.br (the SGS REST API) does not resolve; the SOAP facade serves the
# same series
SGS_URL = "https://www3.bcb.gov.br/wssgs/services/FachadaWSSGS"

SGS_ENVELOPE = """<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:pub="http://publico.ws.casosdeuso.sgs.pec.bcb.gov.br">
  <soapenv:Body>
    <pub:getValoresSeriesXML>
      <in0><item>{code}</item></in0>
      <in1>{start}</in1>
      <in2>{end}</in2>
    </pub:getValoresSeriesXML>
  </soapenv:Body>
</soapenv:Envelope>"""

# DATA is d/m/yyyy (daily), m/yyyy (monthly) or yyyy (yearly)
SGS_QUERY = r"""
SELECT
    $code::INTEGER AS series_code,
    strptime(
        CASE len(string_split(DATA, '/'))
            WHEN 3 THEN DATA
            WHEN 2 THEN '1/' || DATA
            ELSE '1/1/' || DATA
        END,
        '%d/%m/%Y'
    )::DATE AS ref_dt,
    VALOR::DECIMAL(24, 8) AS value,
    BLOQUEADO = 'true' AS blocked
FROM read_xml($source, record_element := 'ITEM', all_varchar := true)
ORDER BY ref_dt
"""


def download_series(code: int, start: date, end: date) -> tuple[str, bytes]:
    """Fetch SGS series values in [start, end] and wrap the XML in a zip."""
    envelope = SGS_ENVELOPE.format(
        code=code, start=start.strftime(r"%d/%m/%Y"), end=end.strftime(r"%d/%m/%Y")
    )

    with httpx2.Client(follow_redirects=True, timeout=60) as client:
        response = client.post(
            SGS_URL,
            content=envelope.encode(),
            headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": '""'},
        )

    # SOAP faults come back as HTTP 500 with an XML body, so parse before
    # checking the status
    try:
        body = ElementTree.fromstring(response.content)
    except ElementTree.ParseError:
        response.raise_for_status()
        raise
    fault = body.find(".//faultstring")
    if fault is not None:
        if "Value(s) not found" in (fault.text or ""):
            raise dg.Failure(
                f"SGS {code} has no values between {start} and {end}: "
                "not published yet",
                allow_retries=False,
            )
        raise dg.Failure(f"SGS {code} fault: {fault.text}")
    response.raise_for_status()

    returned = body.find(".//getValoresSeriesXMLReturn")
    if returned is None or not returned.text:
        raise dg.Failure(f"SGS {code}: unexpected response {response.text[:500]}")

    name = f"sgs_{code}_{start:%Y%m%d}_{end:%Y%m%d}.xml"
    xml = returned.text.replace("encoding='ISO-8859-1'", "encoding='UTF-8'")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, xml.encode())
    return name, buffer.getvalue()


def parse_series(code: int, archive: bytes) -> pl.DataFrame:
    """Parse a zip produced by download_series into series_code/ref_dt/value."""
    with zipfile.ZipFile(io.BytesIO(archive)) as zf:
        (name,) = zf.namelist()
        xml = zf.read(name)

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / name
        path.write_bytes(xml)

        with duckdb.connect() as conn:
            conn.install_extension("webbed", repository="community")
            conn.load_extension("webbed")
            return conn.sql(SGS_QUERY, params={"code": code, "source": str(path)}).pl()
