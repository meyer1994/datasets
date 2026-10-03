-- B3 BVBG.086.01 price report ("pesquisa por pregão") -> flat table.
-- Usage: duckdb pricrpt.duckdb < sql/pricrpt.sql
-- Leaves are read as VARCHAR (all_varchar) and cast explicitly, so decimals
-- never round-trip through DOUBLE.
INSTALL webbed FROM community;
LOAD webbed;

CREATE OR REPLACE TABLE pricrpt AS
SELECT
    regexp_extract(filename, 'BVBG\.086\.01_(BV\d+)\.xml', 1)  AS file_id,
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
        'BVBG.086.01_*.xml',
        record_element := '//PricRpt',
        filename := true,
        all_varchar := true
    )
);
