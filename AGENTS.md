# AGENTS.md

Dagster project that downloads and parses B3 (Brasil / Bovespa) market-data
files — daily bulletins from "pesquisa por pregão"
(`www.b3.com.br/pesquisapregao`) and the historical COTAHIST series
(`bvmf.bmfbovespa.com.br`) — plus BACEN (Banco Central do Brasil) SGS time
series. Raw zips are stored in S3-compatible RustFS; parsed results as Parquet;
dashboards live in Grafana (DuckDB datasource).

## Commands

```bash
uv sync                                    # install (dagster==1.13.25, duckdb, polars, httpx2, s3fs)
source .venv/bin/activate                  # activate env first
docker compose up -d --build              # full stack: dagster:3000, grafana:4000
uv run ruff check .                        # lint (dev group; ruff>=0.16.10)
uv run ruff format .                       # format
```

- Dev and prod run the same `docker-compose.yml`; only `.env` differs (template:
 `.env.example`). Every service loads it via `env_file`; hostnames in it are the
 compose service names (`postgres`, `rustfs`). Only `dagster-webserver` (3000)
 and `grafana` (4000 → 3000) publish ports; postgres/rustfs are internal. Prod
 access goes through `cloudflared`.
- Dagster runs as `dagster-code` (gRPC code server, executes runs),
 `dagster-webserver` and `dagster-daemon`, all from `Dockerfile.dagster`.
 Instance storage (runs, event logs, schedules) is Postgres via
 `dagster-postgres`, configured in `dagster.yaml` from `DAGSTER_PG_*`.
- Compose data persists in named volumes `postgres_data`, `rustfs_data`,
 `grafana_data`, `dagster_storage` (`docker compose down -v` wipes them).
- Grafana runs the MotherDuck DuckDB datasource plugin; provisioning creates the
  `rustfs` S3 secret (env `AWS_*` from `.env`; datasource uid is still
  `duckdb-minio`) and auto-imports
  `grafana/dashboards/*.json` into the `B3` folder.
- Reference files: `README.md` (getting started), `DATASETS.md` (checklist of
  datasets per source, B3 and BACEN).

## Asset list (`src/ndagster/defs/assets_b3_*.py`)

Each asset module uses `key_prefix`/`group_name` identical to its module name,
assets named `<freq>_download` (bytes → `io_manager_raw`) and `<freq>_parse`
(Polars DataFrame → `io_manager_parquet`). Downloads retry 3× with 60s delay;
partitions are time-windowed, timezone `America/Sao_Paulo`, weekdays `0 0 * *
1-5` (monthly: 1st, yearly: Jan 1), start 2025-01-01.

| Module / prefix                        | B3 file                                           | URL param     | Frequency                |
| -------------------------------------- | ------------------------------------------------- | ------------- | ------------------------ |
| `b3_bulletin_index_report`             | IndexReport BVBG.087.01                           | `IR`          | daily                    |
| `b3_bulletin_price_report`             | PriceReport BVBG.086.01                           | `PR`          | daily                    |
| `b3_bulletin_price_report_derivatives` | Simplified Price Report – Derivatives BVBG.187.01 | `SPRD`        | daily                    |
| `b3_bulletin_price_report_equities`    | Simplified Price Report – Equities BVBG.186.01    | `SPRE`        | daily                    |
| `b3_daily_liquidity_limits`            | Daily Liquidity Limit (`LLD.txt`)                 | `LD`          | daily                    |
| `b3_daily_liquidity_limits_other`      | Daily Liquidity Limit – Other (`LLDAdicional.txt`) | `LA`         | daily                    |
| `b3_fee_daily_unit_cost`               | Fee Daily Unit Cost BVBG.044.01                   | `DI`          | daily                    |
| `b3_fee_unit_cost`                     | Fee Unit Cost BVBG.043.01                         | `UN`          | monthly                  |
| `b3_fee_variables`                     | Fee Variables BVBG.024.01                         | `VA`          | monthly                  |
| `b3_history`                           | COTAHIST_D / _M / _A (equities history)           | — (fixed URL) | daily + monthly + yearly |
| `b3_indicator_instruments`             | Indicator Instruments BVBG.029.02                 | `II`          | daily                    |
| `b3_instruments`                       | Instruments BVBG.028.02                           | `IN`          | daily                    |
| `b3_primitive_risk_factors`            | Primitive Risk Factors (`FatoresPrimitivosRisco.txt`) | `FP`      | daily                    |
| `b3_risk_formulas`                     | Risk Formulas (`FormulasRisco.txt`)               | `FR`          | daily                    |
| `b3_standardized_instrument_groups`    | Standardized Instrument Groups                    | `AI`          | daily                    |

Notes:
- `b3_history` mutes 3 pairs (daily/monthly/yearly); parses fixed-width TXT
  inside the zip via DuckDB `zipfs` + `httpfs` extensions and a regexp extract
  query (`COTAHIST_QUERY`).
- XML bulletins are parsed with the DuckDB community `webbed` extension
  (`read_xml`, `all_varchar`, explicit `DECIMAL` casts — never DOUBLE).
- `b3_bulletin_price_report` has a reusable `latest_report(archive, prefix)`
  helper (one BVBG file per zip).
- `b3_instruments` `split_report()` splits each XML into standalone files at
  `BizGrp` boundaries before parsing.
- Text reports (`LD`, `LA`, `FP`, `FR`) are `;`-delimited with a `dd/mm/yyyy`
  header line; parsed with DuckDB `read_csv(auto_detect := false)`, `trad_dt`
  taken from the header. Monthly fee files (`UN`, `VA`) are published on the
  first business day of the following month.
- Assets emit rich output metadata (row counts, avg price, markdown previews /
  top-N tables) to the Dagster UI.

## BACEN asset list (`src/ndagster/defs/assets_bacen_*.py`)

Same conventions as the B3 modules, but partitions start at each series' first
SGS observation (Selic 1986-06, vehicle loans 2000-06, household debt 2005-01,
credit cards 2010, others 2011-03). All use the
shared helper `bacen_sgs.py`: `download_series(code, start, end)` calls the SGS
SOAP facade (`www3.bcb.gov.br/wssgs/services/FachadaWSSGS`,
`getValoresSeriesXML`; `api.bcb.gov.br` does not resolve) and wraps the XML in a
zip; `parse_series(code, archive)` reads it with `webbed` into `series_code`,
`ref_dt` (1st of month / Jan 1 for monthly / yearly), `value DECIMAL(24,8)`,
`blocked`. Empty ranges raise a non-retried `dg.Failure` ("not published yet");
monthly series lag 1–3 months.

| Module / prefix               | SGS series                                               | Code  | Frequency          |
| ----------------------------- | -------------------------------------------------------- | ----- | ------------------ |
| `bacen_selic`                 | Taxa de juros – Selic (% a.d.)                           | 11    | daily              |
| `bacen_selic_monthly`         | Selic acumulada no mês (% a.m.)                          | 4390  | monthly            |
| `bacen_household_debt`        | Endividamento das famílias ex. habitacional / renda (%)  | 29038 | monthly            |
| `bacen_credit_cards`          | Cartões de crédito ativos (millions)                     | 25149 | yearly, 2010–2022  |
| `bacen_vehicle_loan_rate`     | Juros recursos livres PF – aquisição de veículos (% a.m.) | 25471 | monthly           |
| `bacen_personal_loan_rate`    | Juros recursos livres PF – pessoal não consignado (% a.m.) | 25464 | monthly          |
| `bacen_household_credit_rate` | Taxa média de juros PF – total (% a.a.)                  | 20716 | monthly            |
| `bacen_credit_default`        | Inadimplência da carteira de crédito – total (%)         | 21082 | monthly            |

## Resources (`src/ndagster/defs/resources.py`)

- `io_manager_parquet` — `PolarsParquetIOManager(base_dir="s3://uploads")`
  (S3/RustFS via `s3fs`).
- `io_manager_raw` — `RawFileIOManager` (custom `dg.UPathIOManager` storing raw
  `.zip` bytes at `s3://uploads/raw`).

## Project layout

```
.
├── AGENTS.md
├── Dockerfile.grafana            # Grafana + MotherDuck DuckDB datasource plugin (Ubuntu image)
├── README.md
├── DATASETS.md                  # dataset coverage checklist (B3, BACEN)
├── Dockerfile.dagster           # Dagster image (code server, webserver, daemon)
├── docker-compose.yml           # postgres, rustfs, dagster-*, cloudflared, grafana
├── docker/dagster/workspace.yaml # gRPC workspace → dagster-code:4000
├── .env                         # all compose config (gitignored; see .env.example)
├── dagster.yaml                 # Dagster instance config (Postgres storage)
├── pyproject.toml               # deps + [tool.dg] config (root_module=ndagster)
├── grafana/
│   ├── dashboards/              # 23 JSON dashboards (one per b3_* / bacen_* group)
│   └── provisioning/
│       ├── datasources/duckdb.yaml   # DuckDB datasource, rustfs secret setup
│       └── dashboards/dashboards.yaml # auto-import dashboards into B3 folder
├── src/ndagster/
│   ├── definitions.py           # @definitions entrypoint → load_from_defs_folder
│   └── defs/
│       ├── resources.py         # IO managers (parquet + raw zip)
│       ├── bacen_sgs.py         # SGS SOAP download + parse helper
│       ├── assets_b3_*.py       # 15 B3 asset modules (see table above)
│       └── assets_bacen_*.py    # 8 BACEN SGS asset modules
└── tests/                       # empty (__init__.py only)
```

Runtime/generated (gitignored): `.venv/`, `.dg/`, `.ruff_cache/`,
`.tmp_dagster_home_*/`, `BVBG.086.01_*.xml`, `PR*.zip`, `pesquisa-pregao.zip`,
`*.duckdb`, `data/`. A one-off script `sql/pricrpt.sql` was removed from the
repo — parse SQL now lives inline in the asset modules.