# AGENTS.md

Dagster project that downloads and parses B3 (Brasil / Bovespa) market-data
files — daily bulletins from "pesquisa por pregão"
(`www.b3.com.br/pesquisapregao`) and the historical COTAHIST series
(`bvmf.bmfbovespa.com.br`). Raw zips are stored in S3-compatible MinIO; parsed
results as Parquet; dashboards live in Grafana (DuckDB datasource).

## Commands

```bash
uv sync                                    # install (dagster==1.13.25, duckdb, polars, httpx2, s3fs)
source .venv/bin/activate                  # activate env first
dg dev                                     # Dagster UI at http://localhost:3000
docker compose up -d                       # postgres:5432, minio:9000/9001, grafana:3001
uv run ruff check .                        # lint (dev group; ruff>=0.16.10)
uv run ruff format .                       # format
```

- Grafana runs the MotherDuck DuckDB datasource plugin; provisioning creates the
  `minio` S3 secret (env `AWS_*` from `.env`) and auto-imports
  `grafana/dashboards/*.json` into the `B3` folder.
- Reference files: `README.md` (getting started), `TODO.md` (checklist of which
  B3 file types remain).

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
| `b3_fee_daily_unit_cost`               | Fee Daily Unit Cost BVBG.044.01                   | `DI`          | daily                    |
| `b3_fee_unit_cost`                     | Fee Unit Cost BVBG.043.01                         | `UN`          | monthly                  |
| `b3_history`                           | COTAHIST_D / _M / _A (equities history)           | — (fixed URL) | daily + monthly + yearly |
| `b3_indicator_instruments`             | Indicator Instruments BVBG.029.02                 | `II`          | daily                    |
| `b3_instruments`                       | Instruments BVBG.028.02                           | `IN`          | daily                    |
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
- Assets emit rich output metadata (row counts, avg price, markdown previews /
  top-N tables) to the Dagster UI.

## Resources (`src/ndagster/defs/resources.py`)

- `io_manager_parquet` — `PolarsParquetIOManager(base_dir="s3://uploads")`
  (S3/MinIO via `s3fs`).
- `io_manager_raw` — `RawFileIOManager` (custom `dg.UPathIOManager` storing raw
  `.zip` bytes at `s3://uploads/raw`).

## Project layout

```
.
├── AGENTS.md
├── Dockerfile.grafana            # Grafana + MotherDuck DuckDB datasource plugin (Ubuntu image)
├── README.md
├── TODO.md                      # B3 file-type coverage checklist
├── docker-compose.yml           # postgres, minio, grafana services
├── .env                         # MinIO + Grafana credentials (gitignored)
├── pyproject.toml               # deps + [tool.dg] config (root_module=ndagster)
├── grafana/
│   ├── dashboards/              # 10 JSON dashboards (one per b3_* group)
│   └── provisioning/
│       ├── datasources/duckdb.yaml   # DuckDB datasource, minio secret setup
│       └── dashboards/dashboards.yaml # auto-import dashboards into B3 folder
├── src/ndagster/
│   ├── definitions.py           # @definitions entrypoint → load_from_defs_folder
│   └── defs/
│       ├── resources.py         # IO managers (parquet + raw zip)
│       └── assets_b3_*.py       # 10 asset modules (see table above)
└── tests/                       # empty (__init__.py only)
```

Runtime/generated (gitignored): `.venv/`, `.dg/`, `.ruff_cache/`,
`.tmp_dagster_home_*/`, `BVBG.086.01_*.xml`, `PR*.zip`, `pesquisa-pregao.zip`,
`*.duckdb`, `data/`. A one-off script `sql/pricrpt.sql` was removed from the
repo — parse SQL now lives inline in the asset modules.