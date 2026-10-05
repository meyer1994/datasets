# ndagster

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

The whole stack (Postgres, RustFS, Dagster code server / webserver / daemon,
Grafana, Cloudflare tunnel) runs from `docker-compose.yml`, in dev and in prod.
The only difference between environments is `.env`:

```bash
cp .env.example .env # then edit credentials and TUNNEL_TOKEN
docker compose up -d --build
```

Open http://localhost:3000 for Dagster and http://localhost:4000 for Grafana.
Rebuild (`docker compose up -d --build`) after changing code. Data is kept in the
`postgres_data`, `rustfs_data`, `grafana_data` and `dagster_storage` named volumes.
Dagster stores runs, event logs and schedules in Postgres (see `dagster.yaml`).

To expose the Dockerized Dagster UI through a Cloudflare Tunnel, create a tunnel in
the Cloudflare Zero Trust dashboard, add a public hostname pointing to
`http://dagster-webserver:3000` and set `TUNNEL_TOKEN` in `.env`. The
`cloudflared` service starts with the rest of the stack.

The Dagster UI has no authentication, so protect the hostname with a Cloudflare
Access policy.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
