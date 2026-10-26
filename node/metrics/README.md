# snarkos-node-metrics

[![Crates.io](https://img.shields.io/crates/v/snarkos-node-metrics.svg?color=neon)](https://crates.io/crates/snarkos-node-metrics)
[![Authors](https://img.shields.io/badge/authors-Aleo-orange.svg)](https://aleo.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)

The `snarkos-node-metrics` crate provides access to metrics for the `snarkos` node.

## Instructions

#### Quick Start (via `scripts/devnet.sh`)

`scripts/devnet.sh` brings this stack up automatically: it regenerates `prometheus.yml`
to match the validator count and network you choose, runs `docker compose up --detach`
(falling back to `docker-compose` if that's what's installed), and opens
`http://localhost:3000/d/snarkos` once Grafana is ready. The Prometheus datasource and
the snarkOS dashboard are pre-provisioned (`node/metrics/provisioning/`), so no manual
datasource or import steps are needed. Grafana still requires authentication for any
URL, so the first thing you'll see is its login page — log in with `admin`/`admin` (you
can skip the "change password" prompt) and it redirects you straight to the dashboard.
This is a one-time-per-browser-session login, not a bug in the automation.

If Docker isn't installed, `devnet.sh` skips this step with a warning and the devnet
still starts normally; install Docker and re-run.

#### The dashboard

The provisioned dashboard is a port of the `SnarkOS / Incident Response` dashboard the
team runs in Grafana Cloud, so a devnet reproduction shows you the same panels you would
be reading during a real incident. It carries the rows a devnet produces data for
(`Overview`, `Metrics`, `Block Height`, `Block Data`, `Consensus`, `Transmission Data`,
`RocksDB`, `Traffic`); the cloud-only rows are left out, because their metrics come from Google
Cloud Monitoring, GCP Logging, `node_exporter`, or from snarkVM builds carrying VM
instrumentation this repo does not depend on.

The `$network` and `$role` dashboard variables, and most panel queries, select on the
`snarkos_network`, `snarkos_role`, `instance_name` and `public_ip` labels. `devnet.sh`
attaches those to each scrape target so the queries carry over from the cloud dashboard
unchanged. A few panels compare against `offset 1d` or `offset 7d` and stay empty until a
devnet has been running long enough to have that history.

The `Transmission Data` row needs traffic to show anything, so answer `y` to `devnet.sh`'s
"generate transactions" prompt. Its solution panels stay at zero either way: solutions come
from provers, and `devnet.sh` starts only validators and clients.

#### Running the stack standalone

To start up Grafana and Prometheus without `devnet.sh` (e.g. against nodes you started
yourself with `--metrics`):
```bash
cd node/metrics
docker compose up --detach
```

To check that the metrics are running, go to http://localhost:9000.

Then go to [http://localhost:3000/](http://localhost:3000/) — the initial login is
`admin`/`admin`. The Prometheus datasource and snarkOS dashboard are already
provisioned (same as above), so the dashboard should be populated right away. The
committed `prometheus.yml` is whatever `devnet.sh` last generated; edit its
`static_configs` to match the nodes you started, keeping the `instance_name`,
`public_ip`, `snarkos_role` and `snarkos_network` labels on each target, since the
dashboard queries select on them.

#### Importing into a different Grafana

The provisioned dashboard hardcodes the datasource uid `prometheus`, which only resolves
against the datasource this stack provisions. To take the dashboard elsewhere, open it
here and export it with Grafana's "export for use in another instance" option: that
rewrites the datasource into an `__inputs` placeholder, so the target Grafana prompts you
to pick one on import.
