# Fault-injection testing with kinoko

[kinoko](https://kinoko.sh/docs) runs a workload (the containers in `compose.yaml`) inside a
deterministic hypervisor. It explores many interleavings of client actions and injected
faults: containers killed, frozen, cut off from each other or slowed down. Whenever an
invariant from `model.lua` breaks, it reports the shortest sequence of steps that leads
there. Because the guest is deterministic, every finding can be replayed and inspected
exactly.

This directory is a kinoko workload for Snuffle's Loki ingest path.

## What it is good for (and what it is not)

kinoko finds bugs that only show up when something fails at the wrong moment. Unit and
integration tests rarely reach those paths. Examples for Snuffle:

- **Durability of acks.** Snuffle answers a push with 2xx, but the data never becomes
  readable.
- **Retries and duplicates.** One Loki push writes four separate ClickHouse inserts
  (`log_streams`, `log_stream_labels`, `logs`, `log_stream_stats`). A failure after `logs`
  returns 503, and the client then retries a push that was already partly written.
- **Error mapping under failure.** A backend outage should come back as 5xx, not as a 4xx
  (which tells clients to drop the data) or as a partial 200.
- **Recovery.** Snuffle should serve again once ClickHouse comes back. A `usage` resource
  can also bound memory under load.

It is **not** the right tool for:

- **Query correctness**, such as PromQL/LogQL semantics or histogram maths. Use
  `make test` and `make integration-test`.
- **Performance.** Guest time is virtual and the guest has no network. Use
  `make perf-test` (see [PERFORMANCE.md](../PERFORMANCE.md)).

## What this workload checks

- **Containers:** `clickhouse` (stock image plus the Snuffle log schema and a lean config),
  `snuffle` (built from this checkout) and an idle `client`. kinoko runs its actions and
  readings from `client`.
- **Actions:**
  - `push` behaves like a Promtail-style client. Each line gets a unique id. It retries on
    5xx or no answer, and appends the id to `/tmp/acked` only after a 2xx.
  - `query` runs LogQL `query_range` queries alongside the pushes.
- **Faults:** ClickHouse or Snuffle killed, ClickHouse or Snuffle frozen, the network
  between the two cut, and Snuffle→ClickHouse traffic slowed by 500 ms.
- **Checks:**
  - `every acked push is queryable` (always): every acked id comes back from Snuffle's
    LogQL API. The acked list is read *before* the query, so this holds at every moment,
    not only once things have settled.
  - `no pushed line is returned twice` (always).
  - `snuffle answers queries once faults end` (eventually).
  - `faults never turn into 4xx` and `query answers 200, 5xx or nothing` (reply checks).

`clickhouse-lean.xml` turns off ClickHouse's background system-log tables and shrinks its
caches. kinoko keeps a snapshot of every guest it explores, so memory the guest keeps
changing makes those snapshots large.

## Getting set up (PostHog)

PostHog runs its own kinoko instance on the company tailnet. Ask a teammate for its URL; it
is deliberately not written here. You need Tailscale connected. The public instance at
`https://kinoko.sh` works the same way, with its own account and minutes.

```bash
# Install the CLI from the instance you will use. The instance serves a build that
# matches its server, and the public build can lag behind.
curl -fsSL https://<kinoko-instance>/install.sh | sh     # installs ~/.local/bin/kinoko

# Sign in. This opens the website and stores a key in ~/.config/kinoko.
kinoko login --url https://<kinoko-instance>
```

For CI or agents, create an API key in the website's Settings. Pass it as
`KINOKO_API_KEY`, with `KINOKO_URL` set to the instance.

You also need Docker. kinoko packs the images into `images.tar` (linux/amd64), so on Apple
Silicon the first build cross-compiles.

## Running it

From the repo root:

```bash
docker compose -f kinoko/compose.yaml build   # build snuffle-kinoko and snuffle-kinoko-clickhouse

kinoko check kinoko        # free: static check of compose.yaml + model.lua
kinoko run --local kinoko  # free: runs it on local docker, no faults; does it start, read, answer?

kinoko push kinoko --system snuffle --name loki-ingest   # packs + uploads (~300 MB), prints the workload id
kinoko run snuffle/loki-ingest --minutes 10              # starts a run, prints the run id
kinoko runs                                              # status of recent runs
kinoko show <run>                                        # outcome, findings and report once it ends
```

Rebuild the images before every push. `kinoko push` packs whatever is tagged locally, so a
stale image tests old code. `--keep` reuses an existing `images.tar` and skips the repack.

Practical limits:

- **Cost:** a run spends its `--minutes`, plus about a minute to boot. Runs that end as
  `error` are refunded. Both instances meter minutes per account.
- **Concurrency:** at most 2 runs at once per account.
- **Live progress:** the CLI only shows `running` while a run is going. Watch the run's page
  on the website.

## When a run finds something

`kinoko show <run>` lists each failed check, with the seed and the node of the search tree
where it first broke. The run's page shows the shortest sequence of steps that reproduces
it. To look inside the guest along that path:

```bash
# One-off: run commands at points on the path (node, virtual seconds or "end", command)
cat > probe.txt <<'EOF'
778 22.5 podman exec client cat /tmp/acked
778 22.5 podman logs --tail 50 snuffle
778 end  podman exec clickhouse clickhouse-client -q "SELECT body FROM logs ORDER BY timestamp"
EOF
kinoko probe <run> probe.txt

# Interactive: keep a guest up for that finding and ask it things
kinoko session <run> <node>
kinoko ask <session> 22.5 'podman exec client cat /tmp/acked'
```

Each probe line runs on its own copy of the guest at that moment. Commands can change
whatever they like without affecting the path.

## How to think about it

- **Treat `model.lua` as a spec.** It states what Snuffle promises under failure. Change it
  in the same PR as the behaviour it describes, like any test. A check's name is its id
  across runs, and renaming a check starts a new history.
- **Iterate locally for free.** Use `kinoko check` and `kinoko run --local` while editing
  the model. Spend paid minutes on fault search.
- **Short runs on risky PRs, long runs on `main`.** A 5–10 minute run suits PRs that touch
  ingest, query or ClickHouse client code. A longer nightly run on `main` is where rare
  interleavings turn up. `kinoko push` detects git and GitHub Actions PR metadata, so runs
  can be compared check by check against the base branch. This is not wired into CI yet.
  CI would need an API key secret, Tailscale access to the internal instance, and a poll on
  `kinoko show` until the run ends.
- **Grow coverage one invariant at a time.** Natural next steps are remote write (needs a
  client that can send snappy protobuf), PromQL queries against written samples, and a
  `usage` bound on Snuffle's memory.

## Known rough edges

- **Harness memory.** The first 20-minute run ended with exit 137: memory on the kinoko
  host grew to about 375 GB over roughly 130 rounds. `clickhouse-lean.xml` is the mitigation
  on the workload side. If runs still error the same way, the problem is in the harness.
- **CLI drift.** The CLI is unversioned, and `install.sh` always fetches the latest build.
  If `kinoko check` suddenly rejects a model that used to pass, suspect a CLI change first.
- **Snuffle's Loki `query_range` requires `start`.** Real Loki defaults to the last hour.
  That is why the actions here use `bash` + `curl` with explicit timestamps instead of the
  `http` mechanism (the model has no clock).
