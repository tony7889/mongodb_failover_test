# MongoDB Replica-Set Failover Test

A Python client-side failover test application with:

- A CLI synthetic workload runner.
- A FastAPI service for starting and polling tests.
- PyMongo SDAM topology, server, and heartbeat event capture.
- Optional controlled primary step-down through `mongosh`.
- Majority-acknowledged idempotent upserts followed by reads.

## Safety

The test writes documents to the configured database and collection. Use a non-production test replica set. The `--step-down` option intentionally forces an election through `mongosh`; do not enable it in production without an approved change window.

The MongoDB URI is passed to the `mongosh` subprocess when step-down is enabled. Use a protected execution environment because process arguments may be visible to local process inspection tools.

## Requirements

- Python 3.10+
- A MongoDB replica set with at least one electable secondary
- `mongosh` on the PATH if using `--step-down`
- A URI that includes all replica-set members or the replica-set SRV connection format

Install:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Set the URI:

```bash
export MONGODB_URI='mongodb://user:password@host1:27017,host2:27017,host3:27017/?replicaSet=rs0&authSource=admin'
```

## CLI test

Client-only observation, with failover triggered separately by an administrator:

```bash
python -m failover_test.cli run \
  --duration 180 \
  --interval 0.5
```

Trigger the election from the test process after 10 seconds:

```bash
python -m failover_test.cli run \
  --duration 180 \
  --interval 0.5 \
  --step-down \
  --step-down-delay 10 \
  --step-down-seconds 60
```

The process prints JSON containing operation counts, errors, recovery time, and captured topology events. Exit code `0` means the run completed with successful operations and no observed operation failures; exit code `1` means the run observed failures or could not complete.

## FastAPI service

Start the service:

```bash
python -m failover_test.cli serve --host 0.0.0.0 --port 8080
```

Start a run without triggering failover:

```bash
curl -X POST http://localhost:8080/runs \
  -H 'content-type: application/json' \
  -d '{"duration_seconds":180,"interval_seconds":0.5,"step_down":false}'
```

Start a controlled step-down test:

```bash
curl -X POST http://localhost:8080/runs \
  -H 'content-type: application/json' \
  -d '{"duration_seconds":180,"interval_seconds":0.5,"step_down":true,"step_down_delay_seconds":10,"step_down_seconds":60}'
```

Poll the returned run ID:

```bash
curl http://localhost:8080/runs/<run_id>
```

## Suggested acceptance criteria

1. A new primary is elected within the agreed recovery objective.
2. The client reconnects without application restart.
3. Operations resume after the election.
4. The application records topology changes and any transient errors.
5. No duplicate business effect occurs. The sample uses a unique `_id` per probe and majority write concern; production workloads should also use application-level idempotency where appropriate.

## Project layout

```text
failover_test/
  api.py       FastAPI endpoints
  cli.py       CLI entry point
  config.py    Environment and runtime configuration
  runner.py    Workload, failover trigger, and event collection
```
