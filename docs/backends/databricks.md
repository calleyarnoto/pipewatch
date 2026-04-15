# Databricks Backend

The `databricks` backend checks the health of a Databricks job by inspecting
the most recent run via the [Databricks Jobs API 2.1](https://docs.databricks.com/api/workspace/jobs).

## Configuration

```yaml
backend:
  type: databricks
  host: "https://<workspace>.azuredatabricks.net"
  token: "dapi-xxxxxxxxxxxxxxxx"

pipelines:
  - name: nightly_etl
    params:
      job_id: "123456"
  - name: hourly_sync
    params:
      job_id: "789012"
```

### Backend parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `host`    | Yes      | Base URL of the Databricks workspace |
| `token`   | Yes      | Personal access token or service principal token |

### Pipeline params

| Parameter | Required | Description |
|-----------|----------|-------------|
| `job_id`  | Yes      | Numeric Databricks job ID to monitor |

## Status mapping

| Databricks state | pipewatch status |
|------------------|------------------|
| `result_state: SUCCESS` | `HEALTHY` |
| `result_state: FAILED` / `TIMEDOUT` / etc. | `FAILED` |
| `life_cycle_state: RUNNING / PENDING / BLOCKED` | `UNKNOWN` |
| No runs found | `UNKNOWN` |
| API error | `UNKNOWN` |

## Required packages

```
requests
```

No additional Databricks SDK is required — the backend communicates directly
with the REST API using `requests`.
