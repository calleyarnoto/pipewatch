# pipewatch

A lightweight CLI for monitoring and alerting on ETL pipeline health with pluggable backends.

## Backends

| Name | Description |
|---|---|
| `dummy` | Always returns a configurable static status (useful for testing) |
| `airflow` | Checks the latest DAG-run status via the Airflow REST API |
| `prometheus` | Evaluates a PromQL query against a Prometheus instance |
| `postgres` | Runs a SQL query against PostgreSQL and checks the result |
| `mysql` | Runs a SQL query against MySQL and checks the result |
| `bigquery` | Runs a SQL query against BigQuery and checks the result |
| `mongodb` | Runs an aggregation pipeline against MongoDB and checks the result |
| `redis` | Reads a key from Redis and checks its value |
| `elasticsearch` | Runs a query against Elasticsearch and checks the hit count |
| `http` | Checks an HTTP endpoint for a healthy status code or JSON value |
| `kafka` | Checks consumer-group lag against a Kafka topic |
| `s3` | Counts objects in an S3 bucket/prefix |
| `dynamodb` | Scans/queries a DynamoDB table and checks the item count |
| `databricks` | Checks the latest Databricks job-run status |
| `sftp` | Counts files on an SFTP server at a given path |
| `snowflake` | Runs a SQL query against Snowflake and checks the result |
| `gcs` | Counts objects in a GCS bucket/prefix |

## Alert Channels

| Name | Description |
|---|---|
| `log` | Writes alerts to the Python logging system |
| `slack` | Posts alerts to a Slack channel via Incoming Webhooks |
| `email` | Sends alert emails via SMTP |
| `pagerduty` | Creates/resolves PagerDuty incidents |
| `webhook` | POSTs a JSON payload to an arbitrary HTTP endpoint |
| `opsgenie` | Creates OpsGenie alerts |
| `victorops` | Sends VictorOps (Splunk On-Call) notifications |
| `teams` | Posts adaptive-card messages to Microsoft Teams |
| `discord` | Posts embed messages to a Discord channel via webhook |
| `sms` | Sends SMS alerts via Twilio |

## Quick Start

```bash
pip install pipewatch
pipewatch run --config pipewatch.yaml
```

## Configuration

```yaml
backend: gcs
backend_config:
  project: my-gcp-project
  credentials_path: /path/to/sa.json   # optional; uses ADC if omitted

alert_channels:
  - type: slack
    webhook_url: https://hooks.slack.com/services/...

pipelines:
  - name: daily_export
    extra:
      bucket: my-data-bucket
      prefix: exports/2024/
      threshold: 1
```
