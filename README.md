# pipewatch

> A lightweight CLI for monitoring and alerting on ETL pipeline health with pluggable backends.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Define your pipeline checks in a YAML config file:

```yaml
# pipewatch.yaml
pipelines:
  - name: daily_sales_etl
    check: row_count
    threshold: 1000
    backend: slack
    alert_channel: "#data-alerts"
```

Then run the watcher:

```bash
pipewatch run --config pipewatch.yaml
```

Check a single pipeline manually:

```bash
pipewatch check daily_sales_etl --verbose
```

List all configured pipelines and their last status:

```bash
pipewatch status
```

---

## Pluggable Backends

pipewatch supports multiple alerting backends out of the box:

- **Slack** — post alerts to a channel
- **PagerDuty** — trigger on-call incidents
- **Email** — SMTP-based notifications
- **Stdout** — simple console output (default)

Custom backends can be registered via the plugin interface.

---

## License

MIT © 2024 [yourname](https://github.com/yourname)