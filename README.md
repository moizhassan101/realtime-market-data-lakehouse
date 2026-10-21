<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-Structured_Streaming-E25A1C?logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Databricks-Free_Edition-FF3621?logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Delta_Lake-MERGE_Upserts-00ADD8" />
  <img src="https://img.shields.io/badge/Azure-Event_Hubs-0078D4?logo=microsoftazure&logoColor=white" />
  <img src="https://img.shields.io/badge/Pattern-CDC_Upsert-2E8B57" />
</p>

<h1 align="center">Real-Time Market Data Lakehouse</h1>

<p align="center"><b>A near-real-time cryptocurrency pipeline that keeps a live price table in sync via CDC-style upserts — built end-to-end with a Kafka-protocol producer, PySpark Structured Streaming, and Delta Lake MERGE logic.</b></p>

<p align="center">
  <a href="#architecture">Architecture</a> ·
  <a href="#data-model-target-table">Data Model</a> ·
  <a href="#how-to-run-it">How to Run</a>
</p>

---

## Whole Project Video Walkthrough

**Links to Loom Recorded Videos | 5-6 minutes** 
1. https://www.loom.com/share/c4e97852a09e40ca8ac6aba272885683
2. https://www.loom.com/share/5bed0a8507b44de7a193289a50965e7c
3. https://www.loom.com/share/54f03f8f37b84d7bbc70cf6dad01c010

---

---

## Overview

This project ingests live cryptocurrency prices from a public REST API and keeps a single Delta table in sync using a **streaming producer → Kafka-protocol broker → Structured Streaming → Delta `MERGE`** pipeline — a CDC-style upsert pattern, not an append-only event log. Unlike a batch job that reloads a snapshot, this pipeline continuously folds new readings into a table that always reflects the current price per coin.

Everything here is written by hand: a Python producer, a Databricks Structured Streaming notebook, and the merge/dedup logic that keeps the target table correct under replayed and duplicated messages.

**What it demonstrates:**
- Designing and implementing a streaming ingestion-to-lakehouse architecture from scratch
- Writing resilient, idempotent processing logic (`foreachBatch` upserts with delivery confirmation and retry-tolerant polling)
- Diagnosing and fixing real distributed-systems problems (intra-batch duplication from Event Hubs' replay behavior, a shaded Kafka connector namespace, and a serverless compute constraint that rules out always-on streaming)
- Migrating an architecture under a real constraint — swapping a local Kafka broker for a managed cloud broker once Databricks' compute couldn't reach `localhost`

---

## Architecture

The pipeline moves data through four stages, each narrower in scope than the last:

| Stage || Purpose || Pattern |
|---|---|---|---|---|
| **Ingestion** || Poll CoinGecko for five coins' live prices || Python loop, 30s interval, one JSON record per coin |
| **Transport** || Move events off the producer's machine onto a durable, reachable broker || Azure Event Hubs, Kafka protocol, SASL_SSL |
| **Processing** || Parse, deduplicate, and compute the latest value per coin || PySpark Structured Streaming, `foreachBatch` |
| **Storage** || Maintain current state, not history || Delta Lake `MERGE INTO` (CDC-style upsert) |
| **Orchestration** || Re-run the pipeline on a fixed cadence || Databricks Job, `trigger(availableNow=True)`, every 5 min |


---

## Data Model (Target Table)

Unlike a dimensional warehouse, this pipeline's "model" is a single current-state table — one row per coin, always reflecting the latest known price:

| Column | Type | Notes |
|---|---|---|
| `coin` | STRING | Merge key — exactly one row per coin |
| `usd` | DOUBLE | Latest known USD price |
| `timestamp` | LONG | Unix timestamp of that reading |

<details>
<summary><b>Interesting modeling decisions (click to expand)</b></summary>

- **Upsert, not append** — `workspace.default.crypto_prices_delta` is deliberately a current-state table (`MERGE INTO` on `coin`), not a growing event log. That's a different modeling choice than most "streaming" portfolio projects make, and the one that mirrors how CDC feeds are actually synced into production systems.
- **Dedup key is `coin` + `MAX(timestamp)`, not the raw event** — because Event Hubs can replay several readings for the same coin into a single micro-batch, the row kept isn't "the event that arrived," it's "the most recent timestamp per coin," computed with a window function before the merge even runs.
- **No physical history, by design** — once a newer reading lands, the older one is gone; there's no built-in way to chart a price trend from this table alone (see the producer/consumer breakdown below for what a historized version would need).

</details>

---

## What Each Component Actually Does

<details>
<summary><b>Producer — polling &amp; publishing (<code>producer.py</code>)</b></summary>

<br>

Polls CoinGecko every 30 seconds for `bitcoin, ethereum, solana, cardano, ripple` and publishes each price as a JSON record to the `crypto-prices` Kafka topic.

- **Resilient polling** — a transient network failure doesn't crash the loop:
  ```python
  try:
      data = fetch_prices()
      # ... build and produce records ...
  except requests.exceptions.RequestException as e:
      print(f"API call failed, will retry next cycle: {e}")
  ```
- **Delivery confirmation, not blind trust** — `producer.flush()` alone can report success even on a silent failure; a callback confirms each message actually landed:
  ```python
  def delivery_report(err, msg):
      if err is not None:
          print(f"DELIVERY FAILED: {err}")
      else:
          print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")
  ```
- **Kafka-protocol auth against a cloud broker** — SASL_SSL with a connection string standing in for a username/password pair:
  ```python
  producer = Producer({
      "bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
      "security.protocol": "SASL_SSL",
      "sasl.mechanism": "PLAIN",
      "sasl.username": "$ConnectionString",
      "sasl.password": EVENTHUB_CONN_STR
  })
  ```

</details>

<details>
<summary><b>Streaming Consumer &amp; Merge Logic (<code>realtime-crypto-market-datalakehouse.py</code>)</b></summary>

<br>

A Databricks notebook that reads the topic as a stream, parses the JSON payload, and merges each micro-batch into Delta.

- **Shaded Kafka namespace** — Databricks' Spark-Kafka connector relocates its SASL classes, so the JAAS config has to reference the shaded path, not the one most Kafka docs assume:
  ```python
  "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string}";'
  ```
- **Intra-batch deduplication before merge** — Event Hubs replays its full retention window on first read, so a single micro-batch can contain several snapshots of the same coin; `MERGE INTO` alone won't catch that:
  ```python
  window = Window.partitionBy("coin").orderBy(F.col("timestamp").desc())
  deduped_df = (batch_df
      .withColumn("rn", F.row_number().over(window))
      .filter("rn = 1")
      .drop("rn"))
  ```
- **The upsert itself** — one row per coin, updated or inserted depending on whether it already exists:
  ```python
  (target.alias("t")
      .merge(deduped_df.alias("s"), "t.coin = s.coin")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute())
  ```
- **Serverless-compatible scheduling** — no always-on trigger; the job runs to completion, then stops, and a Databricks Job re-triggers it every 5 minutes:
  ```python
  query = (parsed_df.writeStream
      .foreachBatch(upsert_to_delta)
      .option("checkpointLocation", "/Volumes/workspace/default/checkpoints/crypto_prices_merge_v5")
      .trigger(availableNow=True)
      .start())
  ```

</details>

<details>
<summary><b>Local Kafka Dev Setup (<code>docker-compose.yml</code>)</b></summary>

<br>

A single-node, KRaft-mode Kafka broker (no Zookeeper) used to build and test the producer in isolation before any cloud resources existed. It's not what the deployed pipeline runs against — once the consumer moved to Databricks, cloud compute had no route to a broker on `localhost`, which is why Azure Event Hubs' Kafka-compatible endpoint replaced it for the live pipeline. Kept in the repo as the fastest way to iterate on the producer offline.

</details>

---

## Data Quality & Correctness Checks

Confidence in this pipeline came from verifying behavior directly, not from trusting the first tool that looked convenient:

- **Offset verification over a misleading console tool** — the Kafka console consumer displayed nothing despite a running producer; `kafka-get-offsets.sh` showed 600+ real messages in the topic, proving the display (not the pipeline) was at fault.
- **Explicit table refresh before merge** — without it, Delta could operate on a stale cached snapshot of the target table mid-run.
- **Post-merge row count check** — querying `workspace.default.crypto_prices_delta` after each run confirms exactly one row per coin, not a growing count of duplicates.

---

## Repository Structure

```
realtime-market-data-lakehouse/
├── producer.py                              # Polls CoinGecko, publishes to Event Hubs over the Kafka protocol
├── realtime-crypto-market-datalakehouse.py   # Databricks notebook: Structured Streaming consumer + Delta upsert
├── docker-compose.yml                        # Local single-node Kafka (KRaft mode) — used for early producer dev/test
├── project1-build-log.md                     # Full build log: every command run, every bug hit, every fix applied
├── pyproject.toml                            # Python project metadata
└── README.md
```

---

## How to Run It

1. **Install prerequisites** — Python 3.11+, an Azure Event Hubs namespace (Standard tier — the Kafka protocol isn't available on Basic), a Databricks workspace (Free Edition works), and a free CoinGecko Demo API key.
2. **Set credentials**
   ```bash
   export COINGECKO_API_KEY="your_coingecko_key"
   export EVENTHUB_CONNECTION_STRING="your_eventhub_connection_string"
   ```
3. **(Optional) start a local Kafka broker** for offline producer testing
   ```bash
   docker compose up -d
   ```
4. **Run the producer**
   ```bash
   pip install confluent-kafka requests
   python producer.py
   ```
5. **Run the consumer on Databricks** — import `realtime-crypto-market-datalakehouse.py` as a notebook, then store the connection string as a secret:
   ```bash
   databricks secrets put --scope market-lakehouse --key eventhub-conn-str
   ```
6. **Schedule it** — run the notebook once to confirm it completes, then schedule it as a Databricks Job on a 5-minute interval.
7. **Query it**
   ```sql
   SELECT * FROM workspace.default.crypto_prices_delta;
   ```

## Tech Stack

`Python` · `PySpark` · `Databricks` · `Delta Lake` · `Azure Event Hubs` · `Kafka Protocol (confluent-kafka)` · `Docker` · `Git`

---

## Author

**Moiz Hassan** — [GitHub](https://github.com/moizhassan101)
