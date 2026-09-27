# Project 1 — Real-Time Market Data Lakehouse: Build Log

This documents every command run and why, in order. It's a learning/reference log — not the final GitHub README (that comes later, once the project is finished, and will be cleaner/results-focused).

---

## Environment

- **Where code lives:** VS Code, local project folder (`Project 2 - Real-Time Market Data Lakehouse` — folder name doesn't need to match project number, just keep it consistent)
- **Where commands run:** VS Code's built-in terminal (PowerShell), inside that folder
- **Docker Desktop:** runs in the background as an app — must be open and fully started before any `docker` command works. The CLI (`docker`) talks to Docker Desktop's engine; if the app isn't running, every command fails with a connection error.
- **venv:** Python virtual environment, activated for running `.py` scripts (`pip install`, `python producer.py`). Has no relationship to Docker — Docker commands work the same whether venv is active or not.

---

## Step 1: Get a CoinGecko API key

**Purpose:** authenticate requests to CoinGecko's live crypto price API.

1. Signed up at `coingecko.com/en/api/pricing`, chose the free **Demo** plan, got an API key.
2. Tested it directly:
   ```
   curl -H "x-cg-demo-api-key: YOUR_KEY" "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
   ```
   → returned live JSON prices, confirming the key works before writing any pipeline code.

---

## Step 2: Set up Kafka locally via Docker

**Purpose:** Kafka is the streaming backbone — the producer writes price events here, and (later) Databricks reads them from here.

`docker-compose.yml` (single-node Kafka, KRaft mode — no Zookeeper needed):
```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

Start it:
```
docker compose up -d
```
**Issue hit:** `failed to connect to the docker API` — Docker Desktop app wasn't actually running (only the CLI is separate from the engine). **Fix:** opened Docker Desktop app, waited for it to fully start, reran the command.

Create the topic and confirm it exists:
```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic crypto-prices --bootstrap-server localhost:9092
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Step 3: Producer script (pulls prices, pushes to Kafka)

**Purpose:** the actual ingestion — a Python script that polls CoinGecko every 30 seconds and writes each price as a JSON message into the `crypto-prices` Kafka topic.

Install dependencies:
```
pip install confluent-kafka requests
```
**Issue hit:** `ImportError: DLL load failed while importing cimpl` — a corrupted wheel install on Windows. **Fix:**
```
pip uninstall confluent-kafka -y
pip cache purge
pip install confluent-kafka --no-cache-dir
```

**Final, working `producer.py`** (after fixing three bugs — see below):
```python
import json, os, time, requests
from confluent_kafka import Producer

API_KEY = os.environ["COINGECKO_API_KEY"]
COINS = "bitcoin,ethereum,solana,cardano,ripple"
URL = f"https://api.coingecko.com/api/v3/simple/price?ids={COINS}&vs_currencies=usd"
HEADERS = {"x-cg-demo-api-key": API_KEY}

producer = Producer({"bootstrap.servers": "localhost:9092"})

def fetch_prices():
    r = requests.get(URL, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def delivery_report(err, msg):
    if err is not None:
        print(f"DELIVERY FAILED: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

while True:
    try:
        data = fetch_prices()
        ts = int(time.time())
        for coin, price in data.items():
            record = {"coin": coin, "usd": price["usd"], "timestamp": ts}
            producer.produce("crypto-prices", key=coin, value=json.dumps(record), callback=delivery_report)
        producer.flush()
    except requests.exceptions.RequestException as e:
        print(f"API call failed, will retry next cycle: {e}")
    time.sleep(30)
```

**Bugs found and fixed along the way:**
| Bug | Problem | Fix |
|---|---|---|
| `COINS = "bitcoin, ethereum, ..."` | Spaces after commas broke the API's id list — only `bitcoin` was recognized | Removed spaces: `"bitcoin,ethereum,..."` |
| `HEADERS = {"c-cg-demo-api-key": ...}` | Wrong header name (`c-cg-` instead of `x-cg-`) — key wasn't actually being sent | Corrected to `"x-cg-demo-api-key"` |
| Hardcoded API key in the file | Security risk — GitHub scans public repos for exposed keys | Moved to environment variable: `os.environ["COINGECKO_API_KEY"]` |
| No error handling around the API call | One network hiccup crashed the whole producer | Wrapped the loop body in `try/except requests.exceptions.RequestException` |
| `producer.flush()` printed "Pushed" even on silent failure | No real delivery confirmation | Added a `delivery_report` callback to `producer.produce()` |

Set the API key before running (per terminal session):
```
$env:COINGECKO_API_KEY="YOUR_KEY"
```
Run:
```
python producer.py
```

---

## Step 4: Verify data is actually landing in Kafka

**Purpose:** confirm messages are really being written before building anything downstream that depends on it.

Tried the built-in console consumer (a live "tail" of the topic — purpose-built for exactly this):
```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic crypto-prices --from-beginning --bootstrap-server localhost:9092
```
**Issue hit:** screen stayed blank despite the producer clearly running. Turned out to be a display/rendering issue with this specific command in this terminal, not a real data problem.

**Definitive proof instead** — checked the topic's actual message count (offset):
```
docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic crypto-prices
```
→ returned `crypto-prices:0:606` — 606 real messages confirmed in the topic. This is what actually proved the pipeline works, when the console consumer's display couldn't be trusted.

---

## Status: Step 4 complete. Kafka ingestion confirmed working.

**Next:** Databricks Community Edition — sign up, create a free cluster, then write the PySpark Structured Streaming consumer that reads from this Kafka topic.
