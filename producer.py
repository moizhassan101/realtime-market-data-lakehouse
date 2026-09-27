import json, time, requests, os
from confluent_kafka import Producer

API_KEY = "CG-b7snHnWqxvCSE3cGMMiXS3oJ" # Geko coin API key
COINS = "bitcoin,ethereum,solana,cardano,ripple"
URL = f"https://api.coingecko.com/api/v3/simple/price?ids={COINS}&vs_currencies=usd"
HEADERS = {"x-cg-demo-api-key": API_KEY}
NAMESPACE = "RealTimeMarketDataLakehouse"
EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONNECTION_STRING"]

producer = Producer({
    "bootstrap.servers":f"{NAMESPACE}.servicebus.windows.net:9093",
    "security.protocol":"SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": EVENTHUB_CONN_STR
})

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
            record = {"coin":coin, "usd":price["usd"], "timestamp":ts}
            producer.produce("crypto-prices", key=coin, value=json.dumps(record), callback=delivery_report)
        producer.flush()
    except requests.exceptions.RequestException as e:
        print(f"API call failed, will retry next cycle: {e}")
    time.sleep(30)