import time
import requests
from kafka import KafkaProducer
import json

URL = 'https://api.weather.gov/alerts/active'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_alerts():
    response = requests.get(URL, timeout=10)
    data = response.json()
    return data.get('features', [])

while True:
    print("Fetching NOAA alerts...")
    features = fetch_alerts()

    for f in features:
        alert = f.get('properties', {})
        producer.send('weather_alerts', alert)

        producer.flush()
        print(f'Pushed {len(features)} alerts.')
        time.sleep(10)