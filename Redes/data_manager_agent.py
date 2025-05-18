import paho.mqtt.client as mqtt
import json
from influxdb_client import InfluxDBClient

# Configurações
GROUP_ID = "SEU_GROUP_ID"
INFLUXDB_URL = "https://us-west-2-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = "seu_token"
INFLUXDB_ORG = "seu_org"
INFLUXDB_BUCKET = "seu_bucket"

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)
    print(f"Dados recebidos: {data}")
    
    # Processar e padronizar dados (ex: converter unidades)
    processed_data = {
        "measurement": "machine_data",
        "tags": {"machine_id": data["end_device_ids"]["machine_id"]},
        "fields": {
            "rpm": data["uplink_message"]["decoded_payload"]["rpm"],
            "temp": data["uplink_message"]["decoded_payload"]["coolant_temperature"],
            "oil_pressure": data["uplink_message"]["decoded_payload"]["oil_pressure"],
        }
    }
    
    # Salvar no InfluxDB
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client_db:
        write_api = client_db.write_api()
        write_api.write(INFLUXDB_BUCKET, INFLUXDB_ORG, processed_data)

def main():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect("10.6.1.9", 1883, 60)
    client.subscribe(f"v3/{GROUP_ID}@ttn/devices/+/up")
    client.loop_forever()

if __name__ == "__main__":
    main()