import paho.mqtt.client as mqtt
import json
from influxdb_client import InfluxDBClient
import os

# ==========================
# CONFIGURAÇÕES
# ==========================

GROUP_ID = "7"  # Altere para o seu ID de grupo TTN

# MQTT Broker (localhost ou IP do broker)
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

# Tópico de entrada: dados das máquinas (uplink)
TOPIC_UP = f"v3/{GROUP_ID}@ttn/devices/+/up"

# Tópico de saída: alertas para o Data Manager Agent
TOPIC_ALERT = f"v3/{GROUP_ID}@ttn/devices/data_manager_agent/alert"

# Configurações do InfluxDB
INFLUXDB_URL = "https://eu-central-1-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = "onboarding-pythonWizard-token-1743503707747"
INFLUXDB_ORG = "uc2023218119@student.uc.pt"
INFLUXDB_BUCKET = "machines"

# ==========================
# LÊ INTERVALOS DO FICHEIRO DE CONFIGURAÇÃO
# ==========================

# Esperado: cada linha = low high ideal
interval_labels = ["rpm", "coolant_temperature", "oil_pressure", "battery_potential", "consumption"]
intervals = {}

with open("intervals.cfg", "r") as f:
    lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    for i, label in enumerate(interval_labels):
        parts = lines[i].split()
        intervals[label] = {
            "low": float(parts[0]),
            "high": float(parts[1]),
            "ideal": float(parts[2])
        }

# ==========================
# FUNÇÃO DE VERIFICAÇÃO DOS SENSORES
# ==========================

def verificar_alertas(sensor_data):
    """
    Verifica cada valor dos sensores em relação aos limites definidos.
    Retorna uma lista de alertas, se houver.
    """
    alertas = []
    for sensor, value in sensor_data.items():
        if sensor not in intervals:
            continue  # Ignora sensores não definidos
        limites = intervals[sensor]
        if value < limites["low"]:
            alertas.append(f"{sensor} abaixo do limite ({value} < {limites['low']})")
        elif value > limites["high"]:
            alertas.append(f"{sensor} acima do limite ({value} > {limites['high']})")
    return alertas

# ==========================
# CALLBACK DE MENSAGENS MQTT
# ==========================

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload)
    print(f"Dados recebidos: {payload}")

    machine_id = payload["end_device_ids"]["machine_id"]
    data = payload["uplink_message"]["decoded_payload"]

    # Verifica valores anómalos
    alertas = verificar_alertas(data)

    # Publica alerta, se necessário
    if alertas:
        alerta_payload = {
            "machine_id": machine_id,
            "alertas": alertas
        }
        client.publish(TOPIC_ALERT, json.dumps(alerta_payload))
        print(f"🔴 Alerta publicado: {alerta_payload}")
    else:
        print("✅ Todos os valores dentro dos limites.")

    # Grava no InfluxDB
    influx_data = {
        "measurement": "machine_data",
        "tags": {"machine_id": machine_id},
        "fields": data
    }
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as db_client:
        write_api = db_client.write_api()
        write_api.write(INFLUXDB_BUCKET, INFLUXDB_ORG, influx_data)

# ==========================
# FUNÇÃO PRINCIPAL
# ==========================

def main():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(TOPIC_UP)
    print(f"Subscrito no tópico: {TOPIC_UP}")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n Execução interrompida")
        client.disconnect()
        print("Ligação por MQTT encerrada")
    except Exception as e:
        print(f"Erro inesperado: {e}")
        client.disconnect()

if __name__ == "__main__":
    main()
