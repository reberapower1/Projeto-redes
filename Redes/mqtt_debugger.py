import paho.mqtt.client as mqtt
import time
from datetime import datetime

# Configurações do Broker (usar o IP do Raspberry Pi durante a defesa)
MQTT_BROKER = "10.6.1.9"
MQTT_PORT = 1883
GROUP_ID = "SEU_GROUP_ID"  # Substitua pelo seu GroupID

# Callback quando conecta ao broker
def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao broker MQTT com código {rc}")
    # Assina todos os tópicos do grupo (wildcard '+')
    client.subscribe(f"v3/{GROUP_ID}@ttn/devices/+/up")          # Dados das máquinas
    client.subscribe(f"v3/{GROUP_ID}@ttn/devices/+/down/+")      # Comandos para máquinas
    client.subscribe(f"internal/#")                              # Tópicos internos (opcional)

# Callback quando recebe uma mensagem
def on_message(client, userdata, msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [Tópico: {msg.topic}]: {msg.payload.decode()}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()  # Mantém a conexão ativa
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()