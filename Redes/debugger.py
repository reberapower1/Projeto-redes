import paho.mqtt.client as mqtt
from datetime import datetime
import json

# Configurações do Broker MQTT
MQTT_BROKER = "10.6.1.9"
MQTT_PORT = 1883
GROUP_ID = "22"  

# Tópicos para subscrever 
TOPICS = [
    f"{GROUP_ID}/#",       
    f"v3/{GROUP_ID}@ttn/#", 
    f"{GROUP_ID}/data_to_machine_manager"
    f"{GROUP_ID}/commands_from_machine_manager"  
]

# Função chamada quando uma mensagem é recebida
def on_message(client, userdata, msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        # Tenta decodificar como JSON (para melhor legibilidade)
        payload = json.loads(msg.payload.decode())
        formatted_msg = json.dumps(payload, indent=2)
    except:
        formatted_msg = msg.payload.decode()
    
    print(f"[{timestamp}]:[{msg.topic}]: {formatted_msg}")

# Configuração do cliente MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_message = on_message

# Conexão e subscrição
client.connect(MQTT_BROKER, MQTT_PORT)
for topic in TOPICS:
    client.subscribe(topic)
    print(f"Subscrito no tópico: {topic}")

print("Debugger MQTT iniciado. Pressione Ctrl+C para parar...")

try:
    client.loop_forever()  # Mantém o debugger rodando
except KeyboardInterrupt:
    print("\nDebugger encerrado.")
    client.disconnect()