import paho.mqtt.client as mqtt
import json
import random
import time
import sys
import base64

# ============================
# Leitura dos Parâmetros de Entrada
# ============================
# Argumentos: GroupID, Tempo de Atualização, Código da Máquina
GROUP_ID = sys.argv[1]
UPDATE_TIME = int(sys.argv[2])
MACHINE_CODE = sys.argv[3]

# Associação entre códigos e IDs fixos
CODE_TO_ID = {
    "A23X": "M1",
    "B47Y": "M2",
    "C89Z": "M3",
    "D56W": "M4",
    "E34V": "M5",
    "F78T": "M6",
    "G92Q": "M7",
    "H65P": "M8",
}
MACHINE_ID = CODE_TO_ID.get(MACHINE_CODE)

# ============================
# Definições do MQTT (ATUALIZADAS)
# ============================
MQTT_BROKER = "localhost"  # Ou "127.0.0.1"
MQTT_PORT = 1883           # Porta padrão MQTT

# Tópicos MQTT definidos pelo TTN
TOPIC_UP = f"v3/{GROUP_ID}@ttn/devices/{MACHINE_ID}/up"
TOPIC_DOWN_ACTUATOR = f"v3/{GROUP_ID}@ttn/devices/{MACHINE_ID}/down/push_actuator"
TOPIC_DOWN_ALERT = f"v3/{GROUP_ID}@ttn/devices/{MACHINE_ID}/down/push_alert"

# ============================
# Variáveis globais para RSSI/SNR
# ============================
rssi = -75  # Valor inicial entre -120 e -50
snr = 9.2   # Valor inicial entre -20 e 10

# ============================
# Inicialização de valores de sensores
# ============================
rpm = 1100
coolant_temp = 90.0
oil_pressure = 3.0
battery_potential = 12.6
consumption = 15.0

shutdown = False  # Define se a máquina está desligada

# ============================
# Funções MQTT (ATUALIZADAS)
# ============================
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Conectado ao broker MQTT com código {rc}")
    client.subscribe(TOPIC_DOWN_ACTUATOR)
    client.subscribe(TOPIC_DOWN_ALERT)

def on_message(client, userdata, msg):
    global shutdown, rpm, consumption, oil_pressure, coolant_temp

    print(f"Mensagem recebida no tópico {msg.topic}: {msg.payload.decode()}")
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command", "")

        if msg.topic.endswith("push_alert"):
            if command == "shutdown":
                print("\u26a0\ufe0f Alerta CRÍTICO recebido: a máquina vai se desligar.")
                shutdown = True

        elif msg.topic.endswith("push_actuator"):
            if command == "reduce_load":
                print("\ud83d\udcc9 A carga da máquina vai diminuir...")
                rpm = max(800, rpm - 200)
                if oil_pressure > 1.5:
                    oil_pressure -= 0.3
                if coolant_temp > 70:
                    coolant_temp -= 0.5
                if consumption > 1:
                    consumption -= 1
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

# ============================
# Dados simulados da máquina (ATUALIZADO)
# ============================
def generate_machine_data():
    global rpm, coolant_temp, oil_pressure, battery_potential, consumption, shutdown
    global rssi, snr

    # Se está em estado de shutdown, convergir valores para 0
    if shutdown:
        rpm = max(0, rpm - 300)
        consumption = max(0, consumption - 2)
        oil_pressure = max(0, oil_pressure - 0.5)
        coolant_temp = max(20.0 if 'F' not in MACHINE_CODE else 68.0, coolant_temp - 1.0)

        # Se tudo zerado, reativa máquina
        shutdown_temp_limit = 20.0 if 'F' not in MACHINE_CODE else 68.0
        if rpm == 0 and oil_pressure == 0 and coolant_temp <= shutdown_temp_limit:
            shutdown = False
            print("\ud83d\udd04 Máquina reiniciada")

    else:
        # Variações aleatórias dentro dos limites
        rpm += random.uniform(-50, 200)
        coolant_temp += random.uniform(-0.3, 1.0)
        oil_pressure += random.uniform(-0.1, 0.5)
        battery_potential += random.uniform(-0.1, 0.2)
        consumption += random.uniform(-1, 1)

        # Aplica limites físicos
        rpm = max(800, min(3000, rpm))
        coolant_temp = max(70.0, min(130.0, coolant_temp))
        oil_pressure = max(1.5, min(8.0, oil_pressure))
        battery_potential = max(10.0, min(14.0, battery_potential))
        consumption = max(1.0, min(50.0, consumption))

    # Simulação incremental de RSSI/SNR
    rssi = max(-120, min(-50, rssi + random.uniform(-3, 3)))
    snr = max(-20, min(10, snr + random.uniform(-0.5, 0.5)))
    channel_rssi = rssi + random.uniform(-3, 3)

    # Payload Base64 real
    decoded_payload = {
        "rpm": round(rpm, 1),
        "coolant_temperature": round(coolant_temp, 1),
        "oil_pressure": round(oil_pressure, 2),
        "battery_potential": round(battery_potential, 2),
        "consumption": round(consumption, 2),
        "machine_type": MACHINE_CODE
    }
    frm_payload = base64.b64encode(json.dumps(decoded_payload).encode()).decode()

    # Estrutura do JSON
    data = {
        "end_device_ids": {
            "machine_id": MACHINE_ID,
            "application_id": GROUP_ID,
            "dev_eui": "70B3D57ED00347C5",
            "join_eui": "0000000000000000",
            "dev_addr": "260B1234"
        },
        "received_at": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "uplink_message": {
            "f_port": 1,
            "f_cnt": random.randint(0, 9999),
            "frm_payload": frm_payload,
            "decoded_payload": decoded_payload,
            "rx_metadata": [{
                "gateway_id": "gateway-1",
                "rssi": round(rssi, 2),
                "snr": round(snr, 2),
                "channel_rssi": round(channel_rssi, 2),
                "uplink_token": "TOKEN_VALUE"
            }],
            "settings": {
                "data_rate": {
                    "modulation": "LORA",
                    "bandwidth": 125000,
                    "spreading_factor": 7
                },
                "frequency": "868300000",
                "timestamp": int(time.time())
            },
            "consumed_airtime": "0.061696s"
        }
    }
    return data

# ============================
# Execução principal (ATUALIZADA)
# ============================
def main():
    # Usando a API v2 do Paho MQTT para evitar warnings
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f"Conectando ao broker em {MQTT_BROKER}:{MQTT_PORT}...")
    except Exception as e:
        print(f"Falha na ligação MQTT: {e}")
        print("Verifique se:")
        print("1. O broker Mosquitto está rodando (execute 'mosquitto -v' em outro terminal)")
        print("2. O firewall permite conexões na porta 1883")
        print("3. Você está usando o IP/porta corretos")
        sys.exit(1)
    
    client.loop_start()

    try:
        while True:
            data = generate_machine_data()
            client.publish(TOPIC_UP, json.dumps(data))
            print(f"Dados publicados para {MACHINE_ID}")
            time.sleep(UPDATE_TIME)
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
        print("Desconectado do broker MQTT")

if __name__ == "__main__":
    main()