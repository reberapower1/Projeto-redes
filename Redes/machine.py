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

# Associação entre códigos e IDs 
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
# Definições do MQTT 
# ============================
MQTT_BROKER = "localhost"  
MQTT_PORT = 1883           

# Tópicos MQTT 
TOPIC_UP = f"v3/{GROUP_ID}@ttn/devices/{MACHINE_ID}/up"
TOPIC_DOWN_ACTUATOR = f"v3/{GROUP_ID}@ttn/devices/{MACHINE_ID}/down/push_actuator"
TOPIC_DOWN_ALERT = f"v3/{GROUP_ID}@ttn/devices/{MACHINE_ID}/down/push_alert"

# ============================
# Configuração das unidades por máquina
# ============================
MACHINE_CONFIG = {
    "A23X": {
        "oil_pressure_unit": "psi",
        "coolant_temp_unit": "°C",
        "battery_potential_unit": "V",
        "consumption_unit": "l/h"
    },
    "B47Y": {
        "oil_pressure_unit": "bar",
        "coolant_temp_unit": "°C",
        "battery_potential_unit": "V",
        "consumption_unit": "gal/h"
    },
    "C89Z": {
        "oil_pressure_unit": "psi",
        "coolant_temp_unit": "°C",
        "battery_potential_unit": "V",
        "consumption_unit": "gal/h"
    },
    "D56W": {
        "oil_pressure_unit": "bar",
        "coolant_temp_unit": "°C",
        "battery_potential_unit": "V",
        "consumption_unit": "l/h"
    },
    "E34V": {
        "oil_pressure_unit": "psi",
        "coolant_temp_unit": "°F",
        "battery_potential_unit": "V",
        "consumption_unit": "gal/h"
    },
    "F78T": {
        "oil_pressure_unit": "bar",
        "coolant_temp_unit": "°F",
        "battery_potential_unit": "V",
        "consumption_unit": "l/h"
    },
    "G92Q": {
        "oil_pressure_unit": "psi",
        "coolant_temp_unit": "°F",
        "battery_potential_unit": "V",
        "consumption_unit": "l/h"
    },
    "H65P": {
        "oil_pressure_unit": "bar",
        "coolant_temp_unit": "°F",
        "battery_potential_unit": "mV",
        "consumption_unit": "gal/h"
    }
}

# Fatores de conversão
PSI_TO_BAR = 0.0689476
BAR_TO_PSI = 14.5038
L_TO_GAL = 0.264172
GAL_TO_L = 3.78541
V_TO_MV = 1000
MV_TO_V = 0.001

# ============================
# Variáveis globais para RSSI/SNR
# ============================
rssi = -75  # Valor inicial entre -120 e -50
snr = 9.2   # Valor inicial entre -20 e 10

# ============================
# Inicialização de valores de sensores
# ============================
def init_sensor_values(machine_code):
    config = MACHINE_CONFIG[machine_code]
    
    # Valores base nas respetivas unidades de medidas
    rpm = 1100
    coolant_temp = 90.0 if config["coolant_temp_unit"] == "°C" else 194.0  # 90°C = 194°F
    oil_pressure = 3.0 if config["oil_pressure_unit"] == "bar" else 3.0 * BAR_TO_PSI
    battery_potential = 12.6 if config["battery_potential_unit"] == "V" else 12.6 * V_TO_MV
    consumption = 15.0 if config["consumption_unit"] == "l/h" else 15.0 * L_TO_GAL
    
    return {
        "rpm": rpm,
        "coolant_temp": coolant_temp,
        "oil_pressure": oil_pressure,
        "battery_potential": battery_potential,
        "consumption": consumption
    }


sensor_values = init_sensor_values(MACHINE_CODE)
shutdown = False  # Define se a máquina está desligada

# ============================
# Funções MQTT 
# ============================
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Ligao ao broker MQTT com o código {rc}")
    client.subscribe(TOPIC_DOWN_ACTUATOR)
    client.subscribe(TOPIC_DOWN_ALERT)

def on_message(client, userdata, msg):
    global shutdown, sensor_values

    print(f"Mensagem recebida no tópico {msg.topic}: {msg.payload.decode()}")
    try:
        payload = json.loads(msg.payload.decode())
        command = payload.get("command", "")

        if msg.topic.endswith("push_alert"):
            if command == "shutdown":
                print(" Alerta CRÍTICO recebido: a máquina vai-se desligar.")
                shutdown = True

        elif msg.topic.endswith("push_actuator"):
            if command == "reduce_load":
                print(" A carga da máquina vai diminuir...")
                sensor_values["rpm"] = max(800, sensor_values["rpm"] - 200)
                
                # Reduz pressão do óleo 
                if MACHINE_CONFIG[MACHINE_CODE]["oil_pressure_unit"] == "bar":
                    sensor_values["oil_pressure"] = max(1.5, sensor_values["oil_pressure"] - 0.3)
                else:
                    sensor_values["oil_pressure"] = max(1.5 * BAR_TO_PSI, sensor_values["oil_pressure"] - (0.3 * BAR_TO_PSI))
                
                # Reduz temperatura
                if MACHINE_CONFIG[MACHINE_CODE]["coolant_temp_unit"] == "°C":
                    sensor_values["coolant_temp"] = max(70.0, sensor_values["coolant_temp"] - 0.5)
                else:
                    sensor_values["coolant_temp"] = max(158.0, sensor_values["coolant_temp"] - 0.9)  # 0.5°C ≈ 0.9°F
                
                # Reduz consumo
                if MACHINE_CONFIG[MACHINE_CODE]["consumption_unit"] == "l/h":
                    sensor_values["consumption"] = max(1.0, sensor_values["consumption"] - 1)
                else:
                    sensor_values["consumption"] = max(0.264172, sensor_values["consumption"] - (1 * L_TO_GAL))
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")


# ============================
# Dados simulados da máquina 
# ============================
def generate_machine_data():
    global sensor_values, shutdown
    global rssi, snr

    config = MACHINE_CONFIG[MACHINE_CODE]

    # Se está em estado de shutdown, convergir valores para 0
    if shutdown:
        sensor_values["rpm"] = max(0, sensor_values["rpm"] - 300)
        
        # Reduz consumo
        if config["consumption_unit"] == "l/h":
            sensor_values["consumption"] = max(0, sensor_values["consumption"] - 2)
        else:
            sensor_values["consumption"] = max(0, sensor_values["consumption"] - (2 * L_TO_GAL))
        
        # Reduz pressão do óleo
        if config["oil_pressure_unit"] == "bar":
            sensor_values["oil_pressure"] = max(0, sensor_values["oil_pressure"] - 0.5)
        else:
            sensor_values["oil_pressure"] = max(0, sensor_values["oil_pressure"] - (0.5 * BAR_TO_PSI))
        
        # Reduz temperatura
        if config["coolant_temp_unit"] == "°C":
            sensor_values["coolant_temp"] = max(20.0, sensor_values["coolant_temp"] - 1.0)
        else:
            sensor_values["coolant_temp"] = max(68.0, sensor_values["coolant_temp"] - 1.8) 

        # Se tudo zerado, reativa máquina
        shutdown_temp_limit = 20.0 if config["coolant_temp_unit"] == "°C" else 68.0
        if (sensor_values["rpm"] == 0 and 
            sensor_values["oil_pressure"] == 0 and 
            sensor_values["coolant_temp"] <= shutdown_temp_limit):
            shutdown = False
            print("Máquina reiniciada")
            sensor_values = init_sensor_values(MACHINE_CODE)

    else:
        sensor_values["rpm"] += random.uniform(-50, 200)
        
        # Variação de temperatura
        if config["coolant_temp_unit"] == "°C":
            sensor_values["coolant_temp"] += random.uniform(-0.3, 1.0)
        else:
            sensor_values["coolant_temp"] += random.uniform(-0.54, 1.8)  
        
        # Variação de pressão do óleo
        if config["oil_pressure_unit"] == "bar":
            sensor_values["oil_pressure"] += random.uniform(-0.1, 0.5)
        else:
            sensor_values["oil_pressure"] += random.uniform(-0.1 * BAR_TO_PSI, 0.5 * BAR_TO_PSI)
        
        # Variação de potencial da bateria
        if config["battery_potential_unit"] == "V":
            sensor_values["battery_potential"] += random.uniform(-0.1, 0.2)
        else:
            sensor_values["battery_potential"] += random.uniform(-0.1 * V_TO_MV, 0.2 * V_TO_MV)
        
        # Variação de consumo
        if config["consumption_unit"] == "l/h":
            sensor_values["consumption"] += random.uniform(-1, 1)
        else:
            sensor_values["consumption"] += random.uniform(-1 * L_TO_GAL, 1 * L_TO_GAL)

        # Aplica limites físicos
        sensor_values["rpm"] = max(800, min(3000, sensor_values["rpm"]))
        
        if config["coolant_temp_unit"] == "°C":
            sensor_values["coolant_temp"] = max(70.0, min(130.0, sensor_values["coolant_temp"]))
        else:
            sensor_values["coolant_temp"] = max(158.0, min(266.0, sensor_values["coolant_temp"]))  # 70°C=158°F, 130°C=266°F
        
        if config["oil_pressure_unit"] == "bar":
            sensor_values["oil_pressure"] = max(1.5, min(8.0, sensor_values["oil_pressure"]))
        else:
            sensor_values["oil_pressure"] = max(1.5 * BAR_TO_PSI, min(8.0 * BAR_TO_PSI, sensor_values["oil_pressure"]))
        
        if config["battery_potential_unit"] == "V":
            sensor_values["battery_potential"] = max(10.0, min(14.0, sensor_values["battery_potential"]))
        else:
            sensor_values["battery_potential"] = max(10.0 * V_TO_MV, min(14.0 * V_TO_MV, sensor_values["battery_potential"]))
        
        if config["consumption_unit"] == "l/h":
            sensor_values["consumption"] = max(1.0, min(50.0, sensor_values["consumption"]))
        else:
            sensor_values["consumption"] = max(1.0 * L_TO_GAL, min(50.0 * L_TO_GAL, sensor_values["consumption"]))

    # Simulação incremental de RSSI/SNR
    rssi = max(-120, min(-50, rssi + random.uniform(-3, 3)))
    snr = max(-20, min(10, snr + random.uniform(-0.5, 0.5)))
    channel_rssi = rssi + random.uniform(-3, 3)

    # Payload Base64 
    decoded_payload = {
        "rpm": round(sensor_values["rpm"], 1),
        "coolant_temperature": round(sensor_values["coolant_temp"], 1),
        "oil_pressure": round(sensor_values["oil_pressure"], 2),
        "battery_potential": round(sensor_values["battery_potential"], 2),
        "consumption": round(sensor_values["consumption"], 2),
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

def print_published_data(data):
    config = MACHINE_CONFIG[MACHINE_CODE]
    
    print("\n Valores dos Sensores:")
    sensors = data['uplink_message']['decoded_payload']
    for k, v in sensors.items():
        if k == 'machine_type':
            continue
        
        unit = ""
        if k == "coolant_temperature":
            unit = config["coolant_temp_unit"]
        elif k == "oil_pressure":
            unit = config["oil_pressure_unit"]
        elif k == "battery_potential":
            unit = config["battery_potential_unit"]
        elif k == "consumption":
            unit = config["consumption_unit"]
        elif k == "rpm":
            unit = "rpm"
        
        print(f"  - {k.replace('_', ' ').title():<20}: {v} {unit}")
    
    print("\n Dados de Rede:")
    meta = data['uplink_message']['rx_metadata'][0]
    print(f"  - RSSI: {meta['rssi']} dBm")
    print(f"  - SNR: {meta['snr']} dB")
    print(f"  - Channel RSSI: {meta['channel_rssi']} dBm")
    
    print("\n Metadados LoRaWAN:")
    print(f"  - FPort: {data['uplink_message']['f_port']}")
    print(f"  - FCnt: {data['uplink_message']['f_cnt']}")
    print(f"  - Airtime: {data['uplink_message']['consumed_airtime']}")
    print("="*50 + "\n")

# ============================
# Execução principal 
# ============================
def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f"Conectando ao broker em {MQTT_BROKER}:{MQTT_PORT}...")
    except Exception as e:
        print(f"Falha na ligação MQTT: {e}")
        sys.exit(1)
    
    client.loop_start()

    try:
        while True:
            data = generate_machine_data()
            client.publish(TOPIC_UP, json.dumps(data))
            print_published_data(data)
            print(f"Dados publicados para {MACHINE_ID}")
            time.sleep(UPDATE_TIME)
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()
        print("Desconectado do broker MQTT")

if __name__ == "__main__":
    main()