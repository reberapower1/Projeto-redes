import sys
import paho.mqtt.client as mqtt
import json
from datetime import datetime
from influxdb_client_3 import InfluxDBClient3, Point 

GROUP_ID = sys.argv[1]

# ============================
# Definições do MQTT 
# ============================
MQTT_BROKER = "localhost"  
MQTT_PORT = 1883  

# Tópicos MQTT 
TOPIC_MACHINE = f"v3/{GROUP_ID}@ttn/devices/+/up"

# Configurações
GROUP_ID = "7"
token =  "ifB8rGv5s_u6Wc_q4JmZGE8zQMba_8u-UfLLvTKeBMuofI3lrhaSH73m_QHFZhFmceiegWY6BohE0Cw49AaWBg=="
org = "Projetos Redes"                    
host = "https://eu-central-1-1.aws.cloud2.influxdata.com"  
database = "machines"    

# Inicializa o cliente InfluxDB
influx_client = InfluxDBClient3(host=host, token=token, database=database, org=org)

MACHINE_ID_TO_CODE = {
    "M1": "A23X",
    "M2": "B47Y",
    "M3": "C89Z",
    "M4": "D56W",
    "M5": "E34V",
    "M6": "F78T",
    "M7": "G92Q",
    "M8": "H65P"
}

# Unidades da máquina A23X (M1) 
TARGET_UNITS = {
    "rpm": "rpm",
    "oil_pressure": "psi",
    "coolant_temp": "°C",
    "battery_potential": "V",
    "consumption": "l/h"
}

# Fatores de conversão
CONVERSION_FACTORS = {
    "psi_to_bar": 0.0689476,
    "bar_to_psi": 14.5038,
    "l_to_gal": 0.264172,
    "gal_to_l": 3.78541,
    "c_to_f": lambda c: (c * 9/5) + 32,
    "f_to_c": lambda f: (f - 32) * 5/9,
    "mv_to_v": 0.001,
    "v_to_mv": 1000
}

def convert_to_a23x_units(machine_code, sensor_data):
    converted_data = {}
    
    # Identifica as unidades originais da máquina
    source_units = {
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
    }.get(machine_code, {})
    
    # RPM é sempre o mesmo
    converted_data["rpm"] = sensor_data.get("rpm", 0)
    
    # Conversão de pressão do óleo para psi (unidade da A23X)
    oil_pressure = sensor_data.get("oil_pressure", 0)
    if source_units.get("oil_pressure") == "bar":
        converted_data["oil_pressure"] = oil_pressure * CONVERSION_FACTORS["bar_to_psi"]
    else:
        converted_data["oil_pressure"] = oil_pressure
    
    # Conversão de temperatura para °C 
    coolant_temp = sensor_data.get("coolant_temperature", 0)
    if machine_code in ["E34V", "F78T", "G92Q", "H65P"]:  # Máquinas que usam °F
        converted_data["coolant_temp"] = CONVERSION_FACTORS["f_to_c"](coolant_temp)
    else:
        converted_data["coolant_temp"] = coolant_temp
    
    # Conversão de potencial da bateria para V 
    battery_potential = sensor_data.get("battery_potential", 0)
    if machine_code == "H65P":  # Única máquina que usa mV
        converted_data["battery_potential"] = battery_potential * CONVERSION_FACTORS["mv_to_v"]
    else:
        converted_data["battery_potential"] = battery_potential
    
    # Conversão de consumo para l/h 
    consumption = sensor_data.get("consumption", 0)
    if machine_code in ["B47Y", "C89Z", "E34V", "H65P"]:  # Máquinas que usam gal/h
        converted_data["consumption"] = consumption * CONVERSION_FACTORS["gal_to_l"]
    else:
        converted_data["consumption"] = consumption
    
    return converted_data

def send_to_influx(machine_data):
    """Versão otimizada para integrar com seu fluxo atual"""
    try:
        # Cria todos os pontos
        points = []
        
        # Dados dos sensores 
        sensor_point = Point("machine_metrics") \
        .tag("machine_id", machine_data['machine_id']) \
        .tag("machine_code", machine_data['machine_code']) \
        .field("rpm", float(machine_data['rpm'])) \
        .field("oil_pressure_psi", machine_data['oil_pressure']) \
        .field("coolant_temp_c", machine_data['coolant_temp']) \
        .field("battery_potential_v", machine_data['battery_potential']) \
        .field("consumption_lh", machine_data['consumption']) \
        .time(datetime.now())

        
        # Dados da rede
        signal_point = Point("network_metrics") \
            .tag("machine_id", machine_data['machine_id']) \
            .field("rssi", machine_data['rssi']) \
            .field("snr", machine_data['snr']) \
            .field("channel_rssi", machine_data.get('channel_rssi', -100)) \
            .time(datetime.now())
        points.append(signal_point)
        
        influx_client.write(points)
        
        print(f"Dados da máquina {machine_data['machine_id']} enviados para a base de dados")
        return True
        
    except Exception as e:
        print(f"Erro ao enviar dados: {str(e)}")
        return False

# ============================
# Funções MQTT 
# ============================
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Ligao ao broker MQTT com o código {rc}")
    client.subscribe(TOPIC_MACHINE)
    print(f"Subscrito ao tópico: v3/{GROUP_ID}@ttn/devices/+/up")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        machine_id = data["end_device_ids"]["machine_id"]
        machine_code = MACHINE_ID_TO_CODE.get(machine_id)
        
        if not machine_code:
            print(f"Erro: Machine ID {machine_id} não encontrado")
            return
        
        print("recebi dados da máquina", machine_id)

        sensor_data = data["uplink_message"]["decoded_payload"]
        standardized_data = convert_to_a23x_units(machine_code, sensor_data)
        
        influx_payload = {
            'machine_id': machine_id,
            'machine_code': machine_code,
            'rpm': standardized_data["rpm"],
            'oil_pressure': standardized_data["oil_pressure"],
            'coolant_temp': standardized_data["coolant_temp"],
            'battery_potential': standardized_data["battery_potential"],
            'consumption': standardized_data["consumption"],
            'rssi': data["uplink_message"]["rx_metadata"][0]["rssi"],
            'snr': data["uplink_message"]["rx_metadata"][0]["snr"],
            'channel_rssi': data["uplink_message"]["rx_metadata"][0].get("channel_rssi", -100)
        }

        if not send_to_influx(influx_payload):
            print("Falha ao enviar dados para InfluxDB")
            
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")
    
def main():
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect=on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f"Ligado ao broker na porta {MQTT_BROKER}:{MQTT_PORT}...")
    except Exception as e:
        print(f"Falha na ligação MQTT: {e}")
        sys.exit(1)

    mqtt_client.loop_start
    
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("\nExecução interrompida...")
    finally:
        mqtt_client.disconnect()

if __name__ == "__main__":
    main()