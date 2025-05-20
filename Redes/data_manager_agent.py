import sys
import paho.mqtt.client as mqtt
import json
from datetime import datetime
from influxdb_client_3 import InfluxDBClient3, Point 
import base64

GROUP_ID = sys.argv[1]

# ============================
# Definições do MQTT 
# ============================
MQTT_BROKER = "10.6.1.9"  
MQTT_PORT = 1883  

#=========================
# Tópicos MQTT 
#=========================
TOPIC_MACHINE_1 = f"v3/{GROUP_ID}@ttn/devices/M1/up"
TOPIC_MACHINE_2 = f"v3/{GROUP_ID}@ttn/devices/M2/up"
TOPIC_MACHINE_3 = f"v3/{GROUP_ID}@ttn/devices/M3/up"
TOPIC_MACHINE_4 = f"v3/{GROUP_ID}@ttn/devices/M4/up"
TOPIC_MACHINE_5 = f"v3/{GROUP_ID}@ttn/devices/M5/up"
TOPIC_MACHINE_6 = f"v3/{GROUP_ID}@ttn/devices/M6/up"
TOPIC_MACHINE_7 = f"v3/{GROUP_ID}@ttn/devices/M7/up"
TOPIC_MACHINE_8 = f"v3/{GROUP_ID}@ttn/devices/M8/up"

TOPIC_TO_MACHINE_MANAGER = f"{GROUP_ID}/data_to_machine_manager"
TOPIC_FROM_MACHINE_MANAGER = f"{GROUP_ID}/commands_from_machine_manager"

TOPIC_DOWN_M1 = f"v3/{GROUP_ID}@ttn/devices/M1/down/push_machine"
TOPIC_DOWN_M2 = f"v3/{GROUP_ID}@ttn/devices/M2/down/push_machine"
TOPIC_DOWN_M3 = f"v3/{GROUP_ID}@ttn/devices/M3/down/push_machine"
TOPIC_DOWN_M4 = f"v3/{GROUP_ID}@ttn/devices/M4/down/push_machine"
TOPIC_DOWN_M5 = f"v3/{GROUP_ID}@ttn/devices/M5/down/push_machine"
TOPIC_DOWN_M6 = f"v3/{GROUP_ID}@ttn/devices/M6/down/push_machine"
TOPIC_DOWN_M7 = f"v3/{GROUP_ID}@ttn/devices/M7/down/push_machine"
TOPIC_DOWN_M8 = f"v3/{GROUP_ID}@ttn/devices/M8/down/push_machine"

TOPIC_ALERT_M1 = f"v3/{GROUP_ID}@ttn/devices/M1/down/push_alert"
TOPIC_ALERT_M2 = f"v3/{GROUP_ID}@ttn/devices/M2/down/push_alert"
TOPIC_ALERT_M3 = f"v3/{GROUP_ID}@ttn/devices/M3/down/push_alert"
TOPIC_ALERT_M4 = f"v3/{GROUP_ID}@ttn/devices/M4/down/push_alert"
TOPIC_ALERT_M5 = f"v3/{GROUP_ID}@ttn/devices/M5/down/push_alert"
TOPIC_ALERT_M6 = f"v3/{GROUP_ID}@ttn/devices/M6/down/push_alert"
TOPIC_ALERT_M7 = f"v3/{GROUP_ID}@ttn/devices/M7/down/push_alert"
TOPIC_ALERT_M8 = f"v3/{GROUP_ID}@ttn/devices/M8/down/push_alert"

MACHINE_DOWNLINKS = {
    "M1": TOPIC_DOWN_M1,
    "M2": TOPIC_DOWN_M2,
    "M3": TOPIC_DOWN_M3,
    "M4": TOPIC_DOWN_M4,
    "M5": TOPIC_DOWN_M5,
    "M6": TOPIC_DOWN_M6,
    "M7": TOPIC_DOWN_M7,
    "M8": TOPIC_DOWN_M8
}


# Configurações
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

PARAMETER_BYTES = {
    "rpm": 0x01,
    "coolant_temp": 0x02,
    "oil_pressure": 0x03,
    "battery_potential": 0x04,
    "consumption": 0x05
}

#============================================================
#Função que converte para as unidades de medida da máquina M1
#=============================================================
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
    if source_units.get("oil_pressure_unit") == "bar":
        converted_data["oil_pressure"] = oil_pressure * CONVERSION_FACTORS["bar_to_psi"]
    else:
        converted_data["oil_pressure"] = oil_pressure
    
    # Conversão de temperatura para °C 
    coolant_temp = sensor_data.get("coolant_temperature", 0)
    if source_units.get("coolant_temp_unit") == "°F":
        converted_data["coolant_temp"] = CONVERSION_FACTORS["f_to_c"](coolant_temp)
    else:
        converted_data["coolant_temp"] = coolant_temp
    
    # Conversão de potencial da bateria para V 
    battery_potential = sensor_data.get("battery_potential", 0)
    if source_units.get("battery_potential_unit") == "mV":
        converted_data["battery_potential"] = battery_potential * CONVERSION_FACTORS["mv_to_v"]
    else:
        converted_data["battery_potential"] = battery_potential
    
    # Conversão de consumo para l/h 
    consumption = sensor_data.get("consumption", 0)
    if source_units.get("consumption_unit") == "gal/h":
        converted_data["consumption"] = consumption * CONVERSION_FACTORS["gal_to_l"]
    else:
        converted_data["consumption"] = consumption
    
    return converted_data

#=======================================================
#Função para enviar os dados para a base de dados Influx
# #=====================================================
def send_to_influx(machine_data):
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
        points.append(sensor_point)

        # Dados da rede
        signal_point = Point("network_metrics") \
            .tag("machine_id", machine_data['machine_id']) \
            .field("rssi", float(machine_data['rssi'])) \
            .field("snr", float(machine_data['snr'])) \
            .field("channel_rssi", float(machine_data.get('channel_rssi', -100))) \
            .time(datetime.now())
        points.append(signal_point)
        
        influx_client.write(points)
        
        print(f"Dados da máquina {machine_data['machine_id']} enviados para a base de dados")
        return True
        
    except Exception as e:
        print(f"Erro ao enviar dados: {str(e)}")
        return False
    
#Processa comandos recebidos do Machine Manager
def process_command(client, machine_id, command):
    try:
        parameter = command["sensor"]
        value = int(command["adjustment"])
        
        if parameter not in PARAMETER_BYTES:
            raise ValueError(f"Parâmetro inválido: {parameter}")
        
        send_machine_command(client, machine_id, parameter, value)
        
    except Exception as e:
        print(f"Erro ao processar comando: {e}")
        print(f"Comando problemático: {command}") 

#========================================================
#Função para enviar os dados para o machine manager agent
#========================================================
def send_to_machine_data_manager(client, machine_data):
    try:
        topic = TOPIC_TO_MACHINE_MANAGER  
        payload = {
            "group_id": GROUP_ID,
            "machine_id": machine_data["machine_id"],
            "machine_code": machine_data["machine_code"],
            "rpm": machine_data["rpm"],
            "oil_pressure": machine_data["oil_pressure"],
            "coolant_temp": machine_data["coolant_temp"],
            "battery_potential": machine_data["battery_potential"],
            "consumption": machine_data["consumption"],
            "rssi": machine_data["rssi"],
            "snr": machine_data["snr"],
            "channel_rssi": machine_data["channel_rssi"],
            "timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        } 

        client.publish(topic, json.dumps(payload), qos=1)
        print(f"Dados enviados para o Machine Data Manager {payload}")
    except Exception as e:
        print(f"Erro ao enviar para o Machine Data Manager: {e}")

#================================
#Criar os comandos para a máquina
#================================
def build_control_message(parameter: str, adjustment: int) -> str:
    # Verificação se o parâmetro é válido
    if parameter not in PARAMETER_BYTES:
        raise ValueError(f"Parâmetro inválido: {parameter}")
    
    # Verificação de signed value
    if not (-128 <= adjustment <= 127):
        raise ValueError("O valor de ajuste deve estar entre -128 e 127.")
    
    message_type = 0x01  
    action_type = 0x01   # Modificar parâmetro
    parameter_byte = PARAMETER_BYTES[parameter]
    adjustment_byte = adjustment & 0xFF  # Converter para byte 

    # Construir bytes da mensagem
    byte_array = bytes([message_type, action_type, parameter_byte, adjustment_byte])

    # Codificar em base64
    frm_payload = base64.b64encode(byte_array).decode('utf-8')
    return frm_payload

#===============================
#Enviar comando para a máquina
#===============================
def send_machine_command(client, machine_id, parameter, adjustment):
    try:
        # Verifica se a máquina existe no dicionario
        if machine_id not in MACHINE_DOWNLINKS:
            raise ValueError(f"Máquina {machine_id} não configurada para downlinks")
        
        frm_payload = build_control_message(parameter, adjustment)
        downlink_msg = {
            "downlinks": [{
                "frm_payload": frm_payload,
                "f_port": 10,
                "priority": "NORMAL"
            }]
        }
        
        # Obtém o tópico específico para esta máquina
        topic = MACHINE_DOWNLINKS[machine_id]
        client.publish(topic, json.dumps(downlink_msg))
        print(f"Comando enviado para {machine_id} no tópico: {topic}")
        
    except Exception as e:
        print(f"Erro ao enviar comando para {machine_id}: {e}")

# ============================
# Funções MQTT 
# ============================
def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Ligao ao broker MQTT com o código {rc}")
    try:
        client.subscribe(TOPIC_MACHINE_1)
        client.subscribe(TOPIC_MACHINE_2)
        client.subscribe(TOPIC_MACHINE_3)
        client.subscribe(TOPIC_MACHINE_4)
        client.subscribe(TOPIC_MACHINE_5)
        client.subscribe(TOPIC_MACHINE_6)
        client.subscribe(TOPIC_MACHINE_7)
        client.subscribe(TOPIC_MACHINE_8)
        client.subscribe(TOPIC_FROM_MACHINE_MANAGER)
        print("subscrito no tópicos")
    except Exception as e:
        print ("Falha ao subscrever os tópicos")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        
        if "end_device_ids" in data:  # Mensagem de dados da máquina
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
            
            send_to_machine_data_manager(mqtt_client, influx_payload)

        elif msg.topic == TOPIC_FROM_MACHINE_MANAGER:
            if "commands" in data:
                for cmd in data["commands"]:
                    process_command(client, data["machine_id"], cmd)

    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")
    
def main():
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect=on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f"Ligado ao broker na porta {MQTT_BROKER}:{MQTT_PORT}...")
    except Exception as e:
        print(f"Falha na ligação MQTT: {e}")
        sys.exit(1)

    try:
        mqtt_client.loop_forever()  
    except KeyboardInterrupt:
        print("\nExecução interrompida...")
    finally:
        mqtt_client.disconnect()

if __name__ == "__main__":
    main()