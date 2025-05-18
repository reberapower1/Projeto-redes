import paho.mqtt.client as mqtt
import json
from influxdb_client import InfluxDBClient

# Configurações
GROUP_ID = "SEU_GROUP_ID"
INFLUXDB_URL = "https://us-west-2-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = "seu_token"
INFLUXDB_ORG = "seu_org"
INFLUXDB_BUCKET = "seu_bucket"

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

def on_message(client, userdata, msg):
    try{
        data = json.loads(msg.payload)
        print(f"Dados brutos recebidos: {data}")

        # Extrai o machine_id (ex: "M1")
        machine_id = data["end_device_ids"]["machine_id"]

        # Obtém o machine_code correspondente (ex: "A23X")
        machine_code = MACHINE_ID_TO_CODE.get(machine_id)

        if not machine_code:
            print(f"Erro: Machine ID {machine_id} não mapeado para nenhum código conhecido")
            return

        print(f"Machine ID: {machine_id} → Código: {machine_code}")
    
        # Extrai os dados dos sensores
        sensor_data = data["uplink_message"]["decoded_payload"]
        
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
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")
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