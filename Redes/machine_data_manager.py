from datetime import datetime
import paho.mqtt.client as mqtt
import json

# ==========================
# CONFIGURAÇÕES
# ==========================

GROUP_ID = "22"
MQTT_BROKER = "10.6.1.9"
MQTT_PORT = 1883

TOPIC_FROM_DATA_MANAGER = f"{GROUP_ID}/data_to_machine_manager"
TOPIC_TO_DATA_MANAGER = f"{GROUP_ID}/commands_from_machine_manager"

# ==========================
# LÊ INTERVALOS DO FICHEIRO DE CONFIGURAÇÃO
# ==========================
def load_intervals():
    intervals = {}

    try:
        with open("intervals.cfg", "r") as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            
            intervals = {
                "rpm": {
                    "low": float(lines[0].split()[0]),
                    "high": float(lines[0].split()[1]),
                    "unit": "rpm"
                },
                "coolant_temp": {
                    "low": float(lines[1].split()[0]),
                    "high": float(lines[1].split()[1]),
                    "unit": "°C"
                },
                "oil_pressure": {
                    "low": float(lines[2].split()[0]),
                    "high": float(lines[2].split()[1]),
                    "unit": "psi"
                },
                "battery_potential": {
                    "low": float(lines[3].split()[0]),
                    "high": float(lines[3].split()[1]),
                    "unit": "V"
                },
                "consumption": {
                    "low": float(lines[4].split()[0]),
                    "high": float(lines[4].split()[1]),
                    "unit": "l/h"
                }
            }
        return intervals
    except Exception as e:
        print(f"Erro ao carregar intervalos: {e}")

intervals = load_intervals()
        
# ==========================
# VERIFICA SE OS VALORES ESTÃO CONFORME OS LIMITES DO FICHEIRO INTERVALS
# ==========================

def verificar_anomalias(sensor_data):
    avisos = []
    comandos = []

    for sensor, valor in sensor_data.items():
        if sensor not in intervals:
            print(f"Parâmetro desconhecido: {sensor}")
            continue

        lim = intervals[sensor]
        unidade = lim.get("unit", "")

        #se estiver abaixo, o comando é para aumentar 
        if valor < lim["low"]:
            ajuste = lim["low"] - valor
            avisos.append({
                "parameter": sensor,
                "value": f"{valor:.2f}{unidade}",
                "status": "LOW",
                "threshold": f"{lim['low']:.2f}{unidade}",
                "deviation": f"{ajuste:.2f}{unidade} abaixo"
            })
            comandos.append({
                "sensor": sensor,
                "action": "increase",
                "adjustment": ajuste,
                "unit": unidade
            })

        #se estiver alto, o comando é para baixar
        elif valor > lim["high"]:
            ajuste = valor - lim["high"]
            avisos.append({
                "parameter": sensor,
                "value": f"{valor:.2f}{unidade}",
                "status": "HIGH",
                "threshold": f"{lim['high']:.2f}{unidade}",
                "deviation": f"{ajuste:.2f}{unidade} acima"
            })
            comandos.append({
                "sensor": sensor,
                "action": "reduce",
                "adjustment": -ajuste, 
                "unit": unidade
            })

    return avisos,comandos

# ==========================
# MQTT
# ==========================

def configurar_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    return client

def ligar_mqtt(client):
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(TOPIC_FROM_DATA_MANAGER)
    print(f"Subscrito no tópico: {TOPIC_TO_DATA_MANAGER}")

def desligar_mqtt(client):
    client.disconnect()
    print("Ligação MQTT terminada.")

def formatar_mensagem(machine_id, alertas=None, comandos=None):
    mensagem = {
        "machine_id": machine_id,
        "timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    }
    
    if alertas:
        mensagem["alerts"] = alertas
    
    if comandos:
        mensagem["commands"] = comandos
    
    return mensagem

def enviar_mensagem(client, mensagem):
    try:
        payload = json.dumps(mensagem)
        client.publish(TOPIC_TO_DATA_MANAGER, payload)
        print("\nComando enviada com sucesso!")
    except Exception as e:
        print(f"\nErro ao enviar mensagem: {e}")

# ==========================
# CALLBACK DE MENSAGENS
# ==========================

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        print("Dados recebidos: ", data)
        print(f"\nDados recebidos através do data manager da máquina {data.get('machine_id')}")

        # Validação de campos
        required = ["group_id", "machine_id", "machine_code", "rpm", "oil_pressure", "coolant_temp",
                    "battery_potential", "consumption", "rssi", "snr", "channel_rssi", "timestamp"]
        
        missing = [key for key in required if key not in data]
        if missing:
            print(f"\n[ERRO] Campos obrigatórios faltando: {missing}")
            return
        
        # Verifica valores None
        for key in required:
            if data.get(key) is None:
                print(f"\n[ERRO] Campo {key} está None")
                return

        #retira os valores dos sensores da máquina
        sensor_data = {
            "rpm": data["rpm"],
            "coolant_temp": data["coolant_temp"],
            "oil_pressure": data["oil_pressure"],
            "battery_potential": data["battery_potential"],
            "consumption": data["consumption"]
        }

        # Verificar se estão dentro dos limites
        avisos, comandos = verificar_anomalias(sensor_data)

        #envia alerta ou comando dependendo da situação
        if len(avisos)!=0 or len(comandos)!=0:
            print("valore(s) fora dos limites")
            mensagem = formatar_mensagem(data["machine_id"], avisos, comandos)
            enviar_mensagem(client, mensagem)
        else:
            print("\nTodos as variáveis dentro dos limites")

    except json.JSONDecodeError:
        print("\nMensagem MQTT com formato inválido")
    except Exception as e:
        print(f"\n[ERRO] ao processar mensagem: {e}")

# ==========================
# MAIN
# ==========================

def main():
    client = configurar_mqtt()
    try:
        ligar_mqtt(client)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nInterrompido pelo utilizador.")
    finally:
        desligar_mqtt(client)

if __name__ == "__main__":
    main()
