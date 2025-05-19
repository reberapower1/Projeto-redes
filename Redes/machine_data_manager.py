from datetime import datetime
import paho.mqtt.client as mqtt
import json

# ==========================
# CONFIGURAÇÕES
# ==========================

GROUP_ID = "22"
MQTT_BROKER = "10.6.1.9"
MQTT_PORT = 1883

TOPIC_TO_MACHINE_MANAGER = f"{GROUP_ID}/machine_data"

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
                "coolant_temperature": {
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
# VERIFICAÇÃO E COMANDO
# ==========================

def verificar_anomalias(sensor_data):
    alertas = []
    comandos = []

    for sensor, valor in sensor_data.items():
        if sensor not in intervals:
            print(f"Parâmetro desconhecido: {sensor}")
            continue

        lim = intervals[sensor]
        unidade = lim.get("unit", "")

        if valor < lim["low"]:
            diferenca = lim["low"] - valor
            alertas.append({
                "parameter": sensor,
                "value": f"{valor:.2f}{unidade}",
                "status": "LOW",
                "threshold": f"{lim['low']:.2f}{unidade}",
                "deviation": f"{diferenca:.2f}{unidade} abaixo"
            })
            comandos.append({
                "sensor": sensor,
                "action": "increase",
                "value": valor,
                "threshold": lim["low"],
                "unit": unidade
            })

        elif valor > lim["high"]:
            diferenca = valor - lim["high"]
            alertas.append({
                "parameter": sensor,
                "value": f"{valor:.2f}{unidade}",
                "status": "HIGH",
                "threshold": f"{lim['high']:.2f}{unidade}",
                "deviation": f"{diferenca:.2f}{unidade} acima"
            })
            comandos.append({
                "sensor": sensor,
                "action": "reduce",
                "value": valor,
                "threshold": lim["high"],
                "unit": unidade
            })

    return alertas, comandos

# ==========================
# MQTT
# ==========================

def configurar_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    return client

def ligar_mqtt(client):
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(TOPIC_TO_MACHINE_MANAGER)
    print(f"Subscrito no tópico: {TOPIC_TO_MACHINE_MANAGER}")

def desligar_mqtt(client):
    client.disconnect()
    print("Ligação MQTT terminada.")

def formatar_mensagem(machine_id, alertas=None, comandos=None):
    mensagem = {
        "machine_id": machine_id,
        "timestamp": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    }
    
    if len(alertas)!=0:
        mensagem["alerts"] = alertas
        print("\nALERTA ALERTA")
        for alerta in alertas:
            print(f"{alerta['parameter']}: {alerta['value']} "
                  f"({alerta['status']} - Limite: {alerta['threshold']}, "
                  f"Desvio: {alerta['deviation']})")
    
    if len(comandos):
        mensagem["commands"] = comandos
        print("\nCOMANDOS:")
        for comando in comandos:
            print(f"Ajustar {comando['sensor']}: {comando['action'].upper()} "
                  f"(Valor atual: {comando['value']:.2f}{comando['unit']}, "
                  f"Limite: {comando['threshold']:.2f}{comando['unit']})")
    
    return mensagem

def enviar_mensagem(client, mensagem):
    try:
        payload = json.dumps(mensagem)
        client.publish(TOPIC_TO_MACHINE_MANAGER, payload)
        print("\nMensagem enviada com sucesso!")
    except Exception as e:
        print(f"\nErro ao enviar mensagem: {e}")

def enviar_alerta(client, alerta):
    payload = json.dumps(alerta)
    client.publish(TOPIC_TO_MACHINE_MANAGER, payload)
    print(f"Alerta enviado !!!: {payload}")

def enviar_comando(client, comando):
    payload = json.dumps(comando)
    client.publish(TOPIC_TO_MACHINE_MANAGER, payload)
    print(f"Comando enviado: {payload}")

# ==========================
# CALLBACK DE MENSAGENS
# ==========================

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        print(f"\nDados recebidos de {data.get('machine_id')}")

        # Validação de campos
        required = ["group_id", "machine_id", "machine_code", "rpm", "oil_pressure", "coolant_temp",
                    "battery_potential", "consumption", "rssi", "snr", "channel_rssi", "timestamp"]

        if not all(key in data for key in required):
            print("\n[ERRO] Dados incompletos na mensagem recebida.")
            return

        sensor_data = {
            "rpm": data["rpm"],
            "coolant_temperature": data["coolant_temp"],
            "oil_pressure": data["oil_pressure"],
            "battery_potential": data["battery_potential"],
            "consumption": data["consumption"]
        }

        # Verificar anomalias
        alertas, comandos = verificar_anomalias(sensor_data)

        if len(alertas)!=0 or len(comandos)!=0:
            mensagem = formatar_mensagem(data["machine_id"], alertas, comandos)
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
