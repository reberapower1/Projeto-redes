import sys
import paho.mqtt.client as mqtt
import socket
import json

# Configurações
GROUP_ID = "22"
TOPICO = f"{GROUP_ID}/commands_from_machine_manager"
UDP_AM_IP = "localhost"
UDP_AM_PORT = 9999
MQTT_BROKER = "10.6.1.9"  
MQTT_PORT = 1883

udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def on_connect(client, userdata, flags, rc, properties=None):
    try:
        client.subscribe(TOPICO)
    except Exception as e:
        print(f"Falha na ligação MQTT: {e}")
        sys.exit(1)

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        print(f"[MQTT] Mensagem recebida em {msg.topic}:\n{payload_str}")
        
        payload_json = json.loads(payload_str)

        if "commands" in payload_json and payload_json["commands"]:
            udp_socket.sendto(payload_str.encode(), (UDP_AM_IP, UDP_AM_PORT))
            print("[MQTT→UDP] Comandos enviados para Alert Manager")
        else:
            print("[MQTT] Mensagem sem comandos. Ignorada.")

    except Exception as e:
        print(f"[ERRO] {e}")

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print(f"Ligado ao broker na porta {MQTT_BROKER}:{MQTT_PORT}...")
    except Exception as e:
        print(f"Falha na ligação MQTT: {e}")
        sys.exit(1)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[MQTT→UDP] Interrompido pelo utilizador.")
    finally:
        udp_socket.close()

if __name__ == "__main__":
    main()
