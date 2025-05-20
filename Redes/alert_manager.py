import socket
import json

host = "localhost"
port = 9999

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((host, port))
sock.settimeout(1.0)  # 1 segundo de timeout

# Dicionário global para guardar parâmetros fora do limite
alert_params = {}

print(f"[AM] Alert Manager a escutar em {host}:{port}")

try:
    while True:
        try:
            data, addr = sock.recvfrom(2048)
            message = data.decode('utf-8')
            print(f"[AM] Recebido de {addr}: {message}")

            try:
                parsed = json.loads(message)
                
                # Atualizar o dicionário global com parâmetros fora do limite
                if "alerts" in parsed:
                    alert_params.clear()  # limpar alertas anteriores para esta mensagem
                    for alert in parsed["alerts"]:
                        param = alert.get("parameter")
                        status = alert.get("status")
                        if status == "HIGH":
                            alert_params[param] = alert  # guarda o alerta completo para consulta
                    # Verificar se pelo menos 3 parâmetros estão fora do limite
                    if len(alert_params) >= 3:
                        print("[AM][CRITICAL] Estado crítico! Parâmetros fora do limite:")
                        for p in alert_params:
                            print(f"  - {p}: {alert_params[p]}")
                    else:
                        print(f"[AM] Estado normal. Parâmetros fora do limite: {len(alert_params)}")
                
                if "commands" in parsed:
                    print("[AM] Comandos recebidos:")
                    for cmd in parsed["commands"]:
                        print(f"  - Sensor: {cmd.get('sensor')}, Ação: {cmd.get('action')}, Ajuste: {cmd.get('adjustment')} {cmd.get('unit')}")

            except json.JSONDecodeError:
                print("[AM][ERRO] JSON inválido")

        except socket.timeout:
            continue

except KeyboardInterrupt:
    print("\n[AM] Encerrado.")
finally:
    sock.close()
