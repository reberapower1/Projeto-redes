import socket
import time
from collections import defaultdict

# Configuração UDP
UDP_IP = "127.0.0.1"
UDP_PORT = 5005

alerts = defaultdict(list)

def evaluate_criticality(machine_id):
    # Exemplo: Se 5 alertas em 2 minutos, CRITICAL
    recent_alerts = [t for t in alerts[machine_id] if time.time() - t < 120]
    if len(recent_alerts) >= 5:
        send_udp_alert(machine_id, "CRITICAL")

def send_udp_alert(machine_id, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(f"{machine_id}:{message}".encode(), (UDP_IP, UDP_PORT))