import zmq
import msgpack
import time
import json
import os

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")
LOGINS_FILE = os.path.join(DATA_DIR, "logins.json")

def load_data():
    try:
        with open(CHANNELS_FILE, "r") as f:
            channels = json.load(f)
    except:
        channels = []

    try:
        with open(LOGINS_FILE, "r") as f:
            logins = json.load(f)
    except:
        logins = []

    return channels, logins

def save_channels(channels):
    with open(CHANNELS_FILE, "w") as f:
        json.dump(channels, f)

def save_logins(logins):
    with open(LOGINS_FILE, "w") as f:
        json.dump(logins, f)

channels, logins = load_data()

print("[SERVER PYTHON] Iniciado...", flush=True)

while True:
    raw = socket.recv()
    # print("[DEBUG PY SERVER] bytes recebidos:", len(raw), flush=True)

    msg = msgpack.unpackb(raw, raw=False)
    response = {}

    if msg["type"] == "login":
        user = msg["user"]

        logins.append({
            "user": user,
            "timestamp": msg["timestamp"]
        })
        save_logins(logins)

        response = {
            "status": "ok",
            "message": f"login realizado ({user})",
            "timestamp": time.time()
        }

    elif msg["type"] == "create_channel":
        ch = msg["channel"]

        if ch in channels:
            response = {
                "status": "error",
                "message": "canal já existe",
                "timestamp": time.time()
            }
        else:
            channels.append(ch)
            save_channels(channels)

            response = {
                "status": "ok",
                "message": f"canal '{ch}' criado",
                "timestamp": time.time()
            }

    elif msg["type"] == "list_channels":
        response = {
            "status": "ok",
            "channels": channels,
            "timestamp": time.time()
        }

    else:
        response = {
            "status": "error",
            "message": "tipo inválido",
            "timestamp": time.time()
        }

    print(f"[SERVER] Enviando resposta: {response}", flush=True)

    payload = msgpack.packb(response, use_bin_type=True)
    # print("[DEBUG PY SERVER] bytes enviados:", len(payload), flush=True)
    socket.send(payload)