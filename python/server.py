# import zmq
# import msgpack
# import time
# import json
# import os

# context = zmq.Context()
# socket = context.socket(zmq.REP)
# socket.connect("tcp://broker:5556")

# DATA_DIR = "data"
# os.makedirs(DATA_DIR, exist_ok=True)

# CHANNELS_FILE = os.path.join(DATA_DIR, "channels.json")
# LOGINS_FILE = os.path.join(DATA_DIR, "logins.json")

# def load_data():
#     try:
#         with open(CHANNELS_FILE, "r") as f:
#             channels = json.load(f)
#     except:
#         channels = []

#     try:
#         with open(LOGINS_FILE, "r") as f:
#             logins = json.load(f)
#     except:
#         logins = []

#     return channels, logins

# def save_channels(channels):
#     with open(CHANNELS_FILE, "w") as f:
#         json.dump(channels, f)

# def save_logins(logins):
#     with open(LOGINS_FILE, "w") as f:
#         json.dump(logins, f)

# channels, logins = load_data()

# print("[SERVER PYTHON] Iniciado...", flush=True)

# while True:
#     raw = socket.recv()
#     # print("[DEBUG PY SERVER] bytes recebidos:", len(raw), flush=True)

#     msg = msgpack.unpackb(raw, raw=False)
#     response = {}

#     if msg["type"] == "login":
#         user = msg["user"]

#         logins.append({
#             "user": user,
#             "timestamp": msg["timestamp"]
#         })
#         save_logins(logins)

#         response = {
#             "status": "ok",
#             "message": f"login realizado ({user})",
#             "timestamp": time.time()
#         }

#     elif msg["type"] == "create_channel":
#         ch = msg["channel"]

#         if ch in channels:
#             response = {
#                 "status": "error",
#                 "message": "canal já existe",
#                 "timestamp": time.time()
#             }
#         else:
#             channels.append(ch)
#             save_channels(channels)

#             response = {
#                 "status": "ok",
#                 "message": f"canal '{ch}' criado",
#                 "timestamp": time.time()
#             }

#     elif msg["type"] == "list_channels":
#         response = {
#             "status": "ok",
#             "channels": channels,
#             "timestamp": time.time()
#         }

#     else:
#         response = {
#             "status": "error",
#             "message": "tipo inválido",
#             "timestamp": time.time()
#         }

#     print(f"[SERVER] Enviando resposta: {response}", flush=True)

#     payload = msgpack.packb(response, use_bin_type=True)
#     # print("[DEBUG PY SERVER] bytes enviados:", len(payload), flush=True)
#     socket.send(payload)
import json
import time
from pathlib import Path

import msgpack
import zmq

context = zmq.Context()

rep_socket = context.socket(zmq.REP)
rep_socket.connect("tcp://broker:5556")

pub_socket = context.socket(zmq.PUB)
pub_socket.connect("tcp://proxy:5557")

data_dir = Path("data")
data_dir.mkdir(parents=True, exist_ok=True)

CHANNELS_FILE = data_dir / "channels.json"
LOGINS_FILE = data_dir / "logins.json"
REQUESTS_FILE = data_dir / "requests.jsonl"
PUBLICATIONS_FILE = data_dir / "publications.jsonl"


def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, value):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


def append_jsonl(path, item):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


channels = load_json(CHANNELS_FILE, [])
logins = load_json(LOGINS_FILE, [])

# evita duplicatas se houver sujeira no arquivo
channels = sorted(set(channels))

print("[SERVER PYTHON] Iniciado...", flush=True)

while True:
    raw = rep_socket.recv()
    msg = msgpack.unpackb(raw, raw=False)

    now = time.time()
    msg_type = msg.get("type", "")
    user = msg.get("user", "")

    append_jsonl(
        REQUESTS_FILE,
        {
            "type": msg_type,
            "user": user,
            "request": msg,
            "received_timestamp": now,
        },
    )

    if msg_type == "login":
        entry = {"user": user, "timestamp": msg.get("timestamp", now)}
        logins.append(entry)
        save_json(LOGINS_FILE, logins)

        response = {
            "status": "ok",
            "message": f"login realizado ({user})",
            "timestamp": time.time(),
        }

    elif msg_type == "create_channel":
        ch = msg.get("channel", "").strip()

        if not ch:
            response = {
                "status": "error",
                "message": "nome de canal inválido",
                "timestamp": time.time(),
            }
        elif ch in channels:
            response = {
                "status": "error",
                "message": "canal já existe",
                "timestamp": time.time(),
            }
        else:
            channels.append(ch)
            channels.sort()
            save_json(CHANNELS_FILE, channels)

            response = {
                "status": "ok",
                "message": f"canal '{ch}' criado",
                "timestamp": time.time(),
            }

    elif msg_type == "list_channels":
        response = {
            "status": "ok",
            "channels": channels,
            "timestamp": time.time(),
        }

    elif msg_type == "publish_message":
        ch = msg.get("channel", "").strip()
        text = msg.get("message", "")

        if ch not in channels:
            response = {
                "status": "error",
                "message": "canal inexistente",
                "timestamp": time.time(),
            }
        else:
            publication = {
                "channel": ch,
                "user": user,
                "message": text,
                "request_timestamp": msg.get("timestamp", now),
                "published_timestamp": time.time(),
                "server": "python",
            }

            pub_socket.send_multipart(
                [
                    ch.encode("utf-8"),
                    msgpack.packb(publication, use_bin_type=True),
                ]
            )

            append_jsonl(PUBLICATIONS_FILE, publication)

            response = {
                "status": "ok",
                "message": f"mensagem publicada em '{ch}'",
                "timestamp": time.time(),
            }

    else:
        response = {
            "status": "error",
            "message": "tipo inválido",
            "timestamp": time.time(),
        }

    print(f"[SERVER PYTHON] {response}", flush=True)
    rep_socket.send(msgpack.packb(response, use_bin_type=True))