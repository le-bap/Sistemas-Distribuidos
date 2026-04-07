# import zmq
# import msgpack
# import time
# import random

# context = zmq.Context()
# socket = context.socket(zmq.REQ)
# socket.connect("tcp://broker:5555")

# def send(msg):
#     socket.send(msgpack.packb(msg))
#     return msgpack.unpackb(socket.recv(), raw=False)

# user = f"bot_{random.randint(1000,9999)}"

# print(f"[BOT] Iniciando como {user}")

# while True:
#     print("\n--- NOVO CICLO ---")

#     resp = send({
#         "type": "login",
#         "user": user,
#         "timestamp": time.time()
#     })
#     print("[LOGIN]", resp)


#     channel_name = f"canal_{random.randint(1,300)}"

#     resp = send({
#         "type": "create_channel",
#         "user": user,
#         "channel": channel_name,
#         "timestamp": time.time()
#     })
#     print("[CREATE CHANNEL]", resp)


#     resp = send({
#         "type": "list_channels",
#         "user": user,
#         "timestamp": time.time()
#     })
#     print("[LIST CHANNELS]", resp)

#     time.sleep(0.2)
import msgpack
import random
import threading
import time
import zmq

context = zmq.Context()

req_socket = context.socket(zmq.REQ)
req_socket.connect("tcp://broker:5555")

sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://proxy:5558")


def send(msg):
    req_socket.send(msgpack.packb(msg, use_bin_type=True))
    return msgpack.unpackb(req_socket.recv(), raw=False)


def now():
    return time.time()


user = f"bot_py_{random.randint(1000, 9999)}"
subscribed = set()


def subscriber_loop():
    while True:
        topic, payload = sub_socket.recv_multipart()
        data = msgpack.unpackb(payload, raw=False)
        recv_ts = time.time()
        print(
            f"[SUB][{user}] canal={topic.decode()} "
            f"mensagem={data.get('message')} "
            f"envio={data.get('published_timestamp')} "
            f"recebimento={recv_ts}",
            flush=True,
        )


threading.Thread(target=subscriber_loop, daemon=True).start()

print(f"[BOT PYTHON] Iniciando como {user}", flush=True)
print("[LOGIN]", send({"type": "login", "user": user, "timestamp": now()}), flush=True)

while True:
    channels_resp = send({"type": "list_channels", "user": user, "timestamp": now()})
    channels = channels_resp.get("channels", [])

    if len(channels) < 5:
        new_channel = f"canal_{random.randint(1, 999)}"
        resp = send(
            {
                "type": "create_channel",
                "user": user,
                "channel": new_channel,
                "timestamp": now(),
            }
        )
        print("[CREATE CHANNEL]", resp, flush=True)
        channels = send({"type": "list_channels", "user": user, "timestamp": now()}).get("channels", [])

    available = [c for c in channels if c not in subscribed]
    while len(subscribed) < 3 and available:
        ch = random.choice(available)
        sub_socket.setsockopt(zmq.SUBSCRIBE, ch.encode("utf-8"))
        subscribed.add(ch)
        print(f"[SUBSCRIBE][{user}] {ch}", flush=True)
        available = [c for c in channels if c not in subscribed]

    if not channels:
        time.sleep(1)
        continue

    chosen = random.choice(channels)

    for i in range(10):
        text = f"mensagem {i + 1} do {user}"
        resp = send(
            {
                "type": "publish_message",
                "user": user,
                "channel": chosen,
                "message": text,
                "timestamp": now(),
            }
        )
        print("[PUBLISH]", resp, flush=True)
        time.sleep(1)