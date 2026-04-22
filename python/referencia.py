# import time
# import zmq

# context = zmq.Context()
# socket = context.socket(zmq.REP)
# socket.bind("tcp://*:5560")

# servidores = {}
# proximo_rank = 1

# TIMEOUT = 10
# socket.RCVTIMEO = 1000 


# def limpar_servidores_mortos():
#     agora = time.time()

#     for nome, dados in servidores.items():
#         diff = agora - dados["last_seen"]
#         print(f"[DEBUG] {nome} - último heartbeat há {diff:.2f}s")

#     mortos = []

#     for nome, dados in servidores.items():
#         if agora - dados["last_seen"] > TIMEOUT:
#             mortos.append(nome)

#     for nome in mortos:
#         print(f"[REF] Removendo servidor inativo: {nome}")
#         del servidores[nome]


# print("[REF] Serviço de referência iniciado...")
# while True:
#     try:
#         mensagem = socket.recv_json()
#         tipo = mensagem.get("type")
#     except zmq.Again:
#         mensagem = None
#         tipo = None

#     limpar_servidores_mortos()

#     if mensagem is None:
#         continue

#     if tipo == "register":
#         nome = mensagem.get("name")

#         if nome not in servidores:
#             servidores[nome] = {
#                 "rank": proximo_rank,
#                 "last_seen": time.time()
#             }
#             proximo_rank += 1

#         resposta = {
#             "rank": servidores[nome]["rank"]
#         }

#     elif tipo == "list":
#         resposta = [
#             {"name": nome, "rank": dados["rank"]}
#             for nome, dados in servidores.items()
#         ]

#     elif tipo == "heartbeat":
#         nome = mensagem.get("name")

#         if nome in servidores:
#             servidores[nome]["last_seen"] = time.time()
#             print(f"[REF] Heartbeat recebido de: {nome}")

#         resposta = {
#             "status": "ok",
#             "timestamp": time.time()
#         }

#     else:
#         resposta = {"status": "error"}

#     socket.send_json(resposta)

import time
import zmq

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5560")

servidores = {}
proximo_rank = 1

TIMEOUT = 20
socket.RCVTIMEO = 1000


def limpar_servidores_mortos():
    agora = time.time()
    mortos = []

    for nome, dados in list(servidores.items()):
        diff = agora - dados["last_seen"]
        print(f"[DEBUG] {nome} - último heartbeat há {diff:.2f}s", flush=True)

        if diff > TIMEOUT:
            mortos.append(nome)

    for nome in mortos:
        print(f"[REF] Removendo servidor inativo: {nome}", flush=True)
        del servidores[nome]


print("[REF] Serviço de referência iniciado...", flush=True)

while True:
    try:
        mensagem = socket.recv_json()
        tipo = mensagem.get("type")
    except zmq.Again:
        mensagem = None
        tipo = None

    limpar_servidores_mortos()

    if mensagem is None:
        continue

    if tipo == "register":
        nome = mensagem.get("name")

        if nome not in servidores:
            servidores[nome] = {
                "rank": proximo_rank,
                "last_seen": time.time()
            }
            proximo_rank += 1
        else:
            servidores[nome]["last_seen"] = time.time()

        resposta = {
            "rank": servidores[nome]["rank"]
        }

    elif tipo == "list":
        resposta = [
            {"name": nome, "rank": dados["rank"]}
            for nome, dados in servidores.items()
        ]

    elif tipo == "heartbeat":
        nome = mensagem.get("name")

        if nome in servidores:
            servidores[nome]["last_seen"] = time.time()
            print(f"[REF] Heartbeat recebido de: {nome}", flush=True)
        else:
            # caso heartbeat chegue antes de register por algum motivo
            servidores[nome] = {
                "rank": proximo_rank,
                "last_seen": time.time()
            }
            proximo_rank += 1
            print(f"[REF] Servidor registrado via heartbeat: {nome}", flush=True)

        resposta = {
            "status": "ok",
            "timestamp": time.time()
        }

    else:
        resposta = {"status": "error", "message": "tipo inválido"}

    socket.send_json(resposta)