import random
import threading
import time

import msgpack
import zmq


context = zmq.Context()

REQ_ADDR = "tcp://broker:5555"
SUB_ADDR = "tcp://proxy:5558"

nome_bot = f"bot_python_{random.randint(1000, 9999)}"

contador_local = 0
canais_inscritos = set()


def agora():
    return time.time()


def proximo_contador():
    global contador_local
    contador_local += 1
    return contador_local


def atualizar_contador_recebido(valor):
    global contador_local

    try:
        valor = int(valor)
    except Exception:
        valor = 0

    contador_local = max(contador_local, valor)


def enviar_requisicao(tipo, **dados):
    socket = context.socket(zmq.REQ)
    socket.connect(REQ_ADDR)

    mensagem = {
        "type": tipo,
        "user": nome_bot,
        "timestamp": agora(),
        "contador": proximo_contador(),
    }

    mensagem.update(dados)

    socket.send(msgpack.packb(mensagem, use_bin_type=True))

    resposta_bruta = socket.recv()
    resposta = msgpack.unpackb(resposta_bruta, raw=False)

    atualizar_contador_recebido(resposta.get("contador", 0))

    socket.close()
    return resposta


def listar_canais():
    resposta = enviar_requisicao("list_channels")

    if resposta.get("status") == "ok":
        canais = resposta.get("channels", [])
        if isinstance(canais, list):
            return canais

    return []


def criar_canal():
    nome_canal = f"canal_{random.randint(100, 999)}"
    resposta = enviar_requisicao("create_channel", channel=nome_canal)

    print(f"[CREATE CHANNEL] {nome_canal} -> {resposta}", flush=True)
    return resposta


def fazer_login():
    resposta = enviar_requisicao("login")
    print(f"[LOGIN] {resposta}", flush=True)


def thread_receber_mensagens():
    sub = context.socket(zmq.SUB)
    sub.connect(SUB_ADDR)

    canais_assinados = set()

    while True:
        for canal in list(canais_inscritos):
            if canal not in canais_assinados:
                sub.setsockopt_string(zmq.SUBSCRIBE, canal)
                canais_assinados.add(canal)

        try:
            topico, mensagem_bruta = sub.recv_multipart()
            recebimento = agora()

            canal = topico.decode("utf-8")
            mensagem = msgpack.unpackb(mensagem_bruta, raw=False)

            atualizar_contador_recebido(mensagem.get("contador", 0))

            print(
                f"[MENSAGEM RECEBIDA] canal={canal} | "
                f"mensagem={mensagem.get('message')} | "
                f"envio={mensagem.get('published_timestamp')} | "
                f"recebimento={recebimento} | "
                f"contador_local={contador_local}",
                flush=True,
            )

        except Exception as e:
            print(f"[ERRO SUB] {e}", flush=True)


def garantir_canais_minimos():
    canais = listar_canais()

    if len(canais) < 5:
        criar_canal()
        canais = listar_canais()

    return canais


def garantir_inscricoes(canais):
    canais_disponiveis = [c for c in canais if c not in canais_inscritos]

    while len(canais_inscritos) < 3 and canais_disponiveis:
        canal = random.choice(canais_disponiveis)
        canais_inscritos.add(canal)
        canais_disponiveis.remove(canal)

        print(f"[SUBSCRIBE] {nome_bot} inscrito em {canal}", flush=True)


def publicar_mensagens(canais):
    if not canais:
        print("[CLIENT PYTHON] Nenhum canal disponível para publicar.", flush=True)
        return

    canal = random.choice(canais)

    for i in range(10):
        texto = f"mensagem {i + 1} do {nome_bot}"

        resposta = enviar_requisicao(
            "publish_message",
            channel=canal,
            message=texto,
        )

        print(
            f"[PUBLISH] {texto} -> {canal} | resposta={resposta} | contador_local={contador_local}",
            flush=True,
        )

        time.sleep(1)


print(f"[CLIENT PYTHON] Bot iniciado: {nome_bot}", flush=True)

fazer_login()

threading.Thread(target=thread_receber_mensagens, daemon=True).start()

while True:
    canais = garantir_canais_minimos()
    garantir_inscricoes(canais)

    canais = listar_canais()
    publicar_mensagens(canais)