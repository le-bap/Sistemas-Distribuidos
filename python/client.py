import random
import threading
import time

import msgpack
import zmq

context = zmq.Context()

req_socket = context.socket(zmq.REQ)
req_socket.connect('tcp://broker:5555')

sub_socket = context.socket(zmq.SUB)
sub_socket.connect('tcp://proxy:5558')

user = f"bot_py_{random.randint(1000, 9999)}"
canais_inscritos = []

cont = 0


def agora():
    return time.time()


def atualizar_contador(cont_recebido, origem=""):
    global cont
    cont = max(cont, cont_recebido) + 1


def enviar_para_servidor(mensagem):
    global cont

    req_socket.send(msgpack.packb(mensagem, use_bin_type=True))
    resposta = msgpack.unpackb(req_socket.recv(), raw=False)

    atualizar_contador(resposta.get('contador', 0))
    return resposta


def fazer_login():
    global cont
    cont += 1

    resposta = enviar_para_servidor({
        'type': 'login',
        'user': user,
        'timestamp': agora(),
        'contador': cont
    })
    print('[LOGIN]', resposta, flush=True)


def listar_canais():
    global cont
    cont += 1

    resposta = enviar_para_servidor({
        'type': 'list_channels',
        'user': user,
        'timestamp': agora(),
        'contador': cont
    })

    if resposta.get('status') == 'ok':
        return resposta.get('channels', [])

    return []


def criar_canal():
    global cont
    cont += 1

    nome_canal = f"canal_{random.randint(1, 999)}"

    resposta = enviar_para_servidor({
        'type': 'create_channel',
        'user': user,
        'channel': nome_canal,
        'timestamp': agora(),
        'contador': cont
    })

    print('[CREATE CHANNEL]', resposta, flush=True)


def se_inscrever_em_um_canal(canais_disponiveis):
    canais_nao_inscritos = [
        c for c in canais_disponiveis if c not in canais_inscritos
    ]

    if not canais_nao_inscritos:
        return

    canal_escolhido = random.choice(canais_nao_inscritos)
    sub_socket.setsockopt(zmq.SUBSCRIBE, canal_escolhido.encode('utf-8'))
    canais_inscritos.append(canal_escolhido)

    print(f'[SUBSCRIBE] {user} inscrito em {canal_escolhido}', flush=True)


def publicar_mensagem(canal, numero):
    global cont
    cont += 1

    texto = f"mensagem {numero} do {user}"

    resposta = enviar_para_servidor({
        'type': 'publish_message',
        'user': user,
        'channel': canal,
        'message': texto,
        'timestamp': agora(),
        'contador': cont
    })

    print('[PUBLISH]', resposta, flush=True)


def receber_mensagens():
    global cont

    while True:
        topico, conteudo = sub_socket.recv_multipart()

        dados = msgpack.unpackb(conteudo, raw=False)

        atualizar_contador(dados.get('contador', 0))

        canal = topico.decode('utf-8')
        mensagem = dados.get('message')
        envio = dados.get('published_timestamp')
        recebimento = agora()

        print(
            f"[MENSAGEM RECEBIDA] canal={canal} | mensagem={mensagem} "
            f"| envio={envio} | recebimento={recebimento} | contador_local={cont}",
            flush=True
        )


threading.Thread(target=receber_mensagens, daemon=True).start()

print(f'[CLIENTE PYTHON] Bot iniciado: {user}', flush=True)

fazer_login()

while True:
    canais = listar_canais()

    if len(canais) < 5:
        criar_canal()
        canais = listar_canais()

    if len(canais_inscritos) < 3:
        se_inscrever_em_um_canal(canais)

    if not canais:
        time.sleep(1)
        continue

    canal_escolhido = random.choice(canais)

    for i in range(10):
        publicar_mensagem(canal_escolhido, i + 1)
        time.sleep(1)