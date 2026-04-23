import json
import time
from pathlib import Path

import msgpack
import zmq

context = zmq.Context()

socket_rep = context.socket(zmq.REP)
socket_rep.connect('tcp://broker:5556')

socket_pub = context.socket(zmq.PUB)
socket_pub.connect('tcp://proxy:5557')

ref_socket = context.socket(zmq.REQ)
ref_socket.connect("tcp://referencia:5560")

nome_servidor = "server_python"

pasta_dados = Path('data')
pasta_dados.mkdir(exist_ok=True)

# arquivo_canais = pasta_dados / 'channels.json'
# arquivo_logins = pasta_dados / 'logins.json'
pasta_compartilhada = Path('/app/shared')
pasta_compartilhada.mkdir(exist_ok=True)

arquivo_canais = pasta_compartilhada / 'channels.json'
arquivo_logins = pasta_dados / 'logins.json'
arquivo_requisicoes = pasta_dados / 'requests.jsonl'
arquivo_publicacoes = pasta_dados / 'publications.jsonl'

contador_server = 0
offset_relogio = 0
contador_requisicoes = 0


def ler_json(caminho, valor_padrao):
    if caminho.exists():
        try:
            with open(caminho, 'r', encoding='utf-8') as f:
                conteudo = f.read().strip()
                if not conteudo:
                    return valor_padrao
                return json.loads(conteudo)
        except (json.JSONDecodeError, OSError):
            return valor_padrao
    return valor_padrao

def salvar_json(caminho, dados):
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def salvar_jsonl(caminho, dados):
    with open(caminho, 'a', encoding='utf-8') as f:
        f.write(json.dumps(dados, ensure_ascii=False) + '\n')


def agora_corrigido():
    return time.time() + offset_relogio


def atualizar_contador_recebido(cont_recebido):
    global contador_server
    contador_server = max(contador_server, cont_recebido)

def recarregar_canais():
    global canais
    canais = ler_json(arquivo_canais, [])

def recarregar_logins():
    global logins
    logins = ler_json(arquivo_logins, [])

def proximo_contador():
    global contador_server
    contador_server += 1
    return contador_server


def enviar_heartbeat():
    global offset_relogio

    ref_socket.send_json({
        "type": "heartbeat",
        "name": nome_servidor
    })
    resposta_ref = ref_socket.recv_json()

    tempo_ref = resposta_ref.get("timestamp")
    if tempo_ref is not None:
        offset_relogio = tempo_ref - time.time()
        print(f"[HEARTBEAT] tempo sincronizado: {tempo_ref}", flush=True)

    ref_socket.send_json({
        "type": "list"
    })
    lista_servidores = ref_socket.recv_json()

    print("[SERVIDORES ATIVOS]", flush=True)
    for s in lista_servidores:
        print(f" - {s['name']} (rank={s['rank']})", flush=True)


canais = ler_json(arquivo_canais, [])
logins = ler_json(arquivo_logins, [])

print('[SERVER PYTHON] Iniciado...', flush=True)

# registro inicial para obter rank
ref_socket.send_json({
    "type": "register",
    "name": nome_servidor
})
resposta = ref_socket.recv_json()
rank = resposta["rank"]

print(f"[SERVER] Meu rank: {rank}", flush=True)

while True:
    mensagem_bruta = socket_rep.recv()
    mensagem = msgpack.unpackb(mensagem_bruta, raw=False)

    recarregar_canais()
    recarregar_logins()

    contador_requisicoes += 1

    tipo = mensagem.get('type')
    usuario = mensagem.get('user', '')
    contador_recebido = mensagem.get('contador', 0)

    atualizar_contador_recebido(contador_recebido)

    print("Contador servidor atualizado para:", contador_server, flush=True)

    timestamp_recebimento = agora_corrigido()

    salvar_jsonl(arquivo_requisicoes, {
        'type': tipo,
        'user': usuario,
        'request': mensagem,
        'received_timestamp': timestamp_recebimento
    })

    if tipo == 'login':
        novo_login = {
            'user': usuario,
            'timestamp': mensagem.get('timestamp', timestamp_recebimento),
        }
        logins.append(novo_login)
        salvar_json(arquivo_logins, logins)

        resposta = {
            'status': 'ok',
            'message': f'login realizado ({usuario})',
            'timestamp': agora_corrigido()
        }

    elif tipo == 'create_channel':
        canal = mensagem.get('channel', '').strip()

        if not canal:
            resposta = {
                'status': 'error',
                'message': 'nome inválido',
                'timestamp': agora_corrigido()
            }
        elif canal in canais:
            resposta = {
                'status': 'error',
                'message': 'já existe',
                'timestamp': agora_corrigido()
            }
        else:
            canais.append(canal)
            salvar_json(arquivo_canais, canais)

            resposta = {
                'status': 'ok',
                'message': f"canal '{canal}' criado",
                'timestamp': agora_corrigido()
            }

    elif tipo == 'list_channels':
        resposta = {
            'status': 'ok',
            'channels': canais,
            'timestamp': agora_corrigido()
        }

    elif tipo == 'publish_message':
        canal = mensagem.get('channel', '').strip()
        texto = mensagem.get('message', '')

        if canal not in canais:
            resposta = {
                'status': 'error',
                'message': 'canal inexistente',
                'timestamp': agora_corrigido()
            }
        else:
            contador_pub = proximo_contador()

            publicacao = {
                'channel': canal,
                'user': usuario,
                'message': texto,
                'request_timestamp': mensagem.get('timestamp', timestamp_recebimento),
                'published_timestamp': agora_corrigido(),
                'contador': contador_pub
            }

            socket_pub.send_multipart([
                canal.encode('utf-8'),
                msgpack.packb(publicacao, use_bin_type=True)
            ])

            salvar_jsonl(arquivo_publicacoes, publicacao)

            resposta = {
                'status': 'ok',
                'message': f"mensagem publicada em '{canal}'",
                'timestamp': agora_corrigido()
            }

    else:
        resposta = {
            'status': 'error',
            'message': 'tipo inválido',
            'timestamp': agora_corrigido()
        }

    resposta['contador'] = proximo_contador()

    print('[SERVER] Resposta:', resposta, flush=True)
    socket_rep.send(msgpack.packb(resposta, use_bin_type=True))

    if contador_requisicoes % 10 == 0:
        enviar_heartbeat()