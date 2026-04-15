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

pasta_dados = Path('data')
pasta_dados.mkdir(exist_ok=True)

arquivo_canais = pasta_dados / 'channels.json'
arquivo_logins = pasta_dados / 'logins.json'
arquivo_requisicoes = pasta_dados / 'requests.jsonl'
arquivo_publicacoes = pasta_dados / 'publications.jsonl'

contador_server = 0


def ler_json(caminho, valor_padrao):
    if caminho.exists():
        with open(caminho, 'r', encoding='utf-8') as f:
            return json.load(f)
    return valor_padrao


def salvar_json(caminho, dados):
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def salvar_jsonl(caminho, dados):
    with open(caminho, 'a', encoding='utf-8') as f:
        f.write(json.dumps(dados, ensure_ascii=False) + '\n')


canais = ler_json(arquivo_canais, [])
logins = ler_json(arquivo_logins, [])

print('[SERVER PYTHON] Iniciado...', flush=True)

while True:
    mensagem_bruta = socket_rep.recv()
    mensagem = msgpack.unpackb(mensagem_bruta, raw=False)

    tipo = mensagem.get('type')
    usuario = mensagem.get('user', '')
    contador_recebido = mensagem.get('contador', 0)

    contador_server = max(contador_server, contador_recebido) + 1

    print("Contador servidor atualizado para:", contador_server)

    timestamp_recebimento = time.time()

    salvar_jsonl(arquivo_requisicoes, {
        'type': tipo,
        'user': usuario,
        'contador': contador_server,
        'request': mensagem,
        'received_timestamp': timestamp_recebimento
    })

    if tipo == 'login':
        novo_login = {
            'user': usuario,
            'timestamp': mensagem.get('timestamp', timestamp_recebimento),
            'contador': contador_server
        }
        logins.append(novo_login)
        salvar_json(arquivo_logins, logins)

        resposta = {
            'status': 'ok',
            'message': f'login realizado ({usuario})',
            'contador': contador_server,
            'timestamp': time.time()
        }

    elif tipo == 'create_channel':
        canal = mensagem.get('channel', '').strip()

        if not canal:
            resposta = {
                'status': 'error',
                'message': 'nome inválido',
                'contador': contador_server,
                'timestamp': time.time()
            }
        elif canal in canais:
            resposta = {
                'status': 'error',
                'message': 'já existe',
                'contador': contador_server,
                'timestamp': time.time()
            }
        else:
            canais.append(canal)
            salvar_json(arquivo_canais, canais)

            resposta = {
                'status': 'ok',
                'message': f"canal '{canal}' criado",
                'contador': contador_server,
                'timestamp': time.time()
            }

    elif tipo == 'list_channels':
        resposta = {
            'status': 'ok',
            'channels': canais,
            'contador': contador_server,
            'timestamp': time.time()
        }

    elif tipo == 'publish_message':
        canal = mensagem.get('channel', '').strip()
        texto = mensagem.get('message', '')

        if canal not in canais:
            resposta = {
                'status': 'error',
                'message': 'canal inexistente',
                'contador': contador_server,
                'timestamp': time.time()
            }
        else:
            publicacao = {
                'channel': canal,
                'user': usuario,
                'message': texto,
                'contador': contador_server,
                'request_timestamp': mensagem.get('timestamp', timestamp_recebimento),
                'published_timestamp': time.time()
            }

            socket_pub.send_multipart([
                canal.encode('utf-8'),
                msgpack.packb(publicacao, use_bin_type=True)
            ])

            salvar_jsonl(arquivo_publicacoes, publicacao)

            resposta = {
                'status': 'ok',
                'message': f"mensagem publicada em '{canal}'",
                'contador': contador_server,
                'timestamp': time.time()
            }

    else:
        resposta = {
            'status': 'error',
            'message': 'tipo inválido',
            'contador': contador_server,
            'timestamp': time.time()
        }

    print('[SERVER] Resposta:', resposta, flush=True)

    socket_rep.send(msgpack.packb(resposta, use_bin_type=True))