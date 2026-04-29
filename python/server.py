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
ref_socket.connect('tcp://referencia:5560')

nome_servidor = 'server_python'

pasta_dados = Path('data')
pasta_dados.mkdir(exist_ok=True)

pasta_compartilhada = Path('/app/shared')
pasta_compartilhada.mkdir(exist_ok=True)

arquivo_canais = pasta_compartilhada / 'channels.json'
arquivo_coordenador = pasta_compartilhada / 'coordenador.json'

arquivo_logins = pasta_dados / 'logins.json'
arquivo_requisicoes = pasta_dados / 'requests.jsonl'
arquivo_publicacoes = pasta_dados / 'publications.jsonl'

contador_server = 0
contador_requisicoes = 0
offset_relogio = 0

rank = None
coordenador = ''
servidores_ativos = []

INTERVALO_SINCRONIZACAO = 15


def ler_json(caminho, valor_padrao):
    if caminho.exists():
        try:
            with open(caminho, 'r', encoding='utf-8') as f:
                conteudo = f.read().strip()
                if not conteudo:
                    return valor_padrao
                return json.loads(conteudo)
        except Exception:
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


def proximo_contador():
    global contador_server
    contador_server += 1
    return contador_server


def recarregar_canais():
    global canais
    canais = ler_json(arquivo_canais, [])


def recarregar_logins():
    global logins
    logins = ler_json(arquivo_logins, [])


def registrar_no_servico_referencia():
    global rank

    ref_socket.send_json({
        'type': 'register',
        'name': nome_servidor
    })

    resposta = ref_socket.recv_json()
    rank = resposta.get('rank')

    print(f'[SERVER PYTHON] Meu rank: {rank}', flush=True)


def pedir_lista_servidores():
    global servidores_ativos

    ref_socket.send_json({'type': 'list'})
    resposta = ref_socket.recv_json()

    if isinstance(resposta, list):
        servidores_ativos = resposta
    else:
        servidores_ativos = []

    print('[SERVIDORES ATIVOS]', flush=True)
    for servidor in servidores_ativos:
        print(f" - {servidor.get('name')} (rank={servidor.get('rank')})", flush=True)

    return servidores_ativos


def enviar_heartbeat():
    ref_socket.send_json({
        'type': 'heartbeat',
        'name': nome_servidor
    })

    resposta = ref_socket.recv_json()
    print(f'[HEARTBEAT] resposta={resposta}', flush=True)


def publicar_coordenador(nome_coordenador):
    mensagem = {
        'type': 'coordinator_announce',
        'coordinator': nome_coordenador,
        'server': nome_servidor,
        'timestamp': agora_corrigido(),
        'contador': proximo_contador()
    }

    socket_pub.send_multipart([
        b'servers',
        msgpack.packb(mensagem, use_bin_type=True)
    ])

    print(f'[PUB SERVERS] coordenador eleito: {nome_coordenador}', flush=True)


def salvar_coordenador(nome_coordenador):
    dados = {
        'coordinator': nome_coordenador,
        'timestamp': time.time(),
        'clock': contador_server
    }
    salvar_json(arquivo_coordenador, dados)


def eleger_coordenador():
    global coordenador

    print('[ELEICAO] Iniciando eleição...', flush=True)

    servidores = pedir_lista_servidores()

    if not servidores:
        print('[ELEICAO] Não há servidores ativos para eleger.', flush=True)
        return

    # Regra simples: vence quem tem o menor rank.
    servidor_eleito = min(servidores, key=lambda s: s.get('rank', 999999))
    coordenador = servidor_eleito.get('name')

    salvar_coordenador(coordenador)
    publicar_coordenador(coordenador)

    print(f'[ELEICAO] Coordenador escolhido: {coordenador}', flush=True)


def sou_coordenador():
    return coordenador == nome_servidor

def sincronizar_berkeley():
    global offset_relogio

    if coordenador == "":
        return

    if coordenador == nome_servidor:
        print(
            f"[BERKELEY] Eu sou o coordenador ({nome_servidor}). Hora atual={agora_corrigido()}",
            flush=True
        )
    else:
        print(
            f"[BERKELEY] Coordenador atual é {coordenador}. Aguardando sincronização dele.",
            flush=True
        )

def coordenador_vivo(servidores):
    nomes = [s.get('name') for s in servidores]
    return coordenador in nomes

 def parte4_a_cada_15_mensagens():
    enviar_heartbeat()

    servidores = pedir_lista_servidores()

    if not coordenador or not coordenador_vivo(servidores):
        print('[ELEICAO] Coordenador caiu ou inexistente!', flush=True)
        eleger_coordenador()

    # 🔥 Berkeley (REQ relógio)
    if coordenador and coordenador != nome_servidor:
        print(f"[BERKELEY] Pedindo hora para {coordenador}", flush=True)

    sincronizar_berkeley()

canais = ler_json(arquivo_canais, [])
logins = ler_json(arquivo_logins, [])

print('[SERVER PYTHON] Iniciado...', flush=True)

registrar_no_servico_referencia()
pedir_lista_servidores()

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

    print(f'[CLOCK] contador servidor={contador_server}', flush=True)

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
            
    elif tipo == 'election':
        print('[ELEICAO] Recebi pedido de eleição', flush=True)

        resposta = {
            'status': 'ok',
            'message': 'OK',
            'timestamp': agora_corrigido()
        }

    else:
        resposta = {
            'status': 'error',
            'message': 'tipo inválido',
            'timestamp': agora_corrigido()
        }

    resposta['contador'] = proximo_contador()
    resposta['coordenador'] = coordenador

    print('[SERVER] Resposta:', resposta, flush=True)
    socket_rep.send(msgpack.packb(resposta, use_bin_type=True))

    if contador_requisicoes % INTERVALO_SINCRONIZACAO == 0:
        parte4_a_cada_15_mensagens()