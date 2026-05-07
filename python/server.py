import json
import time
import threading
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

# Socket direto para eleição/berkeley (bind na porta fixa)
socket_server = context.socket(zmq.REP)
socket_server.bind('tcp://*:5571')

# Socket SUB para ouvir anúncios de coordenador
socket_sub_servers = context.socket(zmq.SUB)
socket_sub_servers.connect('tcp://proxy:5558')
socket_sub_servers.setsockopt_string(zmq.SUBSCRIBE, 'servers')

nome_servidor = 'server_python'

PORTA_SERVIDORES = {
    'server_c':      'tcp://server_c:5570',
    'server_python': 'tcp://server_python:5571',
    'server_java':   'tcp://server_java:5572',
}

PORTA_REPLICACAO = {
    'server_c':      'tcp://server_c:5580',
    'server_python': 'tcp://server_python:5581',
    'server_java':   'tcp://server_java:5582',
}
MINHA_PORTA_REPLICACAO = 5581

RANK_SERVIDOR = {
    'server_c':      1,
    'server_python': 2,
    'server_java':   3,
}

pasta_dados = Path('data')
pasta_dados.mkdir(exist_ok=True)

pasta_compartilhada = Path('/app/shared')
pasta_compartilhada.mkdir(exist_ok=True)

arquivo_canais      = pasta_compartilhada / 'channels.json'
arquivo_coordenador = pasta_compartilhada / 'coordenador.json'
arquivo_logins      = pasta_dados / 'logins.json'
arquivo_requisicoes = pasta_dados / 'requests.jsonl'
arquivo_publicacoes = pasta_dados / 'publications.jsonl'

contador_server      = 0
contador_requisicoes = 0
offset_relogio       = 0.0

rank        = None
coordenador = ''
servidores_ativos = []

INTERVALO_SINCRONIZACAO = 15

# Locks
eleicao_lock   = threading.Lock()
replicacao_lock = threading.Lock()
ref_lock        = threading.Lock()   # protege uso do ref_socket entre threads

# ---------------------------------------------------------------------------
# Utilitários de arquivo
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Relógio
# ---------------------------------------------------------------------------

def agora_corrigido():
    return time.time() + offset_relogio


def atualizar_contador_recebido(cont_recebido):
    global contador_server
    contador_server = max(contador_server, cont_recebido)


def proximo_contador():
    global contador_server
    contador_server += 1
    return contador_server


# ---------------------------------------------------------------------------
# Canais / logins em memória
# ---------------------------------------------------------------------------

def recarregar_canais():
    global canais
    canais = ler_json(arquivo_canais, [])


def recarregar_logins():
    global logins
    logins = ler_json(arquivo_logins, [])


# ---------------------------------------------------------------------------
# Serviço de referência  (protegido por ref_lock)
# ---------------------------------------------------------------------------

def registrar_no_servico_referencia():
    global rank
    with ref_lock:
        ref_socket.send_json({'type': 'register', 'name': nome_servidor})
        resposta = ref_socket.recv_json()
    rank = resposta.get('rank')
    print(f'[SERVER PYTHON] Meu rank: {rank}', flush=True)


def pedir_lista_servidores():
    global servidores_ativos
    with ref_lock:
        ref_socket.send_json({'type': 'list'})
        resposta = ref_socket.recv_json()
    if isinstance(resposta, list):
        servidores_ativos = resposta
    else:
        servidores_ativos = []
    print('[SERVIDORES ATIVOS]', flush=True)
    for s in servidores_ativos:
        print(f" - {s.get('name')} (rank={s.get('rank')})", flush=True)
    return servidores_ativos


def enviar_heartbeat():
    with ref_lock:
        ref_socket.send_json({'type': 'heartbeat', 'name': nome_servidor})
        resposta = ref_socket.recv_json()
    print(f'[HEARTBEAT] resposta={resposta}', flush=True)


# ---------------------------------------------------------------------------
# Coordenador / eleição
# ---------------------------------------------------------------------------

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


def servidor_responde_eleicao(nome):
    """Testa se um servidor responde à mensagem de eleição."""
    porta = PORTA_SERVIDORES.get(nome)
    if not porta:
        return False
    sock = context.socket(zmq.REQ)
    try:
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(porta)
        sock.send_json({'type': 'election'})
        resp = sock.recv_json()
        sock.close()
        return resp.get('status') == 'ok'
    except Exception:
        try:
            sock.close()
        except Exception:
            pass
        return False


def eleger_coordenador():
    """
    Eleição com lock global para evitar dupla eleição.
    Estratégia: o servidor com menor rank entre os vivos vence.
    Publica resultado via PUB para todos atualizarem.
    """
    global coordenador

    acquired = eleicao_lock.acquire(blocking=True, timeout=3)
    if not acquired:
        return  # outra eleição já em andamento

    try:
        print('[ELEICAO] Iniciando eleição...', flush=True)
        servidores = pedir_lista_servidores()
        if not servidores:
            print('[ELEICAO] Não há servidores ativos.', flush=True)
            return

        # Testa quais servidores respondem
        servidores_vivos = []
        for servidor in servidores:
            nome = servidor.get('name')
            if servidor_responde_eleicao(nome):
                servidores_vivos.append(servidor)
                print(f'[ELEICAO] {nome} respondeu OK', flush=True)
            else:
                print(f'[ELEICAO] {nome} não respondeu', flush=True)

        if not servidores_vivos:
            servidores_vivos = servidores

        # Escolhe o de menor rank
        servidor_eleito = min(servidores_vivos, key=lambda s: s.get('rank', 999999))
        novo_coordenador = servidor_eleito.get('name')

        coordenador = novo_coordenador
        salvar_coordenador(coordenador)

        # Só publica se EU sou o de menor rank entre os vivos
        # Assim apenas um servidor publica o resultado
        meu_rank = RANK_SERVIDOR.get(nome_servidor, 999)
        menor_rank_vivo = min(s.get('rank', 999) for s in servidores_vivos)
        if meu_rank == menor_rank_vivo:
            publicar_coordenador(coordenador)
            print(f'[ELEICAO] Coordenador escolhido: {coordenador}', flush=True)
        else:
            print(f'[ELEICAO] Aguardando anúncio do coordenador: {coordenador}', flush=True)

    finally:
        eleicao_lock.release()


def sou_coordenador():
    return coordenador == nome_servidor


# ---------------------------------------------------------------------------
# Thread: escuta anúncios de coordenador via PUB/SUB
# ---------------------------------------------------------------------------

def thread_sub_servers():
    """Recebe anúncios de coordenador publicados por outros servidores."""
    global coordenador
    while True:
        try:
            partes = socket_sub_servers.recv_multipart()
            if len(partes) < 2:
                continue
            dados = msgpack.unpackb(partes[1], raw=False)
            if dados.get('type') == 'coordinator_announce':
                novo = dados.get('coordinator', '')
                if novo and novo != coordenador:
                    print(f'[SUB] Coordenador atualizado via anúncio: {novo}', flush=True)
                    coordenador = novo
                    salvar_coordenador(coordenador)
        except Exception as e:
            print(f'[ERRO SUB SERVERS] {e}', flush=True)


# ---------------------------------------------------------------------------
# Berkeley
# ---------------------------------------------------------------------------

def sincronizar_berkeley():
    global offset_relogio
    if not coordenador:
        return
    if coordenador == nome_servidor:
        print(f'[BERKELEY] Sou coordenador ({nome_servidor})', flush=True)
        return
    porta = PORTA_SERVIDORES.get(coordenador)
    if not porta:
        return
    try:
        sock = context.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, 2000)
        sock.connect(porta)
        sock.send_json({'type': 'get_time'})
        resposta = sock.recv_json()
        sock.close()
        if 'time' in resposta:
            hora_correta = resposta['time']
            offset_relogio = hora_correta - time.time()
            print(f'[BERKELEY] Ajustando relógio. Offset={offset_relogio:.6f}', flush=True)
    except Exception as e:
        print(f'[ERRO BERKELEY] {e}', flush=True)


# ---------------------------------------------------------------------------
# Replicação Passiva (Primary-Backup)
# ---------------------------------------------------------------------------

def aplicar_replicacao(evento):
    with replicacao_lock:
        tipo = evento.get('type')

        if tipo == 'replicate_login':
            novo_login = {'user': evento['user'], 'timestamp': evento['timestamp']}
            logins_locais = ler_json(arquivo_logins, [])
            logins_locais.append(novo_login)
            salvar_json(arquivo_logins, logins_locais)
            print(f'[REPLICA] login aplicado: {evento["user"]}', flush=True)

        elif tipo == 'replicate_channel':
            canais_locais = ler_json(arquivo_canais, [])
            canal = evento['channel']
            if canal not in canais_locais:
                canais_locais.append(canal)
                salvar_json(arquivo_canais, canais_locais)
                print(f'[REPLICA] canal aplicado: {canal}', flush=True)

        elif tipo == 'replicate_publish':
            # Usa o contador do primário para manter consistência
            atualizar_contador_recebido(evento.get('contador', 0))
            publicacao = {
                'channel':             evento['channel'],
                'user':                evento['user'],
                'message':             evento['message'],
                'request_timestamp':   evento['request_timestamp'],
                'published_timestamp': evento['published_timestamp'],
                'contador':            evento['contador'],
            }
            salvar_jsonl(arquivo_publicacoes, publicacao)
            print(f'[REPLICA] publicação aplicada: canal={evento["channel"]}', flush=True)


def replicar_para_backups(evento):
    for nome, porta in PORTA_REPLICACAO.items():
        if nome == nome_servidor:
            continue
        sock = context.socket(zmq.REQ)
        try:
            sock.setsockopt(zmq.RCVTIMEO, 2000)
            sock.connect(porta)
            sock.send(msgpack.packb(evento, use_bin_type=True))
            ack = sock.recv()
            print(f'[REPLICA] ACK de {nome}: ok', flush=True)
        except Exception as e:
            print(f'[REPLICA] Falha ao replicar para {nome}: {e}', flush=True)
        finally:
            try:
                sock.close()
            except Exception:
                pass


def encaminhar_para_primario(mensagem_raw):
    """
    Backup encaminha escrita para o primário.
    Se falhar, limpa coordenador e dispara eleição. Retorna None em falha.
    """
    global coordenador

    porta = PORTA_REPLICACAO.get(coordenador)
    if not porta:
        return None
    sock = context.socket(zmq.REQ)
    try:
        sock.setsockopt(zmq.RCVTIMEO, 3000)
        sock.connect(porta)
        sock.send(mensagem_raw)
        dados = sock.recv()
        return msgpack.unpackb(dados, raw=False)
    except Exception as e:
        print(f'[ENCAMINHAR] Primário {coordenador} não respondeu: {e}. Eleição iniciada.', flush=True)
        coordenador = ''
        threading.Thread(target=eleger_coordenador, daemon=True).start()
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass


def thread_replicacao():
    sock_rep = context.socket(zmq.REP)
    sock_rep.bind(f'tcp://*:{MINHA_PORTA_REPLICACAO}')
    print(f'[REPLICA] Thread de replicação escutando na porta {MINHA_PORTA_REPLICACAO}', flush=True)

    while True:
        try:
            dados = sock_rep.recv()
            evento = msgpack.unpackb(dados, raw=False)
            tipo = evento.get('type', '')

            if tipo in ('login', 'create_channel', 'publish_message'):
                # Backup encaminhou escrita → processo como primário
                resposta = processar_escrita(evento)
                sock_rep.send(msgpack.packb(resposta, use_bin_type=True))

            elif tipo.startswith('replicate_'):
                aplicar_replicacao(evento)
                sock_rep.send_json({'status': 'ok'})

            else:
                sock_rep.send_json({'status': 'error', 'message': 'tipo desconhecido'})

        except Exception as e:
            print(f'[ERRO REPLICA] {e}', flush=True)
            try:
                sock_rep.send_json({'status': 'error', 'message': str(e)})
            except Exception:
                pass


def processar_escrita(mensagem):
    tipo    = mensagem.get('type')
    usuario = mensagem.get('user', '')
    canal   = mensagem.get('channel', '').strip()
    texto   = mensagem.get('message', '')
    timestamp_req = mensagem.get('timestamp', agora_corrigido())

    recarregar_canais()
    recarregar_logins()

    with replicacao_lock:
        if tipo == 'login':
            novo_login = {'user': usuario, 'timestamp': timestamp_req}
            logins.append(novo_login)
            salvar_json(arquivo_logins, logins)
            evento = {'type': 'replicate_login', 'user': usuario, 'timestamp': timestamp_req}
            threading.Thread(target=replicar_para_backups, args=(evento,), daemon=True).start()
            return {
                'status': 'ok',
                'message': f'login realizado ({usuario})',
                'timestamp': agora_corrigido(),
                'contador': proximo_contador(),
                'coordenador': coordenador,
            }

        elif tipo == 'create_channel':
            if not canal:
                return {'status': 'error', 'message': 'nome inválido',
                        'timestamp': agora_corrigido(), 'contador': proximo_contador(),
                        'coordenador': coordenador}
            if canal in canais:
                return {'status': 'error', 'message': 'já existe',
                        'timestamp': agora_corrigido(), 'contador': proximo_contador(),
                        'coordenador': coordenador}
            canais.append(canal)
            salvar_json(arquivo_canais, canais)
            evento = {'type': 'replicate_channel', 'channel': canal}
            threading.Thread(target=replicar_para_backups, args=(evento,), daemon=True).start()
            return {
                'status': 'ok',
                'message': f"canal '{canal}' criado",
                'timestamp': agora_corrigido(),
                'contador': proximo_contador(),
                'coordenador': coordenador,
            }

        elif tipo == 'publish_message':
            if canal not in canais:
                return {'status': 'error', 'message': 'canal inexistente',
                        'timestamp': agora_corrigido(), 'contador': proximo_contador(),
                        'coordenador': coordenador}

            contador_pub  = proximo_contador()
            published_ts  = agora_corrigido()
            publicacao = {
                'channel': canal, 'user': usuario, 'message': texto,
                'request_timestamp': timestamp_req,
                'published_timestamp': published_ts,
                'contador': contador_pub,
            }
            socket_pub.send_multipart([
                canal.encode('utf-8'),
                msgpack.packb(publicacao, use_bin_type=True)
            ])
            salvar_jsonl(arquivo_publicacoes, publicacao)
            evento = {'type': 'replicate_publish', **publicacao}
            threading.Thread(target=replicar_para_backups, args=(evento,), daemon=True).start()
            return {
                'status': 'ok',
                'message': f"mensagem publicada em '{canal}'",
                'timestamp': agora_corrigido(),
                'contador': proximo_contador(),
                'coordenador': coordenador,
            }

        return {'status': 'error', 'message': 'tipo inválido',
                'timestamp': agora_corrigido(), 'contador': proximo_contador(),
                'coordenador': coordenador}


# ---------------------------------------------------------------------------
# Thread eleição/berkeley (porta direta)
# ---------------------------------------------------------------------------

def thread_servidor_direto():
    while True:
        try:
            msg = socket_server.recv_json()
            tipo = msg.get('type')
            if tipo == 'election':
                socket_server.send_json({'status': 'ok'})
            elif tipo == 'get_time':
                socket_server.send_json({'time': agora_corrigido()})
            else:
                socket_server.send_json({'status': 'error'})
        except Exception as e:
            print(f'[ERRO SERVER DIRETO] {e}', flush=True)


def parte4_a_cada_15_mensagens():
    global coordenador
    enviar_heartbeat()
    # Verifica se o coordenador atual ainda responde
    if coordenador and not servidor_responde_eleicao(coordenador):
        print(f'[FALHA] Coordenador {coordenador} não responde. Iniciando eleição.', flush=True)
        coordenador = ''
        eleger_coordenador()
    elif coordenador:
        print(f'[OK] Coordenador ainda ativo: {coordenador}', flush=True)
        sincronizar_berkeley()
    else:
        eleger_coordenador()
        sincronizar_berkeley()


# ---------------------------------------------------------------------------
# Inicialização
# ---------------------------------------------------------------------------

canais = ler_json(arquivo_canais, [])
logins = ler_json(arquivo_logins, [])

print('[SERVER PYTHON] Iniciado...', flush=True)

registrar_no_servico_referencia()
pedir_lista_servidores()
socket_rep.setsockopt(zmq.RCVTIMEO, 1000)

threading.Thread(target=thread_servidor_direto, daemon=True).start()
threading.Thread(target=thread_replicacao,      daemon=True).start()
threading.Thread(target=thread_sub_servers,     daemon=True).start()

# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

while True:
    try:
        mensagem_bruta = socket_rep.recv()
    except zmq.Again:
        continue

    mensagem = msgpack.unpackb(mensagem_bruta, raw=False)

    recarregar_canais()
    recarregar_logins()

    contador_requisicoes += 1

    tipo     = mensagem.get('type')
    usuario  = mensagem.get('user', '')
    contador_recebido = mensagem.get('contador', 0)

    atualizar_contador_recebido(contador_recebido)

    print(f'[CLOCK] contador servidor={contador_server}', flush=True)

    timestamp_recebimento = agora_corrigido()

    salvar_jsonl(arquivo_requisicoes, {
        'type':               tipo,
        'user':               usuario,
        'request':            mensagem,
        'received_timestamp': timestamp_recebimento
    })

    if tipo in ('login', 'create_channel', 'publish_message'):
        if sou_coordenador() or not coordenador:
            resposta = processar_escrita(mensagem)
        else:
            print(f'[BACKUP] Encaminhando {tipo} para primário {coordenador}', flush=True)
            resposta = encaminhar_para_primario(mensagem_bruta)
            if resposta is None:
                resposta = {
                    'status': 'error',
                    'message': 'primário indisponível',
                    'timestamp': agora_corrigido(),
                    'contador': proximo_contador(),
                    'coordenador': coordenador,
                }

    elif tipo == 'list_channels':
        resposta = {
            'status':      'ok',
            'channels':    canais,
            'timestamp':   agora_corrigido(),
            'contador':    proximo_contador(),
            'coordenador': coordenador,
        }

    else:
        resposta = {
            'status':      'error',
            'message':     'tipo inválido',
            'timestamp':   agora_corrigido(),
            'contador':    proximo_contador(),
            'coordenador': coordenador,
        }

    print('[SERVER] Resposta:', resposta, flush=True)
    socket_rep.send(msgpack.packb(resposta, use_bin_type=True))

    if contador_requisicoes % INTERVALO_SINCRONIZACAO == 0:
        parte4_a_cada_15_mensagens()