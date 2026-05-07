import json
import time
import uuid
import threading
from pathlib import Path

import msgpack
import zmq


context = zmq.Context()

nome_servidor = "server_python"

BROKER_REP_ADDR = "tcp://broker:5556"
PROXY_XSUB_ADDR = "tcp://proxy:5557"
PROXY_XPUB_ADDR = "tcp://proxy:5558"
REFERENCIA_ADDR = "tcp://referencia:5560"

PORTA_DIRETA = "tcp://*:5571"

PORTA_SERVIDORES = {
    "server_c": "tcp://server_c:5570",
    "server_python": "tcp://server_python:5571",
    "server_java": "tcp://server_java:5572",
}

INTERVALO_HEARTBEAT = 10
INTERVALO_BERKELEY = 15


socket_rep = context.socket(zmq.REP)
socket_rep.connect(BROKER_REP_ADDR)
socket_rep.setsockopt(zmq.RCVTIMEO, 1000)

socket_pub = context.socket(zmq.PUB)
socket_pub.connect(PROXY_XSUB_ADDR)

socket_sub_replication = context.socket(zmq.SUB)
socket_sub_replication.connect(PROXY_XPUB_ADDR)
socket_sub_replication.setsockopt_string(zmq.SUBSCRIBE, "replication")

ref_socket = context.socket(zmq.REQ)
ref_socket.connect(REFERENCIA_ADDR)

socket_server = context.socket(zmq.REP)
socket_server.bind(PORTA_DIRETA)


pasta_dados = Path("data")
pasta_dados.mkdir(exist_ok=True)

arquivo_canais = pasta_dados / "channels.json"
arquivo_logins = pasta_dados / "logins.json"
arquivo_requisicoes = pasta_dados / "requests.jsonl"
arquivo_publicacoes = pasta_dados / "publications.jsonl"
arquivo_replicados = pasta_dados / "replicated_events.jsonl"
arquivo_eventos_aplicados = pasta_dados / "applied_events.json"


contador_server = 0
contador_requisicoes = 0
offset_relogio = 0.0

rank = None
coordenador = ""

canais = []
logins = []
eventos_aplicados = set()

lock_estado = threading.Lock()


def ler_json(caminho, valor_padrao):
    if not caminho.exists():
        return valor_padrao

    try:
        with open(caminho, "r", encoding="utf-8") as f:
            conteudo = f.read().strip()
            if not conteudo:
                return valor_padrao
            return json.loads(conteudo)
    except Exception:
        return valor_padrao


def salvar_json(caminho, dados):
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)


def salvar_jsonl(caminho, dados):
    with open(caminho, "a", encoding="utf-8") as f:
        f.write(json.dumps(dados, ensure_ascii=False) + "\n")


def carregar_estado():
    global canais, logins, eventos_aplicados

    canais = ler_json(arquivo_canais, [])
    logins = ler_json(arquivo_logins, [])
    eventos_aplicados = set(ler_json(arquivo_eventos_aplicados, []))


def salvar_eventos_aplicados():
    salvar_json(arquivo_eventos_aplicados, sorted(list(eventos_aplicados)))


def agora():
    return time.time()


def agora_corrigido():
    return time.time() + offset_relogio


def atualizar_contador_recebido(cont_recebido):
    global contador_server

    try:
        cont_recebido = int(cont_recebido)
    except Exception:
        cont_recebido = 0

    contador_server = max(contador_server, cont_recebido)


def proximo_contador():
    global contador_server
    contador_server += 1
    return contador_server


def registrar_no_servico_referencia():
    global rank

    ref_socket.send_json({
        "type": "register",
        "name": nome_servidor,
    })

    resposta = ref_socket.recv_json()
    rank = resposta.get("rank")

    print(f"[SERVER PYTHON] Meu rank: {rank}", flush=True)


def pedir_lista_servidores():
    ref_socket.send_json({"type": "list"})
    resposta = ref_socket.recv_json()

    if not isinstance(resposta, list):
        return []

    print("[SERVIDORES ATIVOS]", flush=True)
    for servidor in resposta:
        print(f" - {servidor.get('name')} (rank={servidor.get('rank')})", flush=True)

    return resposta


def enviar_heartbeat():
    ref_socket.send_json({
        "type": "heartbeat",
        "name": nome_servidor,
    })

    resposta = ref_socket.recv_json()
    print(f"[HEARTBEAT] resposta={resposta}", flush=True)


def coordenador_esta_vivo():
    global coordenador

    if not coordenador:
        return False

    porta = PORTA_SERVIDORES.get(coordenador)
    if not porta:
        coordenador = ""
        return False

    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 1000)

    try:
        sock.connect(porta)
        sock.send_json({"type": "election"})
        resp = sock.recv_json()
        return resp.get("status") == "ok"
    except Exception:
        coordenador = ""
        return False
    finally:
        sock.close()


def publicar_coordenador(nome_coordenador):
    mensagem = {
        "type": "coordinator_announce",
        "coordinator": nome_coordenador,
        "server": nome_servidor,
        "timestamp": agora_corrigido(),
        "contador": proximo_contador(),
    }

    socket_pub.send_multipart([
        b"servers",
        msgpack.packb(mensagem, use_bin_type=True),
    ])

    print(f"[PUB SERVERS] coordenador eleito: {nome_coordenador}", flush=True)


def eleger_coordenador():
    global coordenador

    print("[ELEICAO] Iniciando eleição...", flush=True)

    servidores = pedir_lista_servidores()

    if not servidores:
        print("[ELEICAO] Não há servidores ativos.", flush=True)
        return

    servidores_vivos = []

    for servidor in servidores:
        nome = servidor.get("name")
        porta = PORTA_SERVIDORES.get(nome)

        if not porta:
            continue

        sock = context.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, 1000)

        try:
            sock.connect(porta)
            sock.send_json({"type": "election"})
            resposta = sock.recv_json()

            if resposta.get("status") == "ok":
                servidores_vivos.append(servidor)
                print(f"[ELEICAO] {nome} respondeu OK", flush=True)

        except Exception:
            print(f"[ELEICAO] {nome} não respondeu", flush=True)

        finally:
            sock.close()

    if not servidores_vivos:
        servidores_vivos = servidores

    servidor_eleito = min(
        servidores_vivos,
        key=lambda s: s.get("rank", 999999)
    )

    coordenador = servidor_eleito.get("name")

    publicar_coordenador(coordenador)

    print(f"[ELEICAO] Coordenador escolhido: {coordenador}", flush=True)


def sincronizar_berkeley():
    global offset_relogio

    if not coordenador:
        return

    if coordenador == nome_servidor:
        print(
            f"[BERKELEY] Sou coordenador ({nome_servidor}). Hora={agora_corrigido()}",
            flush=True,
        )
        return

    porta = PORTA_SERVIDORES.get(coordenador)
    if not porta:
        return

    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 2000)

    try:
        sock.connect(porta)
        sock.send_json({"type": "get_time"})
        resposta = sock.recv_json()

        if "time" in resposta:
            hora_correta = resposta["time"]
            offset_relogio = hora_correta - time.time()
            print(f"[BERKELEY] Offset atualizado: {offset_relogio}", flush=True)

    except Exception as e:
        print(f"[ERRO BERKELEY] {e}", flush=True)

    finally:
        sock.close()


def verificar_eleicao_e_berkeley():
    global coordenador

    if not coordenador_esta_vivo():
        if coordenador:
            print(f"[FALHA] Coordenador caiu: {coordenador}", flush=True)

        coordenador = ""
        eleger_coordenador()
    else:
        print(f"[OK] Coordenador ainda ativo: {coordenador}", flush=True)

    sincronizar_berkeley()


def thread_servidor_direto():
    while True:
        try:
            msg = socket_server.recv_json()
            tipo = msg.get("type")

            if tipo == "election":
                socket_server.send_json({"status": "ok"})

            elif tipo == "get_time":
                socket_server.send_json({"time": agora_corrigido()})

            else:
                socket_server.send_json({"status": "error"})

        except Exception as e:
            print(f"[ERRO SERVER DIRETO] {e}", flush=True)


def gerar_event_id():
    return f"{nome_servidor}-{uuid.uuid4()}"


def publicar_replicacao(operacao, dados):
    evento = {
        "type": "replication",
        "event_id": gerar_event_id(),
        "origin": nome_servidor,
        "operation": operacao,
        "timestamp": agora_corrigido(),
        "contador": proximo_contador(),
        "data": dados,
    }

    with lock_estado:
        eventos_aplicados.add(evento["event_id"])
        salvar_eventos_aplicados()

    socket_pub.send_multipart([
        b"replication",
        msgpack.packb(evento, use_bin_type=True),
    ])

    salvar_jsonl(arquivo_replicados, evento)


def aplicar_evento_replicado(evento):
    event_id = evento.get("event_id")
    origem = evento.get("origin")
    operacao = evento.get("operation")
    dados = evento.get("data", {})

    if not event_id:
        return

    if origem == nome_servidor:
        return

    with lock_estado:
        if event_id in eventos_aplicados:
            return

        eventos_aplicados.add(event_id)

        if operacao == "login":
            usuario = dados.get("user", "")
            timestamp = dados.get("timestamp", agora_corrigido())

            logins.append({
                "user": usuario,
                "timestamp": timestamp,
            })
            salvar_json(arquivo_logins, logins)

        elif operacao == "create_channel":
            canal = dados.get("channel", "").strip()

            if canal and canal not in canais:
                canais.append(canal)
                salvar_json(arquivo_canais, canais)

        elif operacao == "publish_message":
            salvar_jsonl(arquivo_publicacoes, dados)

        salvar_eventos_aplicados()

    print(f"[REPLICACAO] Evento aplicado: {operacao} de {origem}", flush=True)


def thread_replicacao():
    while True:
        try:
            topico, payload = socket_sub_replication.recv_multipart()
            evento = msgpack.unpackb(payload, raw=False)

            contador_recebido = evento.get("contador", 0)
            atualizar_contador_recebido(contador_recebido)

            aplicar_evento_replicado(evento)

        except Exception as e:
            print(f"[ERRO REPLICACAO] {e}", flush=True)


def resposta_simples(status, message):
    return {
        "status": status,
        "message": message,
        "timestamp": agora_corrigido(),
        "contador": proximo_contador(),
        "coordenador": coordenador,
    }


def resposta_lista_canais():
    return {
        "status": "ok",
        "channels": canais,
        "timestamp": agora_corrigido(),
        "contador": proximo_contador(),
        "coordenador": coordenador,
    }


print("[SERVER PYTHON] Iniciado...", flush=True)

carregar_estado()
registrar_no_servico_referencia()
pedir_lista_servidores()

threading.Thread(target=thread_servidor_direto, daemon=True).start()
threading.Thread(target=thread_replicacao, daemon=True).start()

while True:
    try:
        mensagem_bruta = socket_rep.recv()
    except zmq.Again:
        continue

    try:
        mensagem = msgpack.unpackb(mensagem_bruta, raw=False)
    except Exception as e:
        print(f"[ERRO] Mensagem inválida: {e}", flush=True)
        continue

    with lock_estado:
        carregar_estado()

    contador_requisicoes += 1

    tipo = mensagem.get("type", "")
    usuario = mensagem.get("user", "")
    canal = mensagem.get("channel", "").strip()
    texto = mensagem.get("message", "")
    timestamp_msg = mensagem.get("timestamp", agora_corrigido())
    contador_recebido = mensagem.get("contador", 0)

    atualizar_contador_recebido(contador_recebido)

    timestamp_recebimento = agora_corrigido()

    salvar_jsonl(arquivo_requisicoes, {
        "type": tipo,
        "user": usuario,
        "request": mensagem,
        "received_timestamp": timestamp_recebimento,
        "contador": contador_server,
    })

    print(
        f"[SERVER PYTHON] tipo={tipo} | user={usuario} | canal={canal} | "
        f"contador={contador_server} | coordenador={coordenador}",
        flush=True,
    )

    if tipo == "login":
        with lock_estado:
            novo_login = {
                "user": usuario,
                "timestamp": timestamp_msg,
            }

            logins.append(novo_login)
            salvar_json(arquivo_logins, logins)

        publicar_replicacao("login", novo_login)

        resposta = resposta_simples("ok", f"login realizado ({usuario})")

    elif tipo == "create_channel":
        if not canal:
            resposta = resposta_simples("error", "nome de canal inválido")

        elif canal in canais:
            resposta = resposta_simples("error", "canal já existe")

        else:
            with lock_estado:
                canais.append(canal)
                salvar_json(arquivo_canais, canais)

            publicar_replicacao("create_channel", {
                "channel": canal,
            })

            resposta = resposta_simples("ok", f"canal '{canal}' criado")

    elif tipo == "list_channels":
        resposta = resposta_lista_canais()

    elif tipo == "publish_message":
        if canal not in canais:
            resposta = resposta_simples("error", "canal inexistente")

        else:
            contador_pub = proximo_contador()

            publicacao = {
                "channel": canal,
                "user": usuario,
                "message": texto,
                "request_timestamp": timestamp_msg,
                "published_timestamp": agora_corrigido(),
                "contador": contador_pub,
            }

            socket_pub.send_multipart([
                canal.encode("utf-8"),
                msgpack.packb(publicacao, use_bin_type=True),
            ])

            salvar_jsonl(arquivo_publicacoes, publicacao)
            publicar_replicacao("publish_message", publicacao)

            resposta = resposta_simples("ok", f"mensagem publicada em '{canal}'")

    else:
        resposta = resposta_simples("error", "tipo inválido")

    socket_rep.send(msgpack.packb(resposta, use_bin_type=True))

    if contador_requisicoes % INTERVALO_HEARTBEAT == 0:
        enviar_heartbeat()

    if contador_requisicoes % INTERVALO_BERKELEY == 0:
        verificar_eleicao_e_berkeley()