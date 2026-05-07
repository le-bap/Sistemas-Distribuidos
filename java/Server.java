import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.zeromq.ZMQ;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Server {
    static final String NOME_SERVIDOR = "server_java";

    static final String PASTA_DADOS = "data";

    static final String ARQUIVO_CANAIS = "data/channels.json";
    static final String ARQUIVO_LOGINS = "data/logins.json";
    static final String ARQUIVO_REQUISICOES = "data/requests.jsonl";
    static final String ARQUIVO_PUBLICACOES = "data/publications.jsonl";
    static final String ARQUIVO_REPLICADOS = "data/replicated_events.jsonl";
    static final String ARQUIVO_EVENTOS_APLICADOS = "data/applied_events.json";

    static final int PORTA_DIRETA = 5572;

    static final int INTERVALO_HEARTBEAT = 10;
    static final int INTERVALO_BERKELEY = 15;

    static final Map<String, String> PORTAS = new HashMap<>();

    static {
        PORTAS.put("server_c", "tcp://server_c:5570");
        PORTAS.put("server_python", "tcp://server_python:5571");
        PORTAS.put("server_java", "tcp://server_java:5572");
    }

    static List<String> canais = new ArrayList<>();
    static List<String> logins = new ArrayList<>();
    static Set<String> eventosAplicados = new HashSet<>();

    static int contadorServidor = 0;
    static int contadorRequisicoes = 0;
    static double offsetRelogio = 0.0;
    static int rankServidor = 0;
    static String coordenador = "";

    static final Object lockEstado = new Object();

    static ZMQ.Context context;
    static ZMQ.Socket pub;
    static ZMQ.Socket ref;


    public static void main(String[] args) throws Exception {
        new File(PASTA_DADOS).mkdirs();

        carregarEstado();

        context = ZMQ.context(1);

        ZMQ.Socket rep = context.socket(ZMQ.REP);
        rep.connect("tcp://broker:5556");

        pub = context.socket(ZMQ.PUB);
        pub.connect("tcp://proxy:5557");

        ref = context.socket(ZMQ.REQ);
        ref.connect("tcp://referencia:5560");

        ZMQ.Socket subReplication = context.socket(ZMQ.SUB);
        subReplication.connect("tcp://proxy:5558");
        subReplication.subscribe("replication".getBytes());

        System.out.println("[SERVER JAVA] Iniciado...");

        registrarNaReferencia();

        iniciarThreadServidorDireto();
        iniciarThreadReplicacao(subReplication);

        while (true) {
            byte[] mensagemBruta = rep.recv();
            if (mensagemBruta == null) {
                continue;
            }

            contadorRequisicoes++;

            synchronized (lockEstado) {
                carregarEstado();
            }

            Map<String, Object> mensagem = desempacotarMapa(mensagemBruta);

            String tipo = getString(mensagem, "type");
            String usuario = getString(mensagem, "user");
            String canal = getString(mensagem, "channel").trim();
            String texto = getString(mensagem, "message");
            double timestamp = getDouble(mensagem, "timestamp", agoraCorrigido());
            int contadorRecebido = getInt(mensagem, "contador", 0);

            atualizarContadorRecebido(contadorRecebido);

            salvarLinhaJson(
                ARQUIVO_REQUISICOES,
                "{\"type\":\"" + escape(tipo) + "\",\"user\":\"" + escape(usuario) +
                    "\",\"received_timestamp\":" + agoraCorrigido() +
                    ",\"contador\":" + contadorServidor + "}"
            );

            byte[] resposta;

            if (tipo.equals("login")) {
                String loginJson = "{\"user\":\"" + escape(usuario) + "\",\"timestamp\":" + timestamp + "}";

                synchronized (lockEstado) {
                    logins.add(loginJson);
                    salvarLogins();
                }

                Map<String, Object> dados = new LinkedHashMap<>();
                dados.put("user", usuario);
                dados.put("timestamp", timestamp);

                publicarReplicacao("login", dados);

                resposta = empacotarResposta("ok", "login realizado (" + usuario + ")");

            } else if (tipo.equals("create_channel")) {
                if (canal.isEmpty()) {
                    resposta = empacotarResposta("error", "nome de canal inválido");

                } else if (canais.contains(canal)) {
                    resposta = empacotarResposta("error", "canal já existe");

                } else {
                    synchronized (lockEstado) {
                        canais.add(canal);
                        salvarCanais();
                    }

                    Map<String, Object> dados = new LinkedHashMap<>();
                    dados.put("channel", canal);

                    publicarReplicacao("create_channel", dados);

                    resposta = empacotarResposta("ok", "canal '" + canal + "' criado");
                }

            } else if (tipo.equals("list_channels")) {
                resposta = empacotarListaCanais();

            } else if (tipo.equals("publish_message")) {
                if (!canais.contains(canal)) {
                    resposta = empacotarResposta("error", "canal inexistente");

                } else {
                    double publishedTimestamp = agoraCorrigido();
                    int contadorPub = proximoContador();

                    Map<String, Object> publicacao = new LinkedHashMap<>();
                    publicacao.put("channel", canal);
                    publicacao.put("user", usuario);
                    publicacao.put("message", texto);
                    publicacao.put("request_timestamp", timestamp);
                    publicacao.put("published_timestamp", publishedTimestamp);
                    publicacao.put("contador", contadorPub);

                    byte[] publicacaoMsgpack = empacotarMapaGenerico(publicacao);

                    pub.sendMore(canal);
                    pub.send(publicacaoMsgpack);

                    salvarLinhaJson(
                        ARQUIVO_PUBLICACOES,
                        "{\"channel\":\"" + escape(canal) + "\",\"user\":\"" + escape(usuario) +
                            "\",\"message\":\"" + escape(texto) +
                            "\",\"request_timestamp\":" + timestamp +
                            ",\"published_timestamp\":" + publishedTimestamp +
                            ",\"contador\":" + contadorPub + "}"
                    );

                    publicarReplicacao("publish_message", publicacao);

                    resposta = empacotarResposta("ok", "mensagem publicada em '" + canal + "'");
                }

            } else {
                resposta = empacotarResposta("error", "tipo inválido");
            }

            rep.send(resposta);

            System.out.println(
                "[SERVER JAVA] tipo=" + tipo +
                    " | user=" + usuario +
                    " | canal=" + canal +
                    " | contador=" + contadorServidor +
                    " | coordenador=" + coordenador
            );

            if (contadorRequisicoes % INTERVALO_HEARTBEAT == 0) {
                enviarHeartbeat();
            }

            if (contadorRequisicoes % INTERVALO_BERKELEY == 0) {
                verificarEleicaoEBerkeley();
            }
        }
    }


    // ============================================================
    // THREAD DIRETA
    // ============================================================

    static void iniciarThreadServidorDireto() {
        new Thread(() -> {
            ZMQ.Socket srv = context.socket(ZMQ.REP);
            srv.bind("tcp://*:" + PORTA_DIRETA);

            System.out.println("[SERVER JAVA] Thread direta escutando na porta " + PORTA_DIRETA);

            while (true) {
                try {
                    String msg = srv.recvStr();

                    if (msg == null) {
                        continue;
                    }

                    if (msg.contains("\"election\"")) {
                        srv.send("{\"status\":\"ok\"}");

                    } else if (msg.contains("\"get_time\"")) {
                        srv.send("{\"time\":" + agoraCorrigido() + "}");

                    } else {
                        srv.send("{\"status\":\"error\"}");
                    }

                } catch (Exception e) {
                    System.out.println("[ERRO THREAD DIRETA] " + e.getMessage());
                }
            }
        }, "server-direto").start();
    }


    // ============================================================
    // THREAD REPLICAÇÃO
    // ============================================================
static void iniciarThreadReplicacao(ZMQ.Socket subReplication) {
    new Thread(() -> {
        while (true) {
            try {
                byte[] topico = subReplication.recv(0);

                if (topico == null) {
                    continue;
                }

                byte[] payload = subReplication.recv(0);

                if (payload == null) {
                    continue;
                }

                Map<String, Object> evento = desempacotarMapa(payload);

                int contadorRecebido = getInt(evento, "contador", 0);
                atualizarContadorRecebido(contadorRecebido);

                aplicarEventoReplicado(evento);

            } catch (Exception e) {
                System.out.println("[ERRO REPLICACAO] " + e.getMessage());
            }
        }
    }, "replicacao").start();
}

    static void aplicarEventoReplicado(Map<String, Object> evento) throws Exception {
        String eventId = getString(evento, "event_id");
        String origem = getString(evento, "origin");
        String operacao = getString(evento, "operation");

        if (eventId.isEmpty()) {
            return;
        }

        if (origem.equals(NOME_SERVIDOR)) {
            return;
        }

        synchronized (lockEstado) {
            if (eventosAplicados.contains(eventId)) {
                return;
            }

            eventosAplicados.add(eventId);

            Object dadosObj = evento.get("data");

            if (!(dadosObj instanceof Map)) {
                salvarEventosAplicados();
                return;
            }

            Map<String, Object> dados = (Map<String, Object>) dadosObj;

            if (operacao.equals("login")) {
                String usuario = getString(dados, "user");
                double timestamp = getDouble(dados, "timestamp", agoraCorrigido());

                logins.add("{\"user\":\"" + escape(usuario) + "\",\"timestamp\":" + timestamp + "}");
                salvarLogins();

            } else if (operacao.equals("create_channel")) {
                String canal = getString(dados, "channel").trim();

                if (!canal.isEmpty() && !canais.contains(canal)) {
                    canais.add(canal);
                    salvarCanais();
                }

            } else if (operacao.equals("publish_message")) {
                String canal = getString(dados, "channel");
                String usuario = getString(dados, "user");
                String texto = getString(dados, "message");
                double requestTimestamp = getDouble(dados, "request_timestamp", agoraCorrigido());
                double publishedTimestamp = getDouble(dados, "published_timestamp", agoraCorrigido());
                int contador = getInt(dados, "contador", 0);

                salvarLinhaJson(
                    ARQUIVO_PUBLICACOES,
                    "{\"channel\":\"" + escape(canal) +
                        "\",\"user\":\"" + escape(usuario) +
                        "\",\"message\":\"" + escape(texto) +
                        "\",\"request_timestamp\":" + requestTimestamp +
                        ",\"published_timestamp\":" + publishedTimestamp +
                        ",\"contador\":" + contador + "}"
                );
            }

            salvarEventosAplicados();
        }

        System.out.println("[REPLICACAO] Evento aplicado: " + operacao + " de " + origem);
    }

    static void publicarReplicacao(String operacao, Map<String, Object> dados) throws Exception {
        String eventId = NOME_SERVIDOR + "-" + UUID.randomUUID();

        Map<String, Object> evento = new LinkedHashMap<>();
        evento.put("type", "replication");
        evento.put("event_id", eventId);
        evento.put("origin", NOME_SERVIDOR);
        evento.put("operation", operacao);
        evento.put("timestamp", agoraCorrigido());
        evento.put("contador", proximoContador());
        evento.put("data", dados);

        synchronized (lockEstado) {
            eventosAplicados.add(eventId);
            salvarEventosAplicados();
        }

        pub.sendMore("replication");
        pub.send(empacotarMapaGenerico(evento));

        salvarLinhaJson(
            ARQUIVO_REPLICADOS,
            "{\"event_id\":\"" + eventId + "\",\"origin\":\"" + NOME_SERVIDOR +
                "\",\"operation\":\"" + operacao + "\"}"
        );
    }


    // ============================================================
    // REFERÊNCIA
    // ============================================================

    static void registrarNaReferencia() {
        String json = "{\"type\":\"register\",\"name\":\"" + NOME_SERVIDOR + "\"}";
        ref.send(json);

        String resposta = ref.recvStr();

        int idx = resposta.indexOf("\"rank\":");

        if (idx >= 0) {
            String resto = resposta.substring(idx + 7).replaceAll("[^0-9]", "");

            if (!resto.isEmpty()) {
                rankServidor = Integer.parseInt(resto);
            }
        }

        System.out.println("[SERVER JAVA] Meu rank: " + rankServidor);
    }

    static void enviarHeartbeat() {
        String json = "{\"type\":\"heartbeat\",\"name\":\"" + NOME_SERVIDOR + "\"}";
        ref.send(json);

        String resposta = ref.recvStr();
        System.out.println("[HEARTBEAT] resposta=" + resposta);
    }

    static List<ServidorInfo> pedirListaServidores() {
        ref.send("{\"type\":\"list\"}");
        String lista = ref.recvStr();

        List<ServidorInfo> servidores = extrairServidores(lista);

        System.out.println("[SERVIDORES ATIVOS]");

        for (ServidorInfo s : servidores) {
            System.out.println(" - " + s.nome + " (rank=" + s.rank + ")");
        }

        return servidores;
    }


    // ============================================================
    // ELEIÇÃO E BERKELEY
    // ============================================================

    static boolean coordenadorEstaVivo() {
        if (coordenador.equals("")) {
            return false;
        }

        String porta = PORTAS.get(coordenador);

        if (porta == null) {
            return false;
        }

        ZMQ.Socket sock = context.socket(ZMQ.REQ);
        sock.setReceiveTimeOut(1000);

        try {
            sock.connect(porta);
            sock.send("{\"type\":\"election\"}");

            String resp = sock.recvStr();

            return resp != null && resp.contains("ok");

        } catch (Exception e) {
            return false;

        } finally {
            sock.close();
        }
    }

    static void verificarEleicaoEBerkeley() throws Exception {
        if (!coordenadorEstaVivo()) {
            if (!coordenador.equals("")) {
                System.out.println("[FALHA] Coordenador caiu: " + coordenador);
            }

            coordenador = "";
            iniciarEleicao();

        } else {
            System.out.println("[OK] Coordenador ainda ativo: " + coordenador);
        }

        sincronizarBerkeley();
    }

    static void iniciarEleicao() throws Exception {
        System.out.println("[ELEICAO] Iniciando eleição...");

        List<ServidorInfo> servidores = pedirListaServidores();
        List<ServidorInfo> vivos = new ArrayList<>();

        for (ServidorInfo s : servidores) {
            String porta = PORTAS.get(s.nome);

            if (porta == null) {
                continue;
            }

            ZMQ.Socket sock = context.socket(ZMQ.REQ);
            sock.setReceiveTimeOut(1000);

            try {
                sock.connect(porta);
                sock.send("{\"type\":\"election\"}");

                String resp = sock.recvStr();

                if (resp != null && resp.contains("ok")) {
                    vivos.add(s);
                    System.out.println("[ELEICAO] " + s.nome + " respondeu OK");
                }

            } catch (Exception e) {
                System.out.println("[ELEICAO] " + s.nome + " não respondeu");

            } finally {
                sock.close();
            }
        }

        if (vivos.isEmpty()) {
            vivos = servidores;
        }

        ServidorInfo eleito = vivos.stream()
            .min(Comparator.comparingInt(s -> s.rank))
            .orElse(null);

        if (eleito == null) {
            return;
        }

        coordenador = eleito.nome;

        publicarCoordenador(coordenador);

        System.out.println("[ELEICAO] Coordenador escolhido: " + coordenador);
    }

    static void publicarCoordenador(String eleito) throws Exception {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "coordinator_announce");
        msg.put("coordinator", eleito);
        msg.put("server", NOME_SERVIDOR);
        msg.put("timestamp", agoraCorrigido());
        msg.put("contador", proximoContador());

        pub.sendMore("servers");
        pub.send(empacotarMapaGenerico(msg));

        System.out.println("[PUB SERVERS] coordenador eleito: " + eleito);
    }

    static void sincronizarBerkeley() {
        if (coordenador.equals("")) {
            return;
        }

        if (coordenador.equals(NOME_SERVIDOR)) {
            System.out.println("[BERKELEY] Sou coordenador (" + NOME_SERVIDOR + "). Hora=" + agoraCorrigido());
            return;
        }

        String porta = PORTAS.get(coordenador);

        if (porta == null) {
            return;
        }

        ZMQ.Socket sock = context.socket(ZMQ.REQ);
        sock.setReceiveTimeOut(2000);

        try {
            sock.connect(porta);
            sock.send("{\"type\":\"get_time\"}");

            String resposta = sock.recvStr();

            if (resposta != null) {
                int idx = resposta.indexOf("\"time\":");

                if (idx >= 0) {
                    String resto = resposta.substring(idx + 7).replaceAll("[^0-9.]", "");

                    if (!resto.isEmpty()) {
                        double horaCorreta = Double.parseDouble(resto);
                        offsetRelogio = horaCorreta - agora();

                        System.out.println("[BERKELEY] Offset atualizado: " + offsetRelogio);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("[ERRO BERKELEY] " + e.getMessage());

        } finally {
            sock.close();
        }
    }


    // ============================================================
    // RELÓGIO
    // ============================================================

    static double agora() {
        return System.currentTimeMillis() / 1000.0;
    }

    static double agoraCorrigido() {
        return agora() + offsetRelogio;
    }

    static void atualizarContadorRecebido(int contadorRecebido) {
        contadorServidor = Math.max(contadorServidor, contadorRecebido);
    }

    static int proximoContador() {
        contadorServidor++;
        return contadorServidor;
    }


    // ============================================================
    // PERSISTÊNCIA
    // ============================================================

    static void carregarEstado() {
        carregarCanais();
        carregarLogins();
        carregarEventosAplicados();
    }

    static void carregarCanais() {
        canais.clear();

        try {
            if (Files.exists(Paths.get(ARQUIVO_CANAIS))) {
                String conteudo = Files.readString(Paths.get(ARQUIVO_CANAIS)).trim();
                conteudo = conteudo.replace("[", "").replace("]", "").replace("\"", "");

                if (!conteudo.isEmpty()) {
                    String[] partes = conteudo.split(",");

                    for (String parte : partes) {
                        String c = parte.trim();

                        if (!c.isEmpty()) {
                            canais.add(c);
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("[ERRO] ao carregar canais");
        }
    }

    static void carregarLogins() {
        logins.clear();

        try {
            if (Files.exists(Paths.get(ARQUIVO_LOGINS))) {
                List<String> linhas = Files.readAllLines(Paths.get(ARQUIVO_LOGINS));

                for (String linha : linhas) {
                    linha = linha.trim();

                    if (!linha.equals("[") && !linha.equals("]") && !linha.isEmpty()) {
                        if (linha.endsWith(",")) {
                            linha = linha.substring(0, linha.length() - 1);
                        }

                        logins.add(linha);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("[ERRO] ao carregar logins");
        }
    }

    static void carregarEventosAplicados() {
        eventosAplicados.clear();

        try {
            if (Files.exists(Paths.get(ARQUIVO_EVENTOS_APLICADOS))) {
                String conteudo = Files.readString(Paths.get(ARQUIVO_EVENTOS_APLICADOS));

                Matcher m = Pattern.compile("\"([^\"]+)\"").matcher(conteudo);

                while (m.find()) {
                    eventosAplicados.add(m.group(1));
                }
            }

        } catch (Exception e) {
            System.out.println("[ERRO] ao carregar eventos aplicados");
        }
    }

    static void salvarCanais() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(ARQUIVO_CANAIS));
        writer.write("[\n");

        for (int i = 0; i < canais.size(); i++) {
            writer.write("  \"" + escape(canais.get(i)) + "\"");

            if (i < canais.size() - 1) {
                writer.write(",");
            }

            writer.write("\n");
        }

        writer.write("]\n");
        writer.close();
    }

    static void salvarLogins() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(ARQUIVO_LOGINS));
        writer.write("[\n");

        for (int i = 0; i < logins.size(); i++) {
            writer.write("  " + logins.get(i));

            if (i < logins.size() - 1) {
                writer.write(",");
            }

            writer.write("\n");
        }

        writer.write("]\n");
        writer.close();
    }

    static void salvarEventosAplicados() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(ARQUIVO_EVENTOS_APLICADOS));
        writer.write("[\n");

        List<String> lista = new ArrayList<>(eventosAplicados);

        for (int i = 0; i < lista.size(); i++) {
            writer.write("  \"" + escape(lista.get(i)) + "\"");

            if (i < lista.size() - 1) {
                writer.write(",");
            }

            writer.write("\n");
        }

        writer.write("]\n");
        writer.close();
    }

    static void salvarLinhaJson(String arquivo, String linha) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(arquivo, true));
            writer.write(linha);
            writer.newLine();
            writer.close();

        } catch (Exception e) {
            System.out.println("[ERRO] ao salvar jsonl: " + e.getMessage());
        }
    }


    // ============================================================
    // MSGPACK
    // ============================================================

    static byte[] empacotarResposta(String status, String message) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(5);

        packer.packString("status");
        packer.packString(status);

        packer.packString("message");
        packer.packString(message);

        packer.packString("timestamp");
        packer.packDouble(agoraCorrigido());

        packer.packString("contador");
        packer.packInt(proximoContador());

        packer.packString("coordenador");
        packer.packString(coordenador);

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarListaCanais() throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(5);

        packer.packString("status");
        packer.packString("ok");

        packer.packString("channels");
        packer.packArrayHeader(canais.size());

        for (String canal : canais) {
            packer.packString(canal);
        }

        packer.packString("timestamp");
        packer.packDouble(agoraCorrigido());

        packer.packString("contador");
        packer.packInt(proximoContador());

        packer.packString("coordenador");
        packer.packString(coordenador);

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarMapaGenerico(Map<String, Object> mapa) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(mapa.size());

        for (Map.Entry<String, Object> entry : mapa.entrySet()) {
            packer.packString(entry.getKey());
            empacotarValor(packer, entry.getValue());
        }

        packer.close();
        return packer.toByteArray();
    }

    static void empacotarValor(MessageBufferPacker packer, Object valor) throws Exception {
        if (valor == null) {
            packer.packNil();

        } else if (valor instanceof String) {
            packer.packString((String) valor);

        } else if (valor instanceof Integer) {
            packer.packInt((Integer) valor);

        } else if (valor instanceof Long) {
            packer.packLong((Long) valor);

        } else if (valor instanceof Double) {
            packer.packDouble((Double) valor);

        } else if (valor instanceof Float) {
            packer.packFloat((Float) valor);

        } else if (valor instanceof Boolean) {
            packer.packBoolean((Boolean) valor);

        } else if (valor instanceof Map) {
            Map<String, Object> mapa = (Map<String, Object>) valor;

            packer.packMapHeader(mapa.size());

            for (Map.Entry<String, Object> entry : mapa.entrySet()) {
                packer.packString(entry.getKey());
                empacotarValor(packer, entry.getValue());
            }

        } else if (valor instanceof List) {
            List<?> lista = (List<?>) valor;

            packer.packArrayHeader(lista.size());

            for (Object item : lista) {
                empacotarValor(packer, item);
            }

        } else {
            packer.packString(valor.toString());
        }
    }

    static Map<String, Object> desempacotarMapa(byte[] dados) throws Exception {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(dados);
        int mapSize = unpacker.unpackMapHeader();

        Map<String, Object> mapa = new LinkedHashMap<>();

        for (int i = 0; i < mapSize; i++) {
            String key = unpacker.unpackString();
            Object valor = desempacotarValor(unpacker);
            mapa.put(key, valor);
        }

        unpacker.close();
        return mapa;
    }

    static Object desempacotarValor(MessageUnpacker unpacker) throws Exception {
        switch (unpacker.getNextFormat().getValueType()) {
            case NIL:
                unpacker.unpackNil();
                return null;

            case BOOLEAN:
                return unpacker.unpackBoolean();

            case INTEGER:
                return unpacker.unpackLong();

            case FLOAT:
                return unpacker.unpackDouble();

            case STRING:
                return unpacker.unpackString();

            case ARRAY:
                int arraySize = unpacker.unpackArrayHeader();
                List<Object> lista = new ArrayList<>();

                for (int i = 0; i < arraySize; i++) {
                    lista.add(desempacotarValor(unpacker));
                }

                return lista;

            case MAP:
                int mapSize = unpacker.unpackMapHeader();
                Map<String, Object> mapa = new LinkedHashMap<>();

                for (int i = 0; i < mapSize; i++) {
                    String key = unpacker.unpackString();
                    Object valor = desempacotarValor(unpacker);
                    mapa.put(key, valor);
                }

                return mapa;

            default:
                unpacker.skipValue();
                return null;
        }
    }


    // ============================================================
    // HELPERS
    // ============================================================

    static String getString(Map<String, Object> mapa, String chave) {
        Object valor = mapa.get(chave);

        if (valor == null) {
            return "";
        }

        return valor.toString();
    }

    static int getInt(Map<String, Object> mapa, String chave, int padrao) {
        Object valor = mapa.get(chave);

        if (valor instanceof Number) {
            return ((Number) valor).intValue();
        }

        try {
            return Integer.parseInt(String.valueOf(valor));
        } catch (Exception e) {
            return padrao;
        }
    }

    static double getDouble(Map<String, Object> mapa, String chave, double padrao) {
        Object valor = mapa.get(chave);

        if (valor instanceof Number) {
            return ((Number) valor).doubleValue();
        }

        try {
            return Double.parseDouble(String.valueOf(valor));
        } catch (Exception e) {
            return padrao;
        }
    }

    static String escape(String texto) {
        if (texto == null) {
            return "";
        }

        return texto.replace("\\", "\\\\").replace("\"", "'");
    }

    static List<ServidorInfo> extrairServidores(String lista) {
        List<ServidorInfo> servidores = new ArrayList<>();

        Pattern p = Pattern.compile("\\{\\s*\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*\\\"rank\\\"\\s*:\\s*(\\d+)\\s*\\}");
        Matcher m = p.matcher(lista);

        while (m.find()) {
            String nome = m.group(1);
            int rank = Integer.parseInt(m.group(2));

            servidores.add(new ServidorInfo(nome, rank));
        }

        return servidores;
    }

    static class ServidorInfo {
        String nome;
        int rank;

        ServidorInfo(String nome, int rank) {
            this.nome = nome;
            this.rank = rank;
        }
    }
}