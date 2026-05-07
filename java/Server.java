import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.zeromq.ZMQ;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Server {
    static List<String> canais = new ArrayList<>();
    static List<String> logins = new ArrayList<>();

    static final String PASTA_DADOS    = "data";
    static final String ARQUIVO_CANAIS = "/app/shared/channels.json";
    static final String ARQUIVO_LOGINS = "data/logins.json";
    static final String ARQUIVO_REQUISICOES = "data/requests.jsonl";
    static final String ARQUIVO_PUBLICACOES = "data/publications.jsonl";
    static final String ARQUIVO_COORDENADOR = "/app/shared/coordenador.json";
    static final String ARQUIVO_HORA_COORDENADOR = "/app/shared/hora_coordenador.json";

    static final String NOME_SERVIDOR = "server_java";
    static final int    PORTA_DIRETA  = 5572;

    // Portas para eleição/berkeley
    static final Map<String, String> PORTAS = new HashMap<>();
    static {
        PORTAS.put("server_c",      "tcp://server_c:5570");
        PORTAS.put("server_python", "tcp://server_python:5571");
        PORTAS.put("server_java",   "tcp://server_java:5572");
    }

    // Ranks fixos (mesmo critério dos outros servidores)
    static final Map<String, Integer> RANKS = new HashMap<>();
    static {
        RANKS.put("server_c",      1);
        RANKS.put("server_python", 2);
        RANKS.put("server_java",   3);
    }

    // Portas de replicação
    static final Map<String, String> PORTAS_REPLICACAO = new HashMap<>();
    static {
        PORTAS_REPLICACAO.put("server_c",      "tcp://server_c:5580");
        PORTAS_REPLICACAO.put("server_python", "tcp://server_python:5581");
        PORTAS_REPLICACAO.put("server_java",   "tcp://server_java:5582");
    }
    static final int MINHA_PORTA_REPLICACAO = 5582;

    static int    contadorServidor    = 0;
    static int    contadorRequisicoes = 0;
    static double offsetRelogio       = 0.0;
    static int    rankServidor        = 0;
    static String coordenador         = "";

    static ZMQ.Context context;
    static ZMQ.Socket  pub;
    static ZMQ.Socket  ref;

    static final Object writeLock   = new Object();
    static final Object eleicaoLock = new Object();
    static final Object refLock     = new Object();   // protege acesso ao ref socket

    // Previne múltiplas eleições simultâneas
    static final AtomicBoolean eleicaoEmAndamento = new AtomicBoolean(false);

    // ════════════════════════════════════════════════════════════════════════
    // main
    // ════════════════════════════════════════════════════════════════════════
    public static void main(String[] args) throws Exception {
        new File(PASTA_DADOS).mkdirs();
        new File("/app/shared").mkdirs();

        carregarCanais();
        carregarLogins();

        context = ZMQ.context(1);

        ZMQ.Socket rep = context.socket(ZMQ.REP);
        rep.connect("tcp://broker:5556");

        pub = context.socket(ZMQ.PUB);
        pub.connect("tcp://proxy:5557");

        ref = context.socket(ZMQ.REQ);
        ref.connect("tcp://referencia:5560");

        System.out.println("[SERVER JAVA] Iniciado...");

        registrarNaReferencia();

        // Thread: eleição/berkeley (porta direta)
        iniciarThreadServidor(context);

        // Thread: replicação
        iniciarThreadReplicacao(context);

        // Thread: escuta anúncios de coordenador via PUB/SUB
        iniciarThreadSubServers(context);

        while (true) {
            byte[] mensagemBruta = rep.recv();
            contadorRequisicoes++;

            carregarCanais();
            carregarLogins();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mensagemBruta);
            int mapSize = unpacker.unpackMapHeader();

            String tipo    = "";
            String usuario = "";
            String canal   = "";
            String texto   = "";
            double timestamp          = agoraCorrigido();
            int    contadorRecebido   = 0;

            for (int i = 0; i < mapSize; i++) {
                String key = unpacker.unpackString();
                switch (key) {
                    case "type"      -> tipo    = unpacker.unpackString();
                    case "user"      -> usuario = unpacker.unpackString();
                    case "channel"   -> canal   = unpacker.unpackString();
                    case "message"   -> texto   = unpacker.unpackString();
                    case "timestamp" -> {
                        if (unpacker.getNextFormat().getValueType().isFloatType())
                            timestamp = unpacker.unpackDouble();
                        else
                            timestamp = unpacker.unpackLong();
                    }
                    case "contador"  -> contadorRecebido = unpacker.unpackInt();
                    default          -> unpacker.skipValue();
                }
            }

            atualizarContadorRecebido(contadorRecebido);

            salvarLinhaJson(ARQUIVO_REQUISICOES,
                "{\"type\":\"" + tipo + "\",\"user\":\"" + usuario +
                "\",\"received_timestamp\":" + agoraCorrigido() +
                ",\"contador\":" + contadorServidor + "}");

            byte[] resposta;

            boolean escrita = tipo.equals("login") ||
                              tipo.equals("create_channel") ||
                              tipo.equals("publish_message");

            if (escrita) {
                if (souCoordenador() || coordenador.isEmpty()) {
                    resposta = processarEscrita(tipo, usuario, canal, texto, timestamp);
                } else {
                    System.out.println("[BACKUP] Encaminhando " + tipo +
                        " para primário " + coordenador);
                    resposta = encaminharParaPrimario(mensagemBruta);
                    if (resposta == null) {
                        resposta = empacotarResposta("error", "primário indisponível");
                    }
                }
            } else if (tipo.equals("list_channels")) {
                resposta = empacotarListaCanais();
            } else {
                resposta = empacotarResposta("error", "tipo inválido");
            }

            rep.send(resposta);

            System.out.println("[SERVER JAVA] tipo=" + tipo +
                " | user=" + usuario + " | canal=" + canal +
                " | contador=" + contadorServidor +
                " | coordenador=" + coordenador);

            if (contadorRequisicoes % 15 == 0 && contadorRequisicoes > 0) {
                parte4ACada15Mensagens();
            }
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Eleição / Berkeley
    // ════════════════════════════════════════════════════════════════════════

    static boolean servidorRespondeEleicao(String nome) {
        String porta = PORTAS.get(nome);
        if (porta == null) return false;
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

    static void elegerCoordenador(List<ServidorInfo> servidores) {
        // Previne dupla eleição
        if (!eleicaoEmAndamento.compareAndSet(false, true)) {
            System.out.println("[ELEICAO] Já em andamento, ignorando.");
            return;
        }
        try {
            System.out.println("[ELEICAO] Iniciando eleição...");

            List<ServidorInfo> vivos = new ArrayList<>();
            for (ServidorInfo s : servidores) {
                if (servidorRespondeEleicao(s.nome)) {
                    vivos.add(s);
                    System.out.println("[ELEICAO] " + s.nome + " respondeu OK");
                } else {
                    System.out.println("[ELEICAO] " + s.nome + " não respondeu");
                }
            }
            if (vivos.isEmpty()) vivos = servidores;

            // Menor rank vence
            ServidorInfo eleito = vivos.stream()
                .min(java.util.Comparator.comparingInt(s -> s.rank))
                .orElse(null);
            if (eleito == null) return;

            coordenador = eleito.nome;
            salvarCoordenador();

            // Só publica se eu sou o de menor rank entre os vivos
            int meuRank = RANKS.getOrDefault(NOME_SERVIDOR, 999);
            int menorRankVivo = vivos.stream().mapToInt(s -> s.rank).min().orElse(999);
            if (meuRank == menorRankVivo) {
                publicarCoordenador(pub, coordenador);
                System.out.println("[ELEICAO] Coordenador escolhido: " + coordenador);
            } else {
                System.out.println("[ELEICAO] Aguardando anúncio. Coordenador: " + coordenador);
            }
        } finally {
            eleicaoEmAndamento.set(false);
        }
    }

    static void parte4ACada15Mensagens() {
        enviarHeartbeat();

        // Verifica se coordenador atual ainda responde
        if (!coordenador.isEmpty() && !servidorRespondeEleicao(coordenador)) {
            System.out.println("[FALHA] Coordenador caiu: " + coordenador);
            coordenador = "";
        }

        if (coordenador.isEmpty()) {
            List<ServidorInfo> servidores = pedirListaServidores();
            elegerCoordenador(servidores);
        } else {
            System.out.println("[OK] Coordenador ainda ativo: " + coordenador);
        }

        sincronizarBerkeley(context);
    }

    static List<ServidorInfo> pedirListaServidores() {
        String lista;
        synchronized (refLock) {
            ref.send("{\"type\":\"list\"}");
            lista = ref.recvStr();
        }
        List<ServidorInfo> servidores = extrairServidores(lista != null ? lista : "[]");
        System.out.println("[SERVIDORES ATIVOS]");
        for (ServidorInfo s : servidores)
            System.out.println(" - " + s.nome + " (rank=" + s.rank + ")");
        return servidores;
    }

    static void enviarHeartbeat() {
        String json = "{\"type\":\"heartbeat\",\"name\":\"" + NOME_SERVIDOR + "\"}";
        String respostaHb;
        synchronized (refLock) {
            ref.send(json);
            respostaHb = ref.recvStr();
        }
        System.out.println("[HEARTBEAT] resposta=" + respostaHb);
    }

    static void sincronizarBerkeley(ZMQ.Context context) {
        if (coordenador.isEmpty()) return;
        if (coordenador.equals(NOME_SERVIDOR)) {
            double hora = agoraCorrigido();
            salvarHoraCoordenador(hora);
            System.out.println("[BERKELEY] Sou coordenador (" + NOME_SERVIDOR + ")");
            return;
        }
        String porta = PORTAS.get(coordenador);
        if (porta == null) return;
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

    // ════════════════════════════════════════════════════════════════════════
    // Thread SUB — escuta anúncios de coordenador
    // ════════════════════════════════════════════════════════════════════════

    static void iniciarThreadSubServers(ZMQ.Context ctx) {
        new Thread(() -> {
            ZMQ.Socket sub = ctx.socket(ZMQ.SUB);
            sub.connect("tcp://proxy:5558");
            sub.subscribe("servers".getBytes());
            System.out.println("[SUB] Escutando anúncios de coordenador em 'servers'");

            while (true) {
                try {
                    sub.recvStr(); // tópico
                    byte[] dados = sub.recv();
                    Map<String, Object> msg = desempacotar(dados);
                    if ("coordinator_announce".equals(msg.get("type"))) {
                        String novo = (String) msg.getOrDefault("coordinator", "");
                        if (!novo.isEmpty() && !novo.equals(coordenador)) {
                            System.out.println("[SUB] Coordenador atualizado via anúncio: " + novo);
                            coordenador = novo;
                            salvarCoordenador();
                        }
                    }
                } catch (Exception e) {
                    System.out.println("[ERRO SUB] " + e.getMessage());
                }
            }
        }, "thread-sub-servers").start();
    }

    // ════════════════════════════════════════════════════════════════════════
    // Replicação Passiva (Primary-Backup)
    // ════════════════════════════════════════════════════════════════════════

    static byte[] processarEscrita(String tipo, String usuario, String canal,
                                    String texto, double timestamp) throws Exception {
        synchronized (writeLock) {
            carregarCanais();
            carregarLogins();

            if (tipo.equals("login")) {
                logins.add("{\"user\":\"" + usuario + "\",\"timestamp\":" + timestamp + "}");
                salvarLogins();

                Map<String, Object> evento = new HashMap<>();
                evento.put("type", "replicate_login");
                evento.put("user", usuario);
                evento.put("timestamp", timestamp);
                new Thread(() -> replicarParaBackups(evento), "replica-login").start();

                return empacotarResposta("ok", "login realizado (" + usuario + ")");

            } else if (tipo.equals("create_channel")) {
                canal = canal.trim();
                if (canal.isEmpty())
                    return empacotarResposta("error", "nome de canal inválido");
                if (canais.contains(canal))
                    return empacotarResposta("error", "canal já existe");

                canais.add(canal);
                salvarCanais();

                Map<String, Object> evento = new HashMap<>();
                evento.put("type", "replicate_channel");
                evento.put("channel", canal);
                new Thread(() -> replicarParaBackups(evento), "replica-channel").start();

                return empacotarResposta("ok", "canal '" + canal + "' criado");

            } else if (tipo.equals("publish_message")) {
                if (!canais.contains(canal))
                    return empacotarResposta("error", "canal inexistente");

                double publishedTs = agoraCorrigido();
                int    contadorPub = proximoContador();

                byte[] publicacao = empacotarPublicacao(usuario, canal, texto,
                    timestamp, publishedTs, contadorPub);
                pub.sendMore(canal);
                pub.send(publicacao);

                salvarLinhaJson(ARQUIVO_PUBLICACOES,
                    "{\"channel\":\"" + canal + "\",\"user\":\"" + usuario +
                    "\",\"message\":\"" + texto.replace("\"", "'") +
                    "\",\"request_timestamp\":" + timestamp +
                    ",\"published_timestamp\":" + publishedTs +
                    ",\"contador\":" + contadorPub + "}");

                Map<String, Object> evento = new HashMap<>();
                evento.put("type",                "replicate_publish");
                evento.put("channel",             canal);
                evento.put("user",                usuario);
                evento.put("message",             texto);
                evento.put("request_timestamp",   timestamp);
                evento.put("published_timestamp", publishedTs);
                evento.put("contador",            contadorPub);
                new Thread(() -> replicarParaBackups(evento), "replica-publish").start();

                return empacotarResposta("ok", "mensagem publicada em '" + canal + "'");
            }

            return empacotarResposta("error", "tipo inválido");
        }
    }

    static void replicarParaBackups(Map<String, Object> evento) {
        for (Map.Entry<String, String> entry : PORTAS_REPLICACAO.entrySet()) {
            String nome  = entry.getKey();
            String porta = entry.getValue();
            if (nome.equals(NOME_SERVIDOR)) continue;

            ZMQ.Socket sock = context.socket(ZMQ.REQ);
            sock.setReceiveTimeOut(2000);
            try {
                sock.connect(porta);
                sock.send(empacotar(evento));
                byte[] ack = sock.recv();
                System.out.println("[REPLICA] ACK de " + nome +
                    ": " + (ack != null ? "ok" : "null"));
            } catch (Exception e) {
                System.out.println("[REPLICA] Falha ao replicar para " + nome +
                    ": " + e.getMessage());
            } finally {
                sock.close();
            }
        }
    }

    /**
     * Backup encaminha escrita para o primário.
     * Em falha: limpa coordenador e dispara eleição imediata.
     */
    static byte[] encaminharParaPrimario(byte[] mensagemBruta) {
        String porta = PORTAS_REPLICACAO.get(coordenador);
        if (porta == null) return null;

        ZMQ.Socket sock = context.socket(ZMQ.REQ);
        sock.setReceiveTimeOut(3000);
        try {
            sock.connect(porta);
            sock.send(mensagemBruta);
            return sock.recv();
        } catch (Exception e) {
            System.out.println("[ENCAMINHAR] Erro: " + e.getMessage());
            // Limpa coordenador e elege imediatamente
            coordenador = "";
            List<ServidorInfo> lista = pedirListaServidores();
            new Thread(() -> elegerCoordenador(lista), "eleicao-emergencia").start();
            return null;
        } finally {
            sock.close();
        }
    }

    /** Aplica evento de replicação recebido do primário. */
    static void aplicarReplicacao(Map<String, Object> evento) {
        synchronized (writeLock) {
            String tipo = (String) evento.get("type");

            if ("replicate_login".equals(tipo)) {
                String u  = (String) evento.get("user");
                Object ts = evento.get("timestamp");
                logins.add("{\"user\":\"" + u + "\",\"timestamp\":" + ts + "}");
                salvarLogins();
                System.out.println("[REPLICA] login aplicado: " + u);

            } else if ("replicate_channel".equals(tipo)) {
                String c = (String) evento.get("channel");
                carregarCanais();
                if (!canais.contains(c)) {
                    canais.add(c);
                    salvarCanais();
                    System.out.println("[REPLICA] canal aplicado: " + c);
                }

            } else if ("replicate_publish".equals(tipo)) {
                String ch  = (String) evento.get("channel");
                String usr = (String) evento.get("user");
                String msg = (String) evento.get("message");
                Object rts = evento.get("request_timestamp");
                Object pts = evento.get("published_timestamp");
                Object cnt = evento.get("contador");
                // Sincroniza contador com o do primário
                if (cnt instanceof Number) {
                    atualizarContadorRecebido(((Number) cnt).intValue());
                }
                salvarLinhaJson(ARQUIVO_PUBLICACOES,
                    "{\"channel\":\"" + ch + "\",\"user\":\"" + usr +
                    "\",\"message\":\"" + (msg != null ? msg.replace("\"", "'") : "") +
                    "\",\"request_timestamp\":" + rts +
                    ",\"published_timestamp\":" + pts +
                    ",\"contador\":" + cnt + "}");
                System.out.println("[REPLICA] publicação aplicada: canal=" + ch);
            }
        }
    }

    static void iniciarThreadReplicacao(ZMQ.Context ctx) {
        new Thread(() -> {
            ZMQ.Socket srv = ctx.socket(ZMQ.REP);
            srv.bind("tcp://*:" + MINHA_PORTA_REPLICACAO);
            System.out.println("[REPLICA] Thread de replicação escutando na porta " +
                MINHA_PORTA_REPLICACAO);

            while (true) {
                try {
                    byte[] dados = srv.recv();
                    Map<String, Object> evento = desempacotar(dados);
                    String tipo = (String) evento.getOrDefault("type", "");

                    if ("login".equals(tipo) || "create_channel".equals(tipo) ||
                        "publish_message".equals(tipo)) {
                        // Backup encaminhou escrita → processo como primário
                        String usuario = (String) evento.getOrDefault("user", "");
                        String canal   = (String) evento.getOrDefault("channel", "");
                        String texto   = (String) evento.getOrDefault("message", "");
                        double ts      = toDouble(evento.get("timestamp"));
                        byte[] resp = processarEscrita(tipo, usuario, canal, texto, ts);
                        srv.send(resp);

                    } else if (tipo.startsWith("replicate_")) {
                        aplicarReplicacao(evento);
                        srv.send("{\"status\":\"ok\"}".getBytes());

                    } else {
                        srv.send("{\"status\":\"error\"}".getBytes());
                    }

                } catch (Exception e) {
                    System.out.println("[ERRO REPLICA] " + e.getMessage());
                    try { srv.send("{\"status\":\"error\"}".getBytes()); }
                    catch (Exception ignored) {}
                }
            }
        }, "thread-replicacao").start();
    }

    static boolean souCoordenador() {
        return NOME_SERVIDOR.equals(coordenador);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Helpers de serialização
    // ════════════════════════════════════════════════════════════════════════

    static byte[] empacotar(Map<String, Object> mapa) throws Exception {
        MessageBufferPacker pk = MessagePack.newDefaultBufferPacker();
        pk.packMapHeader(mapa.size());
        for (Map.Entry<String, Object> e : mapa.entrySet()) {
            pk.packString(e.getKey());
            Object v = e.getValue();
            if (v instanceof String s)       pk.packString(s);
            else if (v instanceof Integer i) pk.packInt(i);
            else if (v instanceof Long l)    pk.packLong(l);
            else if (v instanceof Double d)  pk.packDouble(d);
            else if (v instanceof Number n)  pk.packDouble(n.doubleValue());
            else pk.packString(String.valueOf(v));
        }
        pk.close();
        return pk.toByteArray();
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> desempacotar(byte[] dados) throws Exception {
        MessageUnpacker up = MessagePack.newDefaultUnpacker(dados);
        int size = up.unpackMapHeader();
        Map<String, Object> mapa = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = up.unpackString();
            var fmt = up.getNextFormat().getValueType();
            Object val;
            if (fmt.isStringType())       val = up.unpackString();
            else if (fmt.isFloatType())   val = up.unpackDouble();
            else if (fmt.isIntegerType()) val = up.unpackLong();
            else                          { up.skipValue(); val = null; }
            mapa.put(key, val);
        }
        return mapa;
    }

    static double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Number n) return n.doubleValue();
        return 0.0;
    }

    // ════════════════════════════════════════════════════════════════════════
    // Thread eleição/berkeley (porta direta)
    // ════════════════════════════════════════════════════════════════════════

    static void iniciarThreadServidor(ZMQ.Context context) {
        new Thread(() -> {
            ZMQ.Socket srv = context.socket(ZMQ.REP);
            srv.bind("tcp://*:" + PORTA_DIRETA);
            System.out.println("[SERVER JAVA] Thread direta escutando na porta " + PORTA_DIRETA);
            while (true) {
                try {
                    String msg = srv.recvStr();
                    if (msg == null) continue;
                    if (msg.contains("\"election\""))
                        srv.send("{\"status\":\"ok\"}");
                    else if (msg.contains("\"get_time\""))
                        srv.send("{\"time\":" + agoraCorrigido() + "}");
                    else
                        srv.send("{\"status\":\"error\"}");
                } catch (Exception e) {
                    System.out.println("[ERRO THREAD] " + e.getMessage());
                }
            }
        }, "servidor-direto").start();
    }

    // ════════════════════════════════════════════════════════════════════════
    // Utilitários
    // ════════════════════════════════════════════════════════════════════════

    static double agora() { return System.currentTimeMillis() / 1000.0; }
    static double agoraCorrigido() { return agora() + offsetRelogio; }

    static synchronized void atualizarContadorRecebido(int c) {
        contadorServidor = Math.max(contadorServidor, c);
    }

    static synchronized int proximoContador() { return ++contadorServidor; }

    static void registrarNaReferencia() {
        String json = "{\"type\":\"register\",\"name\":\"" + NOME_SERVIDOR + "\"}";
        String resposta;
        synchronized (refLock) {
            ref.send(json);
            resposta = ref.recvStr();
        }
        int idx = resposta.indexOf("\"rank\":");
        if (idx >= 0) {
            String resto = resposta.substring(idx + 7).replaceAll("[^0-9]", "");
            if (!resto.isEmpty()) rankServidor = Integer.parseInt(resto);
        }
        System.out.println("[SERVER JAVA] Meu rank: " + rankServidor);
    }

    static void publicarCoordenador(ZMQ.Socket pub, String eleito) {
        try {
            Map<String, Object> msg = new HashMap<>();
            msg.put("type",        "coordinator_announce");
            msg.put("coordinator", eleito);
            msg.put("server",      NOME_SERVIDOR);
            msg.put("timestamp",   agoraCorrigido());
            msg.put("contador",    (long) proximoContador());
            pub.sendMore("servers".getBytes());
            pub.send(empacotar(msg));
        } catch (Exception e) {
            System.out.println("[ERRO PUB COORD] " + e.getMessage());
        }
        System.out.println("[PUB SERVERS] coordenador eleito: " + eleito);
    }

    static void salvarCoordenador() {
        try { Files.writeString(Paths.get(ARQUIVO_COORDENADOR),
            "{\"coordenador\":\"" + coordenador + "\"}"); }
        catch (Exception e) { System.out.println("[ERRO] ao salvar coordenador"); }
    }

    static void salvarHoraCoordenador(double hora) {
        try { Files.writeString(Paths.get(ARQUIVO_HORA_COORDENADOR),
            "{\"coordenador\":\"" + NOME_SERVIDOR + "\",\"hora\":" + hora + "}"); }
        catch (Exception e) { System.out.println("[ERRO] ao salvar hora do coordenador"); }
    }

    static List<ServidorInfo> extrairServidores(String lista) {
        List<ServidorInfo> servidores = new ArrayList<>();
        Pattern p = Pattern.compile(
            "\\{\\s*\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"\\s*,\\s*\\\"rank\\\"\\s*:\\s*(\\d+)\\s*\\}");
        Matcher m = p.matcher(lista);
        while (m.find())
            servidores.add(new ServidorInfo(m.group(1), Integer.parseInt(m.group(2))));
        return servidores;
    }

    static void carregarCanais() {
        canais.clear();
        try {
            if (Files.exists(Paths.get(ARQUIVO_CANAIS))) {
                String conteudo = Files.readString(Paths.get(ARQUIVO_CANAIS)).trim()
                    .replace("[", "").replace("]", "").replace("\"", "");
                if (!conteudo.isEmpty()) {
                    for (String parte : conteudo.split(",")) {
                        String c = parte.trim();
                        if (!c.isEmpty()) canais.add(c);
                    }
                }
            }
        } catch (Exception e) { System.out.println("[ERRO] ao carregar canais"); }
    }

    static void carregarLogins() {
        logins.clear();
        try {
            if (Files.exists(Paths.get(ARQUIVO_LOGINS))) {
                for (String linha : Files.readAllLines(Paths.get(ARQUIVO_LOGINS))) {
                    linha = linha.trim();
                    if (!linha.equals("[") && !linha.equals("]") && !linha.isEmpty()) {
                        if (linha.endsWith(",")) linha = linha.substring(0, linha.length() - 1);
                        logins.add(linha);
                    }
                }
            }
        } catch (Exception e) { System.out.println("[ERRO] ao carregar logins"); }
    }

    static void salvarCanais() {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(ARQUIVO_CANAIS))) {
            w.write("[\n");
            for (int i = 0; i < canais.size(); i++) {
                w.write("  \"" + canais.get(i) + "\"");
                if (i < canais.size() - 1) w.write(",");
                w.write("\n");
            }
            w.write("]\n");
        } catch (Exception e) { System.out.println("[ERRO] ao salvar canais"); }
    }

    static void salvarLogins() {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(ARQUIVO_LOGINS))) {
            w.write("[\n");
            for (int i = 0; i < logins.size(); i++) {
                w.write("  " + logins.get(i));
                if (i < logins.size() - 1) w.write(",");
                w.write("\n");
            }
            w.write("]\n");
        } catch (Exception e) { System.out.println("[ERRO] ao salvar logins"); }
    }

    static void salvarLinhaJson(String arquivo, String linha) {
        try (BufferedWriter w = new BufferedWriter(new FileWriter(arquivo, true))) {
            w.write(linha);
            w.newLine();
        } catch (Exception e) { System.out.println("[ERRO] ao salvar jsonl"); }
    }

    static byte[] empacotarResposta(String status, String message) throws Exception {
        MessageBufferPacker pk = MessagePack.newDefaultBufferPacker();
        pk.packMapHeader(5);
        pk.packString("status");      pk.packString(status);
        pk.packString("message");     pk.packString(message);
        pk.packString("timestamp");   pk.packDouble(agoraCorrigido());
        pk.packString("contador");    pk.packInt(proximoContador());
        pk.packString("coordenador"); pk.packString(coordenador);
        pk.close();
        return pk.toByteArray();
    }

    static byte[] empacotarListaCanais() throws Exception {
        MessageBufferPacker pk = MessagePack.newDefaultBufferPacker();
        pk.packMapHeader(5);
        pk.packString("status");      pk.packString("ok");
        pk.packString("channels");    pk.packArrayHeader(canais.size());
        for (String c : canais) pk.packString(c);
        pk.packString("timestamp");   pk.packDouble(agoraCorrigido());
        pk.packString("contador");    pk.packInt(proximoContador());
        pk.packString("coordenador"); pk.packString(coordenador);
        pk.close();
        return pk.toByteArray();
    }

    static byte[] empacotarPublicacao(String user, String canal, String message,
                                       double requestTs, double publishedTs,
                                       int contadorPub) throws Exception {
        MessageBufferPacker pk = MessagePack.newDefaultBufferPacker();
        pk.packMapHeader(6);
        pk.packString("user");                pk.packString(user);
        pk.packString("channel");             pk.packString(canal);
        pk.packString("message");             pk.packString(message);
        pk.packString("request_timestamp");   pk.packDouble(requestTs);
        pk.packString("published_timestamp"); pk.packDouble(publishedTs);
        pk.packString("contador");            pk.packInt(contadorPub);
        pk.close();
        return pk.toByteArray();
    }

    static class ServidorInfo {
        String nome; int rank;
        ServidorInfo(String nome, int rank) { this.nome = nome; this.rank = rank; }
    }
}