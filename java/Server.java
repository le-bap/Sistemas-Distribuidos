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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Server {
    static List<String> canais = new ArrayList<>();
    static List<String> logins = new ArrayList<>();

    static final String PASTA_DADOS = "data";
    static final String ARQUIVO_CANAIS = "/app/shared/channels.json";
    static final String ARQUIVO_LOGINS = "data/logins.json";
    static final String ARQUIVO_REQUISICOES = "data/requests.jsonl";
    static final String ARQUIVO_PUBLICACOES = "data/publications.jsonl";

    static final String ARQUIVO_COORDENADOR = "/app/shared/coordenador.json";
    static final String ARQUIVO_HORA_COORDENADOR = "/app/shared/hora_coordenador.json";

    static final String NOME_SERVIDOR = "server_java";

    static int contadorServidor = 0;
    static int contadorRequisicoes = 0;
    static double offsetRelogio = 0.0;
    static int rankServidor = 0;
    static String coordenador = "";

    public static void main(String[] args) throws Exception {
        new File(PASTA_DADOS).mkdirs();
        new File("/app/shared").mkdirs();

        carregarCanais();
        carregarLogins();

        ZMQ.Context context = ZMQ.context(1);

        ZMQ.Socket rep = context.socket(ZMQ.REP);
        rep.connect("tcp://broker:5556");

        ZMQ.Socket pub = context.socket(ZMQ.PUB);
        pub.connect("tcp://proxy:5557");

        ZMQ.Socket ref = context.socket(ZMQ.REQ);
        ref.connect("tcp://referencia:5560");

        System.out.println("[SERVER JAVA] Iniciado...");

        registrarNaReferencia(ref);

        while (true) {
            byte[] mensagemBruta = rep.recv();
            contadorRequisicoes++;

            carregarCanais();
            carregarLogins();
            lerCoordenadorDoArquivo();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mensagemBruta);
            int mapSize = unpacker.unpackMapHeader();

            String tipo = "";
            String usuario = "";
            String canal = "";
            String texto = "";
            double timestamp = agoraCorrigido();
            int contadorRecebido = 0;

            for (int i = 0; i < mapSize; i++) {
                String key = unpacker.unpackString();

                if (key.equals("type")) {
                    tipo = unpacker.unpackString();
                } else if (key.equals("user")) {
                    usuario = unpacker.unpackString();
                } else if (key.equals("channel")) {
                    canal = unpacker.unpackString();
                } else if (key.equals("message")) {
                    texto = unpacker.unpackString();
                } else if (key.equals("timestamp")) {
                    if (unpacker.getNextFormat().getValueType().isFloatType()) {
                        timestamp = unpacker.unpackDouble();
                    } else {
                        timestamp = unpacker.unpackLong();
                    }
                } else if (key.equals("contador")) {
                    contadorRecebido = unpacker.unpackInt();
                } else {
                    unpacker.skipValue();
                }
            }

            atualizarContadorRecebido(contadorRecebido);

            salvarLinhaJson(
                ARQUIVO_REQUISICOES,
                "{\"type\":\"" + tipo + "\",\"user\":\"" + usuario + "\",\"received_timestamp\":" + agoraCorrigido() + ",\"contador\":" + contadorServidor + "}"
            );

            byte[] resposta;

            if (tipo.equals("login")) {
                logins.add("{\"user\":\"" + usuario + "\",\"timestamp\":" + timestamp + "}");
                salvarLogins();

                resposta = empacotarResposta("ok", "login realizado (" + usuario + ")");
            } else if (tipo.equals("create_channel")) {
                if (canal.trim().isEmpty()) {
                    resposta = empacotarResposta("error", "nome de canal inválido");
                } else if (canais.contains(canal)) {
                    resposta = empacotarResposta("error", "canal já existe");
                } else {
                    canais.add(canal);
                    salvarCanais();
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

                    byte[] publicacao = empacotarPublicacao(usuario, canal, texto, timestamp, publishedTimestamp, contadorPub);
                    pub.sendMore(canal);
                    pub.send(publicacao);

                    salvarLinhaJson(
                        ARQUIVO_PUBLICACOES,
                        "{\"channel\":\"" + canal + "\",\"user\":\"" + usuario + "\",\"message\":\"" + texto.replace("\"", "'") + "\",\"request_timestamp\":" + timestamp + ",\"published_timestamp\":" + publishedTimestamp + ",\"contador\":" + contadorPub + "}"
                    );

                    resposta = empacotarResposta("ok", "mensagem publicada em '" + canal + "'");
                }
            } else if (tipo.equals("election")) {
                resposta = empacotarResposta("ok", "OK");
            }else {
                resposta = empacotarResposta("error", "tipo inválido");
            }

            rep.send(resposta);

            String msgLog = "[SERVER JAVA] tipo=" + tipo +
                " | user=" + usuario +
                " | canal=" + canal +
                " | contador=" + contadorServidor +
                " | coordenador=" + coordenador;
            System.out.println(msgLog);

            if (contadorRequisicoes % 15 == 0) {
                enviarHeartbeat(ref, pub);
            }
        }
    }

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

    static void registrarNaReferencia(ZMQ.Socket ref) {
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

    static void enviarHeartbeat(ZMQ.Socket ref, ZMQ.Socket pub) {
        ref.send("{\"type\":\"heartbeat\",\"name\":\"" + NOME_SERVIDOR + "\"}");
        String resposta = ref.recvStr();

        ref.send("{\"type\":\"list\"}");
        String lista = ref.recvStr();

        List<ServidorInfo> servidores = extrairServidores(lista);

        boolean coordenadorVivo = false;

        for (ServidorInfo s : servidores) {
            if (s.nome.equals(coordenador)) {
                coordenadorVivo = true;
                break;
            }
        }

        // 🔥 CORREÇÃO
        if (coordenador.equals("") || !coordenadorVivo) {
            System.out.println("[ELEICAO] Coordenador inválido ou caiu! Nova eleição...");
            iniciarEleicao(servidores, pub);
        }

        // 🔥 Berkeley
        if (!coordenador.equals(NOME_SERVIDOR)) {
            System.out.println("[BERKELEY] Pedindo hora ao coordenador...");
        }

        sincronizarBerkeley();
    }

    static void iniciarEleicao(List<ServidorInfo> servidores, ZMQ.Socket pub) {
        System.out.println("[ELEICAO] Iniciando eleição...");

        String eleito = NOME_SERVIDOR;
        int menorRank = rankServidor;

        for (ServidorInfo s : servidores) {
            if (s.rank < menorRank) {
                menorRank = s.rank;
                eleito = s.nome;
            }
        }

        coordenador = eleito;
        salvarCoordenador();
        publicarCoordenador(pub, eleito);

        System.out.println("[ELEICAO] Coordenador escolhido: " + coordenador);
    }

    static void publicarCoordenador(ZMQ.Socket pub, String eleito) {
        pub.sendMore("servers");
        pub.send(eleito);
        System.out.println("[PUB SERVERS] coordenador eleito: " + eleito);
    }

    static void sincronizarBerkeley() {
        if (coordenador.equals("")) {
            return;
        }

        if (coordenador.equals(NOME_SERVIDOR)) {
            double hora = agoraCorrigido();
            salvarHoraCoordenador(hora);
            System.out.println("[BERKELEY] Eu sou o coordenador (" + NOME_SERVIDOR + "). Hora atual=" + hora);
        } else {
            System.out.println("[BERKELEY] Coordenador atual é " + coordenador + ". Aguardando sincronização dele.");
        }
    }

    static void salvarCoordenador() {
        try {
            Files.writeString(
                Paths.get(ARQUIVO_COORDENADOR),
                "{\"coordenador\":\"" + coordenador + "\"}"
            );
        } catch (Exception e) {
            System.out.println("[ERRO] ao salvar coordenador");
        }
    }

    static void lerCoordenadorDoArquivo() {
        try {
            if (!Files.exists(Paths.get(ARQUIVO_COORDENADOR))) {
                return;
            }

            String conteudo = Files.readString(Paths.get(ARQUIVO_COORDENADOR));
            int idx = conteudo.indexOf("\"coordenador\":");
            if (idx >= 0) {
                String resto = conteudo.substring(idx + 15);
                resto = resto.replace("\"", "").replace("}", "").trim();
                if (!resto.isEmpty()) {
                    coordenador = resto;
                }
            }
        } catch (Exception e) {
            System.out.println("[ERRO] ao ler coordenador");
        }
    }

    static void salvarHoraCoordenador(double hora) {
        try {
            Files.writeString(
                Paths.get(ARQUIVO_HORA_COORDENADOR),
                "{\"coordenador\":\"" + NOME_SERVIDOR + "\",\"hora\":" + hora + "}"
            );
        } catch (Exception e) {
            System.out.println("[ERRO] ao salvar hora do coordenador");
        }
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

    static void salvarCanais() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(ARQUIVO_CANAIS));
            writer.write("[\n");

            for (int i = 0; i < canais.size(); i++) {
                writer.write("  \"" + canais.get(i) + "\"");
                if (i < canais.size() - 1) {
                    writer.write(",");
                }
                writer.write("\n");
            }

            writer.write("]\n");
            writer.close();
        } catch (Exception e) {
            System.out.println("[ERRO] ao salvar canais");
        }
    }

    static void salvarLogins() {
        try {
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
        } catch (Exception e) {
            System.out.println("[ERRO] ao salvar logins");
        }
    }

    static void salvarLinhaJson(String arquivo, String linha) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(arquivo, true));
            writer.write(linha);
            writer.newLine();
            writer.close();
        } catch (Exception e) {
            System.out.println("[ERRO] ao salvar jsonl");
        }
    }

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

    static byte[] empacotarPublicacao(String user, String canal, String message, double requestTimestamp, double publishedTimestamp, int contadorPub) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(6);
        packer.packString("user");
        packer.packString(user);
        packer.packString("channel");
        packer.packString(canal);
        packer.packString("message");
        packer.packString(message);
        packer.packString("request_timestamp");
        packer.packDouble(requestTimestamp);
        packer.packString("published_timestamp");
        packer.packDouble(publishedTimestamp);
        packer.packString("contador");
        packer.packInt(contadorPub);

        packer.close();
        return packer.toByteArray();
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