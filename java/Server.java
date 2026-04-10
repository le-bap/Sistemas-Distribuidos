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

public class Server {
    static List<String> canais = new ArrayList<>();
    static List<String> logins = new ArrayList<>();

    static final String PASTA_DADOS = "data";
    static final String ARQUIVO_CANAIS = "data/channels.json";
    static final String ARQUIVO_LOGINS = "data/logins.json";
    static final String ARQUIVO_REQUISICOES = "data/requests.jsonl";
    static final String ARQUIVO_PUBLICACOES = "data/publications.jsonl";

    public static void main(String[] args) throws Exception {
        new File(PASTA_DADOS).mkdirs();

        carregarCanais();
        carregarLogins();

        ZMQ.Context context = ZMQ.context(1);

        ZMQ.Socket rep = context.socket(ZMQ.REP);
        rep.connect("tcp://broker:5556");

        ZMQ.Socket pub = context.socket(ZMQ.PUB);
        pub.connect("tcp://proxy:5557");

        System.out.println("[SERVER JAVA] Iniciado...");

        while (true) {

            byte[] mensagemBruta = rep.recv();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mensagemBruta);
            int mapSize = unpacker.unpackMapHeader();

            String tipo = "";
            String usuario = "";
            String canal = "";
            String texto = "";
            double timestamp = agora();

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
                } else {
                    unpacker.skipValue();
                }
            }

            salvarLinhaJson(ARQUIVO_REQUISICOES,
                "{\"type\":\"" + tipo + "\",\"user\":\"" + usuario + "\",\"received_timestamp\":" + agora() + "}"
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
                    double publishedTimestamp = agora();

                    byte[] publicacao = empacotarPublicacao(usuario, canal, texto, timestamp, publishedTimestamp);
                    pub.sendMore(canal);
                    pub.send(publicacao);

                    salvarLinhaJson(ARQUIVO_PUBLICACOES,
                        "{\"channel\":\"" + canal + "\",\"user\":\"" + usuario + "\",\"message\":\"" + texto.replace("\"", "'") + "\",\"request_timestamp\":" + timestamp + ",\"published_timestamp\":" + publishedTimestamp + "}"
                    );

                    resposta = empacotarResposta("ok", "mensagem publicada em '" + canal + "'");
                }
            } else {
                resposta = empacotarResposta("error", "tipo inválido");
            }

            rep.send(resposta);
            String msgLog = "[SERVER JAVA] tipo=" + tipo + 
                " | user=" + usuario + 
                " | canal=" + canal +
                " | timestamp= " + timestamp;

            System.out.println(msgLog);
        }
    }

    static double agora() {
        return System.currentTimeMillis() / 1000.0;
    }

    static void carregarCanais() {
        try {
            if (Files.exists(Paths.get(ARQUIVO_CANAIS))) {
                String conteudo = Files.readString(Paths.get(ARQUIVO_CANAIS)).trim();
                conteudo = conteudo.replace("[", "").replace("]", "").replace("\"", "");

                if (!conteudo.isEmpty()) {
                    String[] partes = conteudo.split(",");
                    for (String parte : partes) {
                        String canal = parte.trim();
                        if (!canal.isEmpty()) {
                            canais.add(canal);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[ERRO] ao carregar canais");
        }
    }

    static void carregarLogins() {
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

        packer.packMapHeader(3);
        packer.packString("status");
        packer.packString(status);
        packer.packString("message");
        packer.packString(message);
        packer.packString("timestamp");
        packer.packDouble(agora());

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarListaCanais() throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(3);
        packer.packString("status");
        packer.packString("ok");
        packer.packString("channels");
        packer.packArrayHeader(canais.size());

        for (String canal : canais) {
            packer.packString(canal);
        }

        packer.packString("timestamp");
        packer.packDouble(agora());

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarPublicacao(String user, String canal, String message, double requestTimestamp, double publishedTimestamp) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(5);
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

        packer.close();
        return packer.toByteArray();
    }
}