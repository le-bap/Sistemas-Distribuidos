import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Client {
    static ZMQ.Context context = ZMQ.context(1);
    static ZMQ.Socket req = context.socket(ZMQ.REQ);
    static ZMQ.Socket sub = context.socket(ZMQ.SUB);

    static Random random = new Random();
    static String user = "bot_java_" + (1000 + random.nextInt(9000));
    static Set<String> canaisInscritos = new HashSet<>();

    public static void main(String[] args) throws Exception {
        req.connect("tcp://broker:5555");
        sub.connect("tcp://proxy:5558");

        Thread threadRecebimento = new Thread(() -> {
            while (true) {
                String canal = sub.recvStr();
                byte[] conteudo = sub.recv();

                try {
                    MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(conteudo);
                    int mapSize = unpacker.unpackMapHeader();

                    String mensagem = "";
                    double envio = 0;

                    for (int i = 0; i < mapSize; i++) {
                        String key = unpacker.unpackString();

                        if (key.equals("message")) {
                            mensagem = unpacker.unpackString();
                        } else if (key.equals("published_timestamp")) {
                            if (unpacker.getNextFormat().getValueType().isFloatType()) {
                                envio = unpacker.unpackDouble();
                            } else {
                                envio = unpacker.unpackLong();
                            }
                        } else {
                            unpacker.skipValue();
                        }
                    }

                    double recebimento = agora();

                    System.out.println(
                        "[MENSAGEM RECEBIDA] canal=" + canal +
                        " | mensagem=" + mensagem +
                        " | envio=" + envio +
                        " | recebimento=" + recebimento
                    );
                } catch (Exception e) {
                    System.out.println("[ERRO SUB] erro ao ler mensagem");
                }
            }
        });

        threadRecebimento.setDaemon(true);
        threadRecebimento.start();

        System.out.println("[CLIENT JAVA] Bot iniciado: " + user);

        fazerLogin();

        while (true) {
            List<String> canais = listarCanais();

            if (canais.size() < 5) {
                criarCanal();
                canais = listarCanais();
            }

            if (canaisInscritos.size() < 3) {
                seInscreverEmUmCanal(canais);
            }

            if (canais.isEmpty()) {
                Thread.sleep(1000);
                continue;
            }

            String canalEscolhido = canais.get(random.nextInt(canais.size()));

            for (int i = 0; i < 10; i++) {
                publicarMensagem(canalEscolhido, i + 1);
                Thread.sleep(1000);
            }
        }
    }

    static double agora() {
        return System.currentTimeMillis() / 1000.0;
    }

    static void fazerLogin() throws Exception {
        byte[] mensagem = empacotarLogin();
        req.send(mensagem);

        byte[] resposta = req.recv();
        System.out.println("[LOGIN] " + lerRespostaSimples(resposta));
    }

    static List<String> listarCanais() throws Exception {
        byte[] mensagem = empacotarListChannels();
        req.send(mensagem);

        byte[] resposta = req.recv();

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(resposta);
        int mapSize = unpacker.unpackMapHeader();

        List<String> canais = new ArrayList<>();

        for (int i = 0; i < mapSize; i++) {
            String key = unpacker.unpackString();

            if (key.equals("channels")) {
                int tamanho = unpacker.unpackArrayHeader();

                for (int j = 0; j < tamanho; j++) {
                    canais.add(unpacker.unpackString());
                }
            } else {
                unpacker.skipValue();
            }
        }

        return canais;
    }

    static void criarCanal() throws Exception {
        String canal = "canal_" + (1 + random.nextInt(999));

        byte[] mensagem = empacotarCreateChannel(canal);
        req.send(mensagem);

        byte[] resposta = req.recv();
        System.out.println("[CREATE CHANNEL] " + lerRespostaSimples(resposta));
    }

    static void seInscreverEmUmCanal(List<String> canaisDisponiveis) {
        List<String> naoInscritos = new ArrayList<>();

        for (String canal : canaisDisponiveis) {
            if (!canaisInscritos.contains(canal)) {
                naoInscritos.add(canal);
            }
        }

        if (naoInscritos.isEmpty()) {
            return;
        }

        String canalEscolhido = naoInscritos.get(random.nextInt(naoInscritos.size()));
        sub.subscribe(canalEscolhido.getBytes());
        canaisInscritos.add(canalEscolhido);

        System.out.println("[SUBSCRIBE] " + user + " inscrito em " + canalEscolhido);
    }

    static void publicarMensagem(String canal, int numero) throws Exception {
        String texto = "mensagem " + numero + " do " + user;

        byte[] mensagem = empacotarPublish(canal, texto);
        req.send(mensagem);

        byte[] resposta = req.recv();
        System.out.println("[PUBLISH] " + lerRespostaSimples(resposta));
    }

    static byte[] empacotarLogin() throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(3);
        packer.packString("type");
        packer.packString("login");
        packer.packString("user");
        packer.packString(user);
        packer.packString("timestamp");
        packer.packDouble(agora());

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarListChannels() throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(3);
        packer.packString("type");
        packer.packString("list_channels");
        packer.packString("user");
        packer.packString(user);
        packer.packString("timestamp");
        packer.packDouble(agora());

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarCreateChannel(String canal) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(4);
        packer.packString("type");
        packer.packString("create_channel");
        packer.packString("user");
        packer.packString(user);
        packer.packString("channel");
        packer.packString(canal);
        packer.packString("timestamp");
        packer.packDouble(agora());

        packer.close();
        return packer.toByteArray();
    }

    static byte[] empacotarPublish(String canal, String texto) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(5);
        packer.packString("type");
        packer.packString("publish_message");
        packer.packString("user");
        packer.packString(user);
        packer.packString("channel");
        packer.packString(canal);
        packer.packString("message");
        packer.packString(texto);
        packer.packString("timestamp");
        packer.packDouble(agora());

        packer.close();
        return packer.toByteArray();
    }

    static String lerRespostaSimples(byte[] resposta) throws Exception {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(resposta);
        int mapSize = unpacker.unpackMapHeader();

        String status = "";
        String message = "";

        for (int i = 0; i < mapSize; i++) {
            String key = unpacker.unpackString();

            if (key.equals("status")) {
                status = unpacker.unpackString();
            } else if (key.equals("message")) {
                message = unpacker.unpackString();
            } else {
                unpacker.skipValue();
            }
        }

        return "{status=" + status + ", message=" + message + "}";
    }
}