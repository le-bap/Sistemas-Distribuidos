// import org.zeromq.ZMQ;
// import org.msgpack.core.MessageBufferPacker;
// import org.msgpack.core.MessagePack;
// import org.msgpack.core.MessageUnpacker;
// import org.msgpack.value.ValueType;
// import java.io.IOException;

// import java.util.Random;

// public class Client {
//     public static void main(String[] args) throws Exception {
//         ZMQ.Context context = ZMQ.context(1);
//         ZMQ.Socket socket = context.socket(ZMQ.REQ);

//         socket.connect("tcp://broker:5555");

//         Random rand = new Random();
//         String user = "bot_" + (rand.nextInt(9000) + 1000);

//         System.out.println("[CLIENT JAVA] Iniciando como " + user);

//         while (true) {
//             System.out.println("\n--- NOVO CICLO ---");

//             send(socket, packLogin(user));

//             String channel = "canal_" + (rand.nextInt(300) + 1);
//             send(socket, packCreate(user, channel));

//             send(socket, packList(user));

//             Thread.sleep(1200);
//         }
//     }

//     static void send(ZMQ.Socket socket, byte[] data) throws Exception {
//         socket.send(data);

//         byte[] reply = socket.recv();
//         unpackAndPrint(reply);
//     }

//     static byte[] packLogin(String user) throws Exception {
//         MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

//         packer.packMapHeader(3);
//         packer.packString("type");
//         packer.packString("login");

//         packer.packString("user");
//         packer.packString(user);

//         packer.packString("timestamp");
//         packer.packDouble(System.currentTimeMillis() / 1000.0);

//         packer.close();
//         return packer.toByteArray();
//     }

//     static byte[] packCreate(String user, String channel) throws Exception {
//         MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

//         packer.packMapHeader(4);
//         packer.packString("type");
//         packer.packString("create_channel");

//         packer.packString("user");
//         packer.packString(user);

//         packer.packString("channel");
//         packer.packString(channel);

//         packer.packString("timestamp");
//         packer.packDouble(System.currentTimeMillis() / 1000.0);

//         packer.close();
//         return packer.toByteArray();
//     }

//     static byte[] packList(String user) throws Exception {
//         MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

//         packer.packMapHeader(3);
//         packer.packString("type");
//         packer.packString("list_channels");

//         packer.packString("user");
//         packer.packString(user);

//         packer.packString("timestamp");
//         packer.packDouble(System.currentTimeMillis() / 1000.0);

//         packer.close();
//         return packer.toByteArray();
//     }

//     public static void unpackAndPrint(byte[] response) throws IOException {
//         MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(response);

//         int mapSize = unpacker.unpackMapHeader();

//         String status = null;
//         String message = null;
//         Double timestamp = null;
//         StringBuilder channelsStr = new StringBuilder();

//         for (int i = 0; i < mapSize; i++) {
//             String key = unpacker.unpackString();

//             switch (key) {
//                 case "status":
//                     status = unpacker.unpackString();
//                     break;

//                 case "message":
//                     message = unpacker.unpackString();
//                     break;

//                 case "timestamp":
//                     if (unpacker.getNextFormat().getValueType().isFloatType()) {
//                         timestamp = unpacker.unpackDouble();
//                     } else if (unpacker.getNextFormat().getValueType().isIntegerType()) {
//                         timestamp = (double) unpacker.unpackLong();
//                     }
//                     break;

//                 case "channels":
//                     int arraySize = unpacker.unpackArrayHeader();
//                     channelsStr.append("[ ");
//                     for (int j = 0; j < arraySize; j++) {
//                         channelsStr.append(unpacker.unpackString()).append(" ");
//                     }
//                     channelsStr.append("]");
//                     break;

//                 default:
//                     unpacker.skipValue();
//                     break;
//             }
//         }

//         System.out.print("[RESPONSE] { ");
//         if (status != null) System.out.print("status: " + status + ", ");
//         if (message != null) System.out.print("message: " + message + ", ");
//         if (channelsStr.length() > 0) System.out.print("channels: " + channelsStr + ", ");
//         if (timestamp != null) System.out.print("timestamp: " + timestamp + ", ");
//         System.out.println("}");
//     }
// }

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.*;

public class Client {
    static ZMQ.Context context = ZMQ.context(1);
    static ZMQ.Socket req = context.socket(ZMQ.REQ);
    static ZMQ.Socket sub = context.socket(ZMQ.SUB);

    static String user = "bot_java_" + (1000 + new Random().nextInt(9000));
    static Set<String> subscribed = new HashSet<>();
    static Random random = new Random();

    public static void main(String[] args) throws Exception {
        req.connect("tcp://broker:5555");
        sub.connect("tcp://proxy:5558");

        Thread subscriber = new Thread(() -> {
            while (true) {
                String topic = sub.recvStr();
                byte[] payload = sub.recv();
                try {
                    Map<String, Object> msg = unpackMap(payload);
                    double recvTs = System.currentTimeMillis() / 1000.0;
                    System.out.println(
                            "[SUB][" + user + "] canal=" + topic +
                            " mensagem=" + msg.get("message") +
                            " envio=" + msg.get("published_timestamp") +
                            " recebimento=" + recvTs
                    );
                } catch (Exception e) {
                    System.out.println("[SUB][" + user + "] erro ao desempacotar mensagem");
                }
            }
        });
        subscriber.setDaemon(true);
        subscriber.start();

        System.out.println("[CLIENT JAVA] Iniciando como " + user);
        System.out.println(send(mapOf("type", "login", "user", user, "timestamp", epoch())));

        while (true) {
            Map<String, Object> listResp = send(mapOf(
                    "type", "list_channels",
                    "user", user,
                    "timestamp", epoch()
            ));

            List<String> channels = castStringList(listResp.get("channels"));

            if (channels.size() < 5) {
                String newChannel = "canal_" + (1 + random.nextInt(999));
                System.out.println(send(mapOf(
                        "type", "create_channel",
                        "user", user,
                        "channel", newChannel,
                        "timestamp", epoch()
                )));

                listResp = send(mapOf(
                        "type", "list_channels",
                        "user", user,
                        "timestamp", epoch()
                ));
                channels = castStringList(listResp.get("channels"));
            }

            List<String> remaining = new ArrayList<>();
            for (String c : channels) {
                if (!subscribed.contains(c)) remaining.add(c);
            }

            while (subscribed.size() < 3 && !remaining.isEmpty()) {
                String ch = remaining.get(random.nextInt(remaining.size()));
                sub.subscribe(ch.getBytes());
                subscribed.add(ch);
                remaining.remove(ch);
                System.out.println("[SUBSCRIBE][" + user + "] " + ch);
            }

            if (channels.isEmpty()) {
                Thread.sleep(1000);
                continue;
            }

            String chosen = channels.get(random.nextInt(channels.size()));

            for (int i = 0; i < 10; i++) {
                String text = "mensagem " + (i + 1) + " do " + user;

                Map<String, Object> resp = send(mapOf(
                        "type", "publish_message",
                        "user", user,
                        "channel", chosen,
                        "message", text,
                        "timestamp", epoch()
                ));

                System.out.println("[PUBLISH] " + resp);
                Thread.sleep(1000);
            }
        }
    }

    static Map<String, Object> send(Map<String, Object> map) throws IOException {
        req.send(packMap(map));
        return unpackMap(req.recv());
    }

    static byte[] packMap(Map<String, Object> map) throws IOException {
        MessageBufferPacker p = MessagePack.newDefaultBufferPacker();
        p.packMapHeader(map.size());

        for (Map.Entry<String, Object> e : map.entrySet()) {
            p.packString(e.getKey());
            Object v = e.getValue();

            if (v instanceof String s) p.packString(s);
            else if (v instanceof Integer n) p.packInt(n);
            else if (v instanceof Long n) p.packLong(n);
            else if (v instanceof Double d) p.packDouble(d);
            else if (v instanceof Float d) p.packFloat(d);
            else if (v instanceof List<?> list) {
                p.packArrayHeader(list.size());
                for (Object item : list) p.packString(String.valueOf(item));
            } else {
                p.packString(String.valueOf(v));
            }
        }

        p.close();
        return p.toByteArray();
    }

    static Map<String, Object> unpackMap(byte[] data) throws IOException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
        int size = unpacker.unpackMapHeader();
        Map<String, Object> map = new LinkedHashMap<>();

        for (int i = 0; i < size; i++) {
            String key = unpacker.unpackString();
            switch (unpacker.getNextFormat().getValueType()) {
                case STRING -> map.put(key, unpacker.unpackString());
                case INTEGER -> map.put(key, unpacker.unpackLong());
                case FLOAT -> map.put(key, unpacker.unpackDouble());
                case BOOLEAN -> map.put(key, unpacker.unpackBoolean());
                case ARRAY -> {
                    int arr = unpacker.unpackArrayHeader();
                    List<String> list = new ArrayList<>();
                    for (int j = 0; j < arr; j++) {
                        list.add(unpacker.unpackString());
                    }
                    map.put(key, list);
                }
                default -> unpacker.skipValue();
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    static List<String> castStringList(Object obj) {
        List<String> out = new ArrayList<>();
        if (obj instanceof List<?>) {
            for (Object item : (List<?>) obj) {
                out.add(String.valueOf(item));
            }
        }
        return out;
    }

    static Map<String, Object> mapOf(Object... kvs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(String.valueOf(kvs[i]), kvs[i + 1]);
        }
        return map;
    }

    static double epoch() {
        return System.currentTimeMillis() / 1000.0;
    }
}