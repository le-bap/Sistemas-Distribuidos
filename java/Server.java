// import org.zeromq.ZMQ;
// import org.msgpack.core.*;

// import java.io.*;
// import java.nio.file.*;
// import java.util.*;

// public class Server {

//     static Set<String> channels = new HashSet<>();
//     static List<Map<String, Object>> logins = new ArrayList<>();

//     static final String DATA_DIR = "data";
//     static final String CHANNELS_FILE = DATA_DIR + "/channels.json";
//     static final String LOGINS_FILE = DATA_DIR + "/logins.json";

//     public static void main(String[] args) throws Exception {

//         new File(DATA_DIR).mkdirs();

//         loadData();

//         ZMQ.Context context = ZMQ.context(1);
//         ZMQ.Socket socket = context.socket(ZMQ.REP);

//         socket.connect("tcp://broker:5556");

//         System.out.println("[SERVER JAVA] Iniciado");

//         while (true) {
//             byte[] request = socket.recv();

//             MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(request);

//             int mapSize = unpacker.unpackMapHeader();
//             Map<String, Object> msg = new HashMap<>();

//             for (int i = 0; i < mapSize; i++) {
//                 String key = unpacker.unpackString();

//                 switch (unpacker.getNextFormat().getValueType()) {
//                     case STRING:
//                         msg.put(key, unpacker.unpackString());
//                         break;
//                     case FLOAT:
//                         msg.put(key, unpacker.unpackDouble());
//                         break;
//                     default:
//                         unpacker.skipValue();
//                 }
//             }

//             String type = (String) msg.get("type");

//             MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

//             if ("login".equals(type)) {
//                 String user = (String) msg.get("user");

//                 Map<String, Object> login = new HashMap<>();
//                 login.put("user", user);
//                 login.put("timestamp", msg.get("timestamp"));

//                 logins.add(login);
//                 saveLogins();

//                 System.out.println("[LOGIN] " + user);

//                 packer.packMapHeader(3);
//                 packer.packString("status");
//                 packer.packString("ok");

//                 packer.packString("message");
//                 packer.packString("login realizado (" + user + ")");

//                 packer.packString("timestamp");
//                 packer.packDouble(now());
//             }

//             else if ("create_channel".equals(type)) {
//                 String ch = (String) msg.get("channel");

//                 if (channels.contains(ch)) {
//                     System.out.println("[CREATE] exists: " + ch);

//                     packer.packMapHeader(2);
//                     packer.packString("status");
//                     packer.packString("error");
//                 } else {
//                     channels.add(ch);
//                     saveChannels();

//                     System.out.println("[CREATE] ok: " + ch);

//                     packer.packMapHeader(2);
//                     packer.packString("status");
//                     packer.packString("ok");
//                 }
//             }

//             else if ("list_channels".equals(type)) {
//                 System.out.println("[LIST] total=" + channels.size());

//                 packer.packMapHeader(2);
//                 packer.packString("status");
//                 packer.packString("ok");

//                 packer.packString("channels");
//                 packer.packArrayHeader(channels.size());

//                 for (String ch : channels) {
//                     packer.packString(ch);
//                 }
//             }

//             packer.close();
//             socket.send(packer.toByteArray());
//         }
//     }

//     static void loadData() {
//         try {
//             if (Files.exists(Paths.get(CHANNELS_FILE))) {
//                 String content = Files.readString(Paths.get(CHANNELS_FILE));
//                 if (!content.isEmpty()) {
//                     String[] arr = content.replace("[", "").replace("]", "").replace("\"", "").split(",");
//                     for (String c : arr) {
//                         if (!c.trim().isEmpty())
//                             channels.add(c.trim());
//                     }
//                 }
//             }

//             if (Files.exists(Paths.get(LOGINS_FILE))) {
//                 String content = Files.readString(Paths.get(LOGINS_FILE));
//             }

//         } catch (Exception e) {
//             System.out.println("Erro ao carregar dados");
//         }
//     }

//     static void saveChannels() {
//         try (FileWriter fw = new FileWriter(CHANNELS_FILE)) {
//             fw.write("[");
//             int i = 0;
//             for (String ch : channels) {
//                 fw.write("\"" + ch + "\"");
//                 if (i++ < channels.size() - 1) fw.write(",");
//             }
//             fw.write("]");
//         } catch (Exception e) {
//             System.out.println("Erro ao salvar canais");
//         }
//     }

//     static void saveLogins() {
//         try (FileWriter fw = new FileWriter(LOGINS_FILE)) {
//             fw.write("[\n");
//             for (int i = 0; i < logins.size(); i++) {
//                 Map<String, Object> l = logins.get(i);
//                 fw.write(String.format(
//                     "{\"user\":\"%s\",\"timestamp\":%s}",
//                     l.get("user"), l.get("timestamp")
//                 ));
//                 if (i < logins.size() - 1) fw.write(",\n");
//             }
//             fw.write("\n]");
//         } catch (Exception e) {
//             System.out.println("Erro ao salvar logins");
//         }
//     }

//     static double now() {
//         return System.currentTimeMillis() / 1000.0;
//     }
// }
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.zeromq.ZMQ;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Server {
    static final Path DATA_DIR = Paths.get("data");
    static final Path CHANNELS_FILE = DATA_DIR.resolve("channels.json");
    static final Path LOGINS_FILE = DATA_DIR.resolve("logins.json");
    static final Path REQUESTS_FILE = DATA_DIR.resolve("requests.jsonl");
    static final Path PUBLICATIONS_FILE = DATA_DIR.resolve("publications.jsonl");

    static final Set<String> channels = new TreeSet<>();
    static final List<Map<String, Object>> logins = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        new File(DATA_DIR.toString()).mkdirs();
        loadChannels();
        loadLogins();

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket rep = context.socket(ZMQ.REP);
        rep.connect("tcp://broker:5556");

        ZMQ.Socket pub = context.socket(ZMQ.PUB);
        pub.connect("tcp://proxy:5557");

        System.out.println("[SERVER JAVA] Iniciado");

        while (true) {
            byte[] request = rep.recv();
            Map<String, Object> msg = unpackMap(request);
            String type = str(msg.get("type"));
            String user = str(msg.get("user"));
            double now = epoch();

            appendJsonl(REQUESTS_FILE, mapOf(
                    "type", type,
                    "user", user,
                    "received_timestamp", now,
                    "request", msg.toString()
            ));

            Map<String, Object> response;

            if ("login".equals(type)) {
                Map<String, Object> login = new LinkedHashMap<>();
                login.put("user", user);
                login.put("timestamp", num(msg.get("timestamp"), now));
                logins.add(login);
                saveLogins();
                response = mapOf("status", "ok", "message", "login realizado (" + user + ")", "timestamp", epoch());
            } else if ("create_channel".equals(type)) {
                String channel = str(msg.get("channel"));
                if (channels.contains(channel)) {
                    response = mapOf("status", "error", "message", "canal já existe", "timestamp", epoch());
                } else {
                    channels.add(channel);
                    saveChannels();
                    response = mapOf("status", "ok", "message", "canal '" + channel + "' criado", "timestamp", epoch());
                }
            } else if ("list_channels".equals(type)) {
                response = new LinkedHashMap<>();
                response.put("status", "ok");
                response.put("channels", new ArrayList<>(channels));
                response.put("timestamp", epoch());
            } else if ("publish_message".equals(type)) {
                String channel = str(msg.get("channel"));
                String text = str(msg.get("message"));
                if (!channels.contains(channel)) {
                    response = mapOf("status", "error", "message", "canal inexistente", "timestamp", epoch());
                } else {
                    Map<String, Object> publication = new LinkedHashMap<>();
                    publication.put("channel", channel);
                    publication.put("user", user);
                    publication.put("message", text);
                    publication.put("request_timestamp", num(msg.get("timestamp"), epoch()));
                    publication.put("published_timestamp", epoch());
                    publication.put("server", "java");

                    byte[] payload = packMap(publication);
                    pub.sendMore(channel);
                    pub.send(payload);
                    appendJsonl(PUBLICATIONS_FILE, publication);
                    response = mapOf("status", "ok", "message", "mensagem publicada em '" + channel + "'", "timestamp", epoch());
                }
            } else {
                response = mapOf("status", "error", "message", "tipo inválido", "timestamp", epoch());
            }

            rep.send(packMap(response));
        }
    }

    static void loadChannels() {
        try {
            if (!Files.exists(CHANNELS_FILE)) return;
            String text = Files.readString(CHANNELS_FILE, StandardCharsets.UTF_8).trim();
            if (text.length() < 2) return;
            text = text.substring(1, text.length() - 1).trim();
            if (text.isEmpty()) return;
            for (String item : text.split(",")) {
                String cleaned = item.trim();
                if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
                    cleaned = cleaned.substring(1, cleaned.length() - 1);
                }
                if (!cleaned.isEmpty()) channels.add(cleaned);
            }
        } catch (Exception ignored) {}
    }

    static void loadLogins() { }

    static void saveChannels() throws IOException {
        StringBuilder sb = new StringBuilder("[\n");
        int i = 0;
        for (String c : channels) {
            sb.append("  \"").append(c).append("\"");
            if (i++ < channels.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("]\n");
        Files.writeString(CHANNELS_FILE, sb.toString(), StandardCharsets.UTF_8);
    }

    static void saveLogins() throws IOException {
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < logins.size(); i++) {
            Map<String, Object> l = logins.get(i);
            sb.append("  {\"user\":\"").append(l.get("user")).append("\",\"timestamp\":").append(l.get("timestamp")).append("}");
            if (i < logins.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("]\n");
        Files.writeString(LOGINS_FILE, sb.toString(), StandardCharsets.UTF_8);
    }

    static void appendJsonl(Path path, Map<String, Object> entry) throws IOException {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(path.toFile(), true))) {
            bw.write(toJson(entry));
            bw.newLine();
        }
    }

    static String toJson(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("{");
        int i = 0;
        for (Map.Entry<String, Object> e : entry.entrySet()) {
            if (i++ > 0) sb.append(',');
            sb.append('"').append(e.getKey()).append('"').append(':');
            Object v = e.getValue();
            if (v instanceof Number || v instanceof Boolean) sb.append(v);
            else sb.append('"').append(String.valueOf(v).replace("\"", "\\\"")).append('"');
        }
        sb.append('}');
        return sb.toString();
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
                    for (int j = 0; j < arr; j++) list.add(unpacker.unpackString());
                    map.put(key, list);
                }
                default -> unpacker.skipValue();
            }
        }
        return map;
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
            } else p.packString(String.valueOf(v));
        }
        p.close();
        return p.toByteArray();
    }

    static Map<String, Object> mapOf(Object... kvs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) map.put(String.valueOf(kvs[i]), kvs[i + 1]);
        return map;
    }

    static String str(Object o) { return o == null ? "" : String.valueOf(o); }
    static double num(Object o, double fallback) {
        if (o instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(String.valueOf(o)); } catch (Exception e) { return fallback; }
    }
    static double epoch() { return System.currentTimeMillis() / 1000.0; }
}