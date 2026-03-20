import org.zeromq.ZMQ;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;
import java.io.IOException;

import java.util.Random;

public class Client {
    public static void main(String[] args) throws Exception {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REQ);

        socket.connect("tcp://broker:5555");

        Random rand = new Random();
        String user = "bot_" + (rand.nextInt(9000) + 1000);

        System.out.println("[CLIENT JAVA] Iniciando como " + user);

        while (true) {
            System.out.println("\n--- NOVO CICLO ---");

            send(socket, packLogin(user));

            String channel = "canal_" + (rand.nextInt(300) + 1);
            send(socket, packCreate(user, channel));

            send(socket, packList(user));

            Thread.sleep(1200);
        }
    }

    static void send(ZMQ.Socket socket, byte[] data) throws Exception {
        socket.send(data);

        byte[] reply = socket.recv();
        unpackAndPrint(reply);
    }

    static byte[] packLogin(String user) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(3);
        packer.packString("type");
        packer.packString("login");

        packer.packString("user");
        packer.packString(user);

        packer.packString("timestamp");
        packer.packDouble(System.currentTimeMillis() / 1000.0);

        packer.close();
        return packer.toByteArray();
    }

    static byte[] packCreate(String user, String channel) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(4);
        packer.packString("type");
        packer.packString("create_channel");

        packer.packString("user");
        packer.packString(user);

        packer.packString("channel");
        packer.packString(channel);

        packer.packString("timestamp");
        packer.packDouble(System.currentTimeMillis() / 1000.0);

        packer.close();
        return packer.toByteArray();
    }

    static byte[] packList(String user) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();

        packer.packMapHeader(3);
        packer.packString("type");
        packer.packString("list_channels");

        packer.packString("user");
        packer.packString(user);

        packer.packString("timestamp");
        packer.packDouble(System.currentTimeMillis() / 1000.0);

        packer.close();
        return packer.toByteArray();
    }

    public static void unpackAndPrint(byte[] response) throws IOException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(response);

        int mapSize = unpacker.unpackMapHeader();

        String status = null;
        String message = null;
        Double timestamp = null;
        StringBuilder channelsStr = new StringBuilder();

        for (int i = 0; i < mapSize; i++) {
            String key = unpacker.unpackString();

            switch (key) {
                case "status":
                    status = unpacker.unpackString();
                    break;

                case "message":
                    message = unpacker.unpackString();
                    break;

                case "timestamp":
                    if (unpacker.getNextFormat().getValueType().isFloatType()) {
                        timestamp = unpacker.unpackDouble();
                    } else if (unpacker.getNextFormat().getValueType().isIntegerType()) {
                        timestamp = (double) unpacker.unpackLong();
                    }
                    break;

                case "channels":
                    int arraySize = unpacker.unpackArrayHeader();
                    channelsStr.append("[ ");
                    for (int j = 0; j < arraySize; j++) {
                        channelsStr.append(unpacker.unpackString()).append(" ");
                    }
                    channelsStr.append("]");
                    break;

                default:
                    unpacker.skipValue();
                    break;
            }
        }

        System.out.print("[RESPONSE] { ");
        if (status != null) System.out.print("status: " + status + ", ");
        if (message != null) System.out.print("message: " + message + ", ");
        if (channelsStr.length() > 0) System.out.print("channels: " + channelsStr + ", ");
        if (timestamp != null) System.out.print("timestamp: " + timestamp + ", ");
        System.out.println("}");
    }
}