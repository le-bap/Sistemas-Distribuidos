import zmq

context = zmq.Context()
poller = zmq.Poller()

client_socket = context.socket(zmq.ROUTER)
client_socket.bind("tcp://*:5555")
poller.register(client_socket, zmq.POLLIN)

server_socket = context.socket(zmq.DEALER)
server_socket.bind("tcp://*:5556")
poller.register(server_socket, zmq.POLLIN)

print("[BROKER] Iniciado...", flush=True)

while True:
    socks = dict(poller.poll())

    if socks.get(client_socket) == zmq.POLLIN:
        message = client_socket.recv_multipart()
        server_socket.send_multipart(message)

    if socks.get(server_socket) == zmq.POLLIN:
        message = server_socket.recv_multipart()
        client_socket.send_multipart(message)