#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

void send_request(void *socket, msgpack_sbuffer *sbuf) {
    zmq_send(socket, sbuf->data, sbuf->size, 0);

    char buffer[4096];
    int size = zmq_recv(socket, buffer, sizeof(buffer), 0);

    printf("[RESPONSE] %.*s\n", size, buffer);
}

int main() {
    void *context = zmq_ctx_new();
    void *socket = zmq_socket(context, ZMQ_REQ);

    zmq_connect(socket, "tcp://broker:5555");

    srand(time(NULL));

    char user[32];
    sprintf(user, "bot_%d", rand() % 9000 + 1000);

    printf("[CLIENT C] Iniciando como %s\n", user);

    while (1) {
        printf("\n--- NOVO CICLO ---\n");

        msgpack_sbuffer sbuf;
        msgpack_sbuffer_init(&sbuf);
        msgpack_packer pk;
        msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

        // LOGIN
        msgpack_pack_map(&pk, 3);
        msgpack_pack_str(&pk, 4); msgpack_pack_str_body(&pk, "type", 4);
        msgpack_pack_str(&pk, 5); msgpack_pack_str_body(&pk, "login", 5);

        msgpack_pack_str(&pk, 4); msgpack_pack_str_body(&pk, "user", 4);
        msgpack_pack_str(&pk, strlen(user)); msgpack_pack_str_body(&pk, user, strlen(user));

        msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9);
        msgpack_pack_double(&pk, (double)time(NULL));

        send_request(socket, &sbuf);
        msgpack_sbuffer_destroy(&sbuf);

        // CREATE CHANNEL
        char channel[32];
        sprintf(channel, "canal_%d", rand() % 300 + 1);

        msgpack_sbuffer_init(&sbuf);
        msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

        msgpack_pack_map(&pk, 4);
        msgpack_pack_str(&pk, 4); msgpack_pack_str_body(&pk, "type", 4);
        msgpack_pack_str(&pk, 14); msgpack_pack_str_body(&pk, "create_channel", 14);

        msgpack_pack_str(&pk, 4); msgpack_pack_str_body(&pk, "user", 4);
        msgpack_pack_str(&pk, strlen(user)); msgpack_pack_str_body(&pk, user, strlen(user));

        msgpack_pack_str(&pk, 7); msgpack_pack_str_body(&pk, "channel", 7);
        msgpack_pack_str(&pk, strlen(channel)); msgpack_pack_str_body(&pk, channel, strlen(channel));

        msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9);
        msgpack_pack_double(&pk, (double)time(NULL));

        send_request(socket, &sbuf);
        msgpack_sbuffer_destroy(&sbuf);

        // LIST CHANNELS
        msgpack_sbuffer_init(&sbuf);
        msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

        msgpack_pack_map(&pk, 3);
        msgpack_pack_str(&pk, 4); msgpack_pack_str_body(&pk, "type", 4);
        msgpack_pack_str(&pk, 13); msgpack_pack_str_body(&pk, "list_channels", 13);

        msgpack_pack_str(&pk, 4); msgpack_pack_str_body(&pk, "user", 4);
        msgpack_pack_str(&pk, strlen(user)); msgpack_pack_str_body(&pk, user, strlen(user));

        msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9);
        msgpack_pack_double(&pk, (double)time(NULL));

        send_request(socket, &sbuf);
        msgpack_sbuffer_destroy(&sbuf);

        usleep(600000);
    }

    zmq_close(socket);
    zmq_ctx_destroy(context);
    return 0;
}