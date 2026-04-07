// #include <zmq.h>
// #include <msgpack.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>
// #include <unistd.h>

// void print_msgpack(msgpack_object obj) {
//     if (obj.type != MSGPACK_OBJECT_MAP) {
//         printf("[ERRO] resposta nao eh mapa\n");
//         return;
//     }

//     printf("{ ");

//     for (int i = 0; i < obj.via.map.size; i++) {
//         msgpack_object_kv *kv = &obj.via.map.ptr[i];

//         if (kv->key.type != MSGPACK_OBJECT_STR) {
//             continue;
//         }

//         printf("%.*s: ",
//                (int)kv->key.via.str.size,
//                kv->key.via.str.ptr);

//         if (kv->val.type == MSGPACK_OBJECT_STR) {
//             printf("%.*s",
//                    (int)kv->val.via.str.size,
//                    kv->val.via.str.ptr);
//         }
//         else if (kv->val.type == MSGPACK_OBJECT_ARRAY) {
//             printf("[ ");
//             for (int j = 0; j < kv->val.via.array.size; j++) {
//                 msgpack_object item = kv->val.via.array.ptr[j];

//                 if (item.type == MSGPACK_OBJECT_STR) {
//                     printf("%.*s ",
//                            (int)item.via.str.size,
//                            item.via.str.ptr);
//                 } else {
//                     printf("? ");
//                 }
//             }
//             printf("]");
//         }
//         else if (kv->val.type == MSGPACK_OBJECT_FLOAT32 ||
//                  kv->val.type == MSGPACK_OBJECT_FLOAT64) {
//             printf("%f", kv->val.via.f64);
//         }
//         else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
//             printf("%llu", (unsigned long long)kv->val.via.u64);
//         }
//         else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
//             printf("%lld", (long long)kv->val.via.i64);
//         }
//         else if (kv->val.type == MSGPACK_OBJECT_BOOLEAN) {
//             printf("%s", kv->val.via.boolean ? "true" : "false");
//         }
//         else if (kv->val.type == MSGPACK_OBJECT_NIL) {
//             printf("null");
//         }
//         else {
//             printf("[tipo_nao_tratado]");
//         }

//         if (i < obj.via.map.size - 1) {
//             printf(", ");
//         }
//     }

//     printf("}\n");
// }

// void send_request(void *socket, msgpack_sbuffer *sbuf) {
//     zmq_send(socket, sbuf->data, sbuf->size, 0);

//     char buffer[4096];
//     int size = zmq_recv(socket, buffer, sizeof(buffer), 0);
//     if (size <= 0) {
//         printf("[ERRO] Nenhuma resposta recebida\n");
//         return;
//     }

//     msgpack_unpacked msg;
//     msgpack_unpacked_init(&msg);

//     if (msgpack_unpack_next(&msg, buffer, size, NULL)) {
//         printf("[CLIENT RECEBEU] ");
//         print_msgpack(msg.data);
//     } else {
//         printf("[ERRO] Falha ao decodificar resposta\n");
//     }

//     msgpack_unpacked_destroy(&msg);
// }

// int main() {
//     void *context = zmq_ctx_new();
//     void *socket = zmq_socket(context, ZMQ_REQ);

//     zmq_connect(socket, "tcp://broker:5555");

//     srand(time(NULL));

//     char user[32];
//     sprintf(user, "bot_%d", rand() % 9000 + 1000);

//     printf("[CLIENT C] Iniciando como %s\n", user);

//     while (1) {
//         printf("\n--- NOVO CICLO ---\n");

//         msgpack_sbuffer sbuf;
//         msgpack_sbuffer_init(&sbuf);
//         msgpack_packer pk;
//         msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//         msgpack_pack_map(&pk, 3);

//         msgpack_pack_str(&pk, 4);
//         msgpack_pack_str_body(&pk, "type", 4);
//         msgpack_pack_str(&pk, 5);
//         msgpack_pack_str_body(&pk, "login", 5);

//         msgpack_pack_str(&pk, 4);
//         msgpack_pack_str_body(&pk, "user", 4);
//         msgpack_pack_str(&pk, strlen(user));
//         msgpack_pack_str_body(&pk, user, strlen(user));

//         msgpack_pack_str(&pk, 9);
//         msgpack_pack_str_body(&pk, "timestamp", 9);
//         msgpack_pack_double(&pk, (double)time(NULL));

//         send_request(socket, &sbuf);
//         msgpack_sbuffer_destroy(&sbuf);

//         char channel[32];
//         sprintf(channel, "canal_%d", rand() % 300 + 1);

//         msgpack_sbuffer_init(&sbuf);
//         msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//         msgpack_pack_map(&pk, 4);

//         msgpack_pack_str(&pk, 4);
//         msgpack_pack_str_body(&pk, "type", 4);
//         msgpack_pack_str(&pk, 14);
//         msgpack_pack_str_body(&pk, "create_channel", 14);

//         msgpack_pack_str(&pk, 4);
//         msgpack_pack_str_body(&pk, "user", 4);
//         msgpack_pack_str(&pk, strlen(user));
//         msgpack_pack_str_body(&pk, user, strlen(user));

//         msgpack_pack_str(&pk, 7);
//         msgpack_pack_str_body(&pk, "channel", 7);
//         msgpack_pack_str(&pk, strlen(channel));
//         msgpack_pack_str_body(&pk, channel, strlen(channel));

//         msgpack_pack_str(&pk, 9);
//         msgpack_pack_str_body(&pk, "timestamp", 9);
//         msgpack_pack_double(&pk, (double)time(NULL));

//         send_request(socket, &sbuf);
//         msgpack_sbuffer_destroy(&sbuf);

//         msgpack_sbuffer_init(&sbuf);
//         msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//         msgpack_pack_map(&pk, 3);

//         msgpack_pack_str(&pk, 4);
//         msgpack_pack_str_body(&pk, "type", 4);
//         msgpack_pack_str(&pk, 13);
//         msgpack_pack_str_body(&pk, "list_channels", 13);

//         msgpack_pack_str(&pk, 4);
//         msgpack_pack_str_body(&pk, "user", 4);
//         msgpack_pack_str(&pk, strlen(user));
//         msgpack_pack_str_body(&pk, user, strlen(user));

//         msgpack_pack_str(&pk, 9);
//         msgpack_pack_str_body(&pk, "timestamp", 9);
//         msgpack_pack_double(&pk, (double)time(NULL));

//         send_request(socket, &sbuf);
//         msgpack_sbuffer_destroy(&sbuf);

//         usleep(700000);
//     }

//     zmq_close(socket);
//     zmq_ctx_destroy(context);
//     return 0;
// }

#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#define MAX_CHANNELS 1000

void *sub_socket_global;
char user[64];
char *subscribed[MAX_CHANNELS];
int subscribed_count = 0;

int already_subscribed(const char *channel) {
    for (int i = 0; i < subscribed_count; i++) {
        if (strcmp(subscribed[i], channel) == 0) return 1;
    }
    return 0;
}

double now_ts() {
    return (double)time(NULL);
}

void pack_str(msgpack_packer *pk, const char *s) {
    msgpack_pack_str(pk, strlen(s));
    msgpack_pack_str_body(pk, s, strlen(s));
}

void *subscriber_loop(void *arg) {
    (void)arg;

    while (1) {
        char topic[256] = {0};
        int size1 = zmq_recv(sub_socket_global, topic, sizeof(topic) - 1, 0);
        if (size1 <= 0) continue;
        topic[size1] = '\0';

        char payload[2048];
        int size2 = zmq_recv(sub_socket_global, payload, sizeof(payload), 0);
        if (size2 <= 0) continue;

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        if (msgpack_unpack_next(&msg, payload, size2, NULL)) {
            char text[512] = "";
            double sent = 0;

            msgpack_object obj = msg.data;
            if (obj.type == MSGPACK_OBJECT_MAP) {
                for (int i = 0; i < obj.via.map.size; i++) {
                    msgpack_object_kv *kv = &obj.via.map.ptr[i];

                    if (kv->key.type != MSGPACK_OBJECT_STR) continue;

                    char key[64] = {0};
                    snprintf(key, sizeof(key), "%.*s",
                             (int)kv->key.via.str.size,
                             kv->key.via.str.ptr);

                    if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
                        snprintf(text, sizeof(text), "%.*s",
                                 (int)kv->val.via.str.size,
                                 kv->val.via.str.ptr);
                    } else if (strcmp(key, "published_timestamp") == 0) {
                        if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64)
                            sent = kv->val.via.f64;
                        else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                            sent = (double)kv->val.via.u64;
                    }
                }
            }

            printf("[SUB][%s] canal=%s mensagem=%s envio=%.0f recebimento=%.0f\n",
                   user, topic, text, sent, now_ts());
        }

        msgpack_unpacked_destroy(&msg);
    }

    return NULL;
}

int send_request(void *req, msgpack_sbuffer *sbuf, char *reply, int reply_size) {
    zmq_send(req, sbuf->data, sbuf->size, 0);
    return zmq_recv(req, reply, reply_size, 0);
}

int parse_channels(char *buffer, int size, char channels[][64]) {
    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);
    int count = 0;

    if (msgpack_unpack_next(&msg, buffer, size, NULL)) {
        msgpack_object obj = msg.data;
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];
                if (kv->key.type != MSGPACK_OBJECT_STR) continue;

                char key[64] = {0};
                snprintf(key, sizeof(key), "%.*s",
                         (int)kv->key.via.str.size,
                         kv->key.via.str.ptr);

                if (strcmp(key, "channels") == 0 && kv->val.type == MSGPACK_OBJECT_ARRAY) {
                    for (int j = 0; j < kv->val.via.array.size && j < MAX_CHANNELS; j++) {
                        msgpack_object item = kv->val.via.array.ptr[j];
                        snprintf(channels[count++], 64, "%.*s",
                                 (int)item.via.str.size,
                                 item.via.str.ptr);
                    }
                }
            }
        }
    }

    msgpack_unpacked_destroy(&msg);
    return count;
}

void send_simple_map(void *req, const char *type, const char *channel, const char *message, char *reply, int *reply_len) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    int map_size = 3;
    if (channel) map_size++;
    if (message) map_size++;

    msgpack_pack_map(&pk, map_size);

    pack_str(&pk, "type");
    pack_str(&pk, type);

    pack_str(&pk, "user");
    pack_str(&pk, user);

    pack_str(&pk, "timestamp");
    msgpack_pack_double(&pk, now_ts());

    if (channel) {
        pack_str(&pk, "channel");
        pack_str(&pk, channel);
    }

    if (message) {
        pack_str(&pk, "message");
        pack_str(&pk, message);
    }

    *reply_len = send_request(req, &sbuf, reply, 4096);
    msgpack_sbuffer_destroy(&sbuf);
}

int main() {
    srand(time(NULL));
    sprintf(user, "bot_c_%d", rand() % 9000 + 1000);

    void *context = zmq_ctx_new();

    void *req = zmq_socket(context, ZMQ_REQ);
    zmq_connect(req, "tcp://broker:5555");

    sub_socket_global = zmq_socket(context, ZMQ_SUB);
    zmq_connect(sub_socket_global, "tcp://proxy:5558");

    pthread_t sub_thread;
    pthread_create(&sub_thread, NULL, subscriber_loop, NULL);

    printf("[CLIENT C] Iniciando como %s\n", user);

    char reply[4096];
    int reply_len = 0;

    send_simple_map(req, "login", NULL, NULL, reply, &reply_len);

    while (1) {
        char channels[MAX_CHANNELS][64];

        send_simple_map(req, "list_channels", NULL, NULL, reply, &reply_len);
        int channel_count = parse_channels(reply, reply_len, channels);

        if (channel_count < 5) {
            char new_channel[64];
            sprintf(new_channel, "canal_%d", rand() % 999 + 1);

            send_simple_map(req, "create_channel", new_channel, NULL, reply, &reply_len);
            send_simple_map(req, "list_channels", NULL, NULL, reply, &reply_len);
            channel_count = parse_channels(reply, reply_len, channels);
        }

        while (subscribed_count < 3 && subscribed_count < channel_count) {
            int idx = rand() % channel_count;

            if (!already_subscribed(channels[idx])) {
                zmq_setsockopt(sub_socket_global, ZMQ_SUBSCRIBE, channels[idx], strlen(channels[idx]));
                subscribed[subscribed_count++] = strdup(channels[idx]);
                printf("[SUBSCRIBE][%s] %s\n", user, channels[idx]);
            }
        }

        if (channel_count == 0) {
            sleep(1);
            continue;
        }

        char *publish_channel = channels[rand() % channel_count];

        for (int i = 0; i < 10; i++) {
            char text[128];
            sprintf(text, "mensagem %d do %s", i + 1, user);

            send_simple_map(req, "publish_message", publish_channel, text, reply, &reply_len);
            printf("[PUBLISH][%s] canal=%s texto=%s\n", user, publish_channel, text);
            sleep(1);
        }
    }
}