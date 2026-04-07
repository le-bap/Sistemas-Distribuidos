// #include <zmq.h>
// #include <msgpack.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>
// #include <sys/stat.h>
// #include <sys/types.h>

// #define MAX_CHANNELS 1000
// #define MAX_LOGINS 1000

// char *channels[MAX_CHANNELS];
// int channel_count = 0;

// char *logins[MAX_LOGINS];
// int login_count = 0;

// const char *DATA_DIR = "data";

// const char *CHANNELS_FILE_TXT  = "data/channels.txt";
// const char *LOGINS_FILE_TXT    = "data/logins.txt";

// const char *CHANNELS_FILE_JSON = "data/channels.json";
// const char *LOGINS_FILE_JSON   = "data/logins.json";

// double now_timestamp() {
//     return (double)time(NULL);
// }

// void ensure_data_dirs() {
//     mkdir(DATA_DIR, 0777);
// }

// void pack_key_str(msgpack_packer *pk, const char *key) {
//     msgpack_pack_str(pk, strlen(key));
//     msgpack_pack_str_body(pk, key, strlen(key));
// }

// void pack_val_str(msgpack_packer *pk, const char *value) {
//     msgpack_pack_str(pk, strlen(value));
//     msgpack_pack_str_body(pk, value, strlen(value));
// }

// int channel_exists(const char *name) {
//     for (int i = 0; i < channel_count; i++) {
//         if (strcmp(channels[i], name) == 0) {
//             return 1;
//         }
//     }
//     return 0;
// }

// void save_channels_json() {
//     FILE *f = fopen(CHANNELS_FILE_JSON, "w");
//     if (!f) {
//         perror("[ERRO] nao foi possivel abrir channels.json");
//         return;
//     }

//     fprintf(f, "[");
//     for (int i = 0; i < channel_count; i++) {
//         fprintf(f, "\"%s\"", channels[i]);
//         if (i < channel_count - 1) {
//             fprintf(f, ",");
//         }
//     }
//     fprintf(f, "]");

//     fclose(f);
// }

// void save_logins_json() {
//     FILE *f = fopen(LOGINS_FILE_JSON, "w");
//     if (!f) {
//         perror("[ERRO] nao foi possivel abrir logins.json");
//         return;
//     }

//     fprintf(f, "[\n");
//     for (int i = 0; i < login_count; i++) {
//         char user[128] = "";
//         long timestamp = 0;

//         sscanf(logins[i], "%127s %ld", user, &timestamp);

//         fprintf(f, "  {\"user\":\"%s\",\"timestamp\":%ld}", user, timestamp);
//         if (i < login_count - 1) {
//             fprintf(f, ",");
//         }
//         fprintf(f, "\n");
//     }
//     fprintf(f, "]\n");

//     fclose(f);
// }

// void load_channels() {
//     FILE *f = fopen(CHANNELS_FILE_TXT, "r");
//     if (!f) return;

//     char line[256];
//     while (fgets(line, sizeof(line), f)) {
//         line[strcspn(line, "\n")] = 0;

//         if (strlen(line) == 0) continue;
//         if (channel_count >= MAX_CHANNELS) break;

//         channels[channel_count++] = strdup(line);
//     }

//     fclose(f);
// }

// void save_channels() {
//     FILE *f = fopen(CHANNELS_FILE_TXT, "w");
//     if (!f) {
//         perror("[ERRO] nao foi possivel abrir channels.txt");
//         return;
//     }

//     for (int i = 0; i < channel_count; i++) {
//         fprintf(f, "%s\n", channels[i]);
//     }

//     fclose(f);
//     save_channels_json();
// }

// void load_logins() {
//     FILE *f = fopen(LOGINS_FILE_TXT, "r");
//     if (!f) return;

//     char line[256];
//     while (fgets(line, sizeof(line), f)) {
//         line[strcspn(line, "\n")] = 0;

//         if (strlen(line) == 0) continue;
//         if (login_count >= MAX_LOGINS) break;

//         logins[login_count++] = strdup(line);
//     }

//     fclose(f);
// }

// void save_login(const char *user, double timestamp) {
//     FILE *f = fopen(LOGINS_FILE_TXT, "a");
//     if (!f) {
//         perror("[ERRO] nao foi possivel abrir logins.txt");
//         return;
//     }

//     fprintf(f, "%s %.0f\n", user, timestamp);
//     fclose(f);

//     if (login_count < MAX_LOGINS) {
//         char line[256];
//         snprintf(line, sizeof(line), "%s %.0f", user, timestamp);
//         logins[login_count++] = strdup(line);
//     }

//     save_logins_json();
// }

// void add_channel(const char *name) {
//     if (channel_count < MAX_CHANNELS) {
//         channels[channel_count++] = strdup(name);
//         save_channels();
//     }
// }

// int main() {
//     ensure_data_dirs();

//     load_channels();
//     load_logins();

//     /* gera json inicial baseado no txt já carregado */
//     save_channels_json();
//     save_logins_json();

//     void *context = zmq_ctx_new();
//     void *socket = zmq_socket(context, ZMQ_REP);

//     zmq_connect(socket, "tcp://broker:5556");

//     printf("[SERVER C] Iniciado\n");
//     printf("[SERVER C] canais carregados: %d\n", channel_count);
//     printf("[SERVER C] logins carregados: %d\n", login_count);

//     while (1) {
//         char buffer[4096];
//         int size = zmq_recv(socket, buffer, sizeof(buffer), 0);
//         if (size <= 0) continue;

//         // printf("[DEBUG C SERVER] bytes recebidos: %d\n", size);

//         msgpack_unpacked msg;
//         msgpack_unpacked_init(&msg);

//         if (!msgpack_unpack_next(&msg, buffer, size, NULL)) {
//             msgpack_unpacked_destroy(&msg);
//             continue;
//         }

//         msgpack_object obj = msg.data;

//         char type[32] = "";
//         char user[64] = "";
//         char channel[64] = "";
//         double timestamp = 0;

//         if (obj.type == MSGPACK_OBJECT_MAP) {
//             for (int i = 0; i < obj.via.map.size; i++) {
//                 msgpack_object_kv *kv = &obj.via.map.ptr[i];

//                 if (kv->key.type != MSGPACK_OBJECT_STR) continue;

//                 char key[64] = {0};
//                 snprintf(key, sizeof(key), "%.*s",
//                          (int)kv->key.via.str.size,
//                          kv->key.via.str.ptr);

//                 if (strcmp(key, "type") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
//                     snprintf(type, sizeof(type), "%.*s",
//                              (int)kv->val.via.str.size,
//                              kv->val.via.str.ptr);
//                 }
//                 else if (strcmp(key, "user") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
//                     snprintf(user, sizeof(user), "%.*s",
//                              (int)kv->val.via.str.size,
//                              kv->val.via.str.ptr);
//                 }
//                 else if (strcmp(key, "channel") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
//                     snprintf(channel, sizeof(channel), "%.*s",
//                              (int)kv->val.via.str.size,
//                              kv->val.via.str.ptr);
//                 }
//                 else if (strcmp(key, "timestamp") == 0) {
//                     if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64) {
//                         timestamp = kv->val.via.f64;
//                     } else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
//                         timestamp = (double)kv->val.via.u64;
//                     } else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
//                         timestamp = (double)kv->val.via.i64;
//                     }
//                 }
//             }
//         }

//         msgpack_sbuffer sbuf;
//         msgpack_sbuffer_init(&sbuf);
//         msgpack_packer pk;
//         msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//         if (strcmp(type, "login") == 0) {
//             save_login(user, timestamp);

//             printf("[LOGIN] %s\n", user);

//             char message[128];
//             snprintf(message, sizeof(message), "login realizado (%s)", user);

//             msgpack_pack_map(&pk, 3);

//             pack_key_str(&pk, "status");
//             pack_val_str(&pk, "ok");

//             pack_key_str(&pk, "message");
//             pack_val_str(&pk, message);

//             pack_key_str(&pk, "timestamp");
//             msgpack_pack_double(&pk, now_timestamp());
//         }
//         else if (strcmp(type, "create_channel") == 0) {
//             if (channel_exists(channel)) {
//                 printf("[CREATE] exists: %s\n", channel);

//                 msgpack_pack_map(&pk, 3);

//                 pack_key_str(&pk, "status");
//                 pack_val_str(&pk, "error");

//                 pack_key_str(&pk, "message");
//                 pack_val_str(&pk, "canal já existe");

//                 pack_key_str(&pk, "timestamp");
//                 msgpack_pack_double(&pk, now_timestamp());
//             } else {
//                 add_channel(channel);

//                 printf("[CREATE] ok: %s\n", channel);

//                 char message[128];
//                 snprintf(message, sizeof(message), "canal '%s' criado", channel);

//                 msgpack_pack_map(&pk, 3);

//                 pack_key_str(&pk, "status");
//                 pack_val_str(&pk, "ok");

//                 pack_key_str(&pk, "message");
//                 pack_val_str(&pk, message);

//                 pack_key_str(&pk, "timestamp");
//                 msgpack_pack_double(&pk, now_timestamp());
//             }
//         }
//         else if (strcmp(type, "list_channels") == 0) {
//             printf("[LIST CHANNELS] ");

//             msgpack_pack_map(&pk, 3);

//             pack_key_str(&pk, "status");
//             pack_val_str(&pk, "ok");

//             pack_key_str(&pk, "channels");
//             msgpack_pack_array(&pk, channel_count);

//             for (int i = 0; i < channel_count; i++) {
//                 msgpack_pack_str(&pk, strlen(channels[i]));
//                 msgpack_pack_str_body(&pk, channels[i], strlen(channels[i]));
//             }

//             pack_key_str(&pk, "timestamp");
//             msgpack_pack_double(&pk, now_timestamp());

//             printf("total=%d\n", channel_count);
//         }
//         else {
//             msgpack_pack_map(&pk, 3);

//             pack_key_str(&pk, "status");
//             pack_val_str(&pk, "error");

//             pack_key_str(&pk, "message");
//             pack_val_str(&pk, "tipo invalido");

//             pack_key_str(&pk, "timestamp");
//             msgpack_pack_double(&pk, now_timestamp());
//         }

//         // printf("[DEBUG C SERVER] bytes enviados: %ld\n", (long)sbuf.size);
//         zmq_send(socket, sbuf.data, sbuf.size, 0);

//         msgpack_sbuffer_destroy(&sbuf);
//         msgpack_unpacked_destroy(&msg);
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
#include <sys/stat.h>
#include <sys/types.h>

#define MAX_CHANNELS 1000
#define MAX_LOGINS 1000
#define BUF 4096

char *channels[MAX_CHANNELS];
int channel_count = 0;
char *logins[MAX_LOGINS];
int login_count = 0;

const char *DATA_DIR = "data";
const char *CHANNELS_FILE = "data/channels.json";
const char *LOGINS_FILE = "data/logins.json";
const char *REQUESTS_FILE = "data/requests.jsonl";
const char *PUBLICATIONS_FILE = "data/publications.jsonl";

double now_ts() { return (double)time(NULL); }

void ensure_data_dirs() { mkdir(DATA_DIR, 0777); }

void pack_str_kv(msgpack_packer *pk, const char *key, const char *value) {
    msgpack_pack_str(pk, strlen(key));
    msgpack_pack_str_body(pk, key, strlen(key));
    msgpack_pack_str(pk, strlen(value));
    msgpack_pack_str_body(pk, value, strlen(value));
}

void save_channels() {
    FILE *f = fopen(CHANNELS_FILE, "w");
    if (!f) return;
    fprintf(f, "[\n");
    for (int i = 0; i < channel_count; i++) {
        fprintf(f, "  \"%s\"", channels[i]);
        if (i < channel_count - 1) fprintf(f, ",");
        fprintf(f, "\n");
    }
    fprintf(f, "]\n");
    fclose(f);
}

void save_logins() {
    FILE *f = fopen(LOGINS_FILE, "w");
    if (!f) return;
    fprintf(f, "[\n");
    for (int i = 0; i < login_count; i++) {
        fprintf(f, "%s", logins[i]);
        if (i < login_count - 1) fprintf(f, ",");
        fprintf(f, "\n");
    }
    fprintf(f, "]\n");
    fclose(f);
}

void append_line(const char *path, const char *line) {
    FILE *f = fopen(path, "a");
    if (!f) return;
    fprintf(f, "%s\n", line);
    fclose(f);
}

int channel_exists(const char *name) {
    for (int i = 0; i < channel_count; i++) if (strcmp(channels[i], name) == 0) return 1;
    return 0;
}

void load_channels() {
    FILE *f = fopen(CHANNELS_FILE, "r");
    if (!f) return;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        char *start = strchr(line, '"');
        char *end = start ? strrchr(line, '"') : NULL;
        if (!start || !end || end <= start) continue;
        *end = '\0';
        channels[channel_count++] = strdup(start + 1);
    }
    fclose(f);
}

void send_publication(void *pub_socket, const char *channel, const char *user, const char *message, double req_ts) {
    msgpack_sbuffer sbuf; msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk; msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    double published = now_ts();

    msgpack_pack_map(&pk, 6);
    pack_str_kv(&pk, "channel", channel);
    pack_str_kv(&pk, "user", user);
    pack_str_kv(&pk, "message", message);
    msgpack_pack_str(&pk, 17); msgpack_pack_str_body(&pk, "request_timestamp", 17); msgpack_pack_double(&pk, req_ts);
    msgpack_pack_str(&pk, 19); msgpack_pack_str_body(&pk, "published_timestamp", 19); msgpack_pack_double(&pk, published);
    pack_str_kv(&pk, "server", "c");

    zmq_send(pub_socket, channel, strlen(channel), ZMQ_SNDMORE);
    zmq_send(pub_socket, sbuf.data, sbuf.size, 0);

    char line[1024];
    snprintf(line, sizeof(line), "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\",\"request_timestamp\":%.0f,\"published_timestamp\":%.0f,\"server\":\"c\"}", channel, user, message, req_ts, published);
    append_line(PUBLICATIONS_FILE, line);
    msgpack_sbuffer_destroy(&sbuf);
}

int main() {
    ensure_data_dirs();
    load_channels();

    void *context = zmq_ctx_new();
    void *rep = zmq_socket(context, ZMQ_REP);
    zmq_connect(rep, "tcp://broker:5556");

    void *pub = zmq_socket(context, ZMQ_PUB);
    zmq_connect(pub, "tcp://proxy:5557");

    printf("[SERVER C] Iniciado\n");

    while (1) {
        char buffer[BUF];
        int size = zmq_recv(rep, buffer, sizeof(buffer), 0);
        if (size <= 0) continue;

        msgpack_unpacked msg; msgpack_unpacked_init(&msg);
        if (!msgpack_unpack_next(&msg, buffer, size, NULL)) { msgpack_unpacked_destroy(&msg); continue; }

        char type[64] = ""; char user[128] = ""; char channel[128] = ""; char message[512] = ""; double timestamp = now_ts();
        msgpack_object obj = msg.data;
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];
                if (kv->key.type != MSGPACK_OBJECT_STR) continue;
                char key[64] = {0}; snprintf(key, sizeof(key), "%.*s", (int)kv->key.via.str.size, kv->key.via.str.ptr);
                if (strcmp(key, "type") == 0 && kv->val.type == MSGPACK_OBJECT_STR) snprintf(type, sizeof(type), "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "user") == 0 && kv->val.type == MSGPACK_OBJECT_STR) snprintf(user, sizeof(user), "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "channel") == 0 && kv->val.type == MSGPACK_OBJECT_STR) snprintf(channel, sizeof(channel), "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR) snprintf(message, sizeof(message), "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "timestamp") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64) timestamp = kv->val.via.f64;
                    else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) timestamp = (double)kv->val.via.u64;
                }
            }
        }

        char request_line[1024];
        snprintf(request_line, sizeof(request_line), "{\"type\":\"%s\",\"user\":\"%s\",\"channel\":\"%s\",\"message\":\"%s\",\"received_timestamp\":%.0f}", type, user, channel, message, now_ts());
        append_line(REQUESTS_FILE, request_line);

        msgpack_sbuffer sbuf; msgpack_sbuffer_init(&sbuf);
        msgpack_packer pk; msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

        if (strcmp(type, "login") == 0) {
            char entry[256]; snprintf(entry, sizeof(entry), "  {\"user\":\"%s\",\"timestamp\":%.0f}", user, timestamp);
            if (login_count < MAX_LOGINS) logins[login_count++] = strdup(entry);
            save_logins();
            msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "ok"); pack_str_kv(&pk, "message", "login realizado");
            msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
        } else if (strcmp(type, "create_channel") == 0) {
            if (channel_exists(channel)) {
                msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "error"); pack_str_kv(&pk, "message", "canal ja existe");
                msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
            } else {
                channels[channel_count++] = strdup(channel); save_channels();
                msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "ok"); pack_str_kv(&pk, "message", "canal criado");
                msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
            }
        } else if (strcmp(type, "list_channels") == 0) {
            msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "ok");
            msgpack_pack_str(&pk, 8); msgpack_pack_str_body(&pk, "channels", 8); msgpack_pack_array(&pk, channel_count);
            for (int i = 0; i < channel_count; i++) { msgpack_pack_str(&pk, strlen(channels[i])); msgpack_pack_str_body(&pk, channels[i], strlen(channels[i])); }
            msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
        } else if (strcmp(type, "publish_message") == 0) {
            if (!channel_exists(channel)) {
                msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "error"); pack_str_kv(&pk, "message", "canal inexistente");
                msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
            } else {
                send_publication(pub, channel, user, message, timestamp);
                msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "ok"); pack_str_kv(&pk, "message", "mensagem publicada");
                msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
            }
        } else {
            msgpack_pack_map(&pk, 3); pack_str_kv(&pk, "status", "error"); pack_str_kv(&pk, "message", "tipo invalido");
            msgpack_pack_str(&pk, 9); msgpack_pack_str_body(&pk, "timestamp", 9); msgpack_pack_double(&pk, now_ts());
        }

        zmq_send(rep, sbuf.data, sbuf.size, 0);
        msgpack_sbuffer_destroy(&sbuf);
        msgpack_unpacked_destroy(&msg);
    }
}