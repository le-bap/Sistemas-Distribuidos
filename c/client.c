// #include <zmq.h>
// #include <msgpack.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// #include <time.h>
// #include <pthread.h>

// #define MAX_CANAIS 100
// #define BUFFER 4096

// void *contexto;
// void *req_socket;
// void *sub_socket;

// char usuario[64];
// char canais_inscritos[MAX_CANAIS][64];
// int qtd_inscritos = 0;

// double agora() {
//     return (double)time(NULL);
// }

// int ja_inscrito(char *canal) {
//     for (int i = 0; i < qtd_inscritos; i++) {
//         if (strcmp(canais_inscritos[i], canal) == 0) {
//             return 1;
//         }
//     }
//     return 0;
// }

// void adicionar_inscricao(char *canal) {
//     if (qtd_inscritos < MAX_CANAIS) {
//         strcpy(canais_inscritos[qtd_inscritos], canal);
//         qtd_inscritos++;
//     }
// }

// void enviar_mensagem(void *socket, msgpack_sbuffer *sbuf) {
//     zmq_send(socket, sbuf->data, sbuf->size, 0);
// }

// void *receber_publicacoes(void *arg) {
//     while (1) {
//         zmq_msg_t topico_msg;
//         zmq_msg_t conteudo_msg;

//         zmq_msg_init(&topico_msg);
//         zmq_msg_init(&conteudo_msg);

//         zmq_msg_recv(&topico_msg, sub_socket, 0);
//         zmq_msg_recv(&conteudo_msg, sub_socket, 0);

//         char canal[64];
//         memset(canal, 0, sizeof(canal));
//         memcpy(canal, zmq_msg_data(&topico_msg), zmq_msg_size(&topico_msg));

//         char *dados = (char *)zmq_msg_data(&conteudo_msg);
//         int tamanho = zmq_msg_size(&conteudo_msg);

//         msgpack_unpacked msg;
//         msgpack_unpacked_init(&msg);

//         if (msgpack_unpack_next(&msg, dados, tamanho, NULL)) {
//             msgpack_object obj = msg.data;

//             char texto[256] = "";
//             double envio = 0;

//             if (obj.type == MSGPACK_OBJECT_MAP) {
//                 for (int i = 0; i < obj.via.map.size; i++) {
//                     msgpack_object_kv *kv = &obj.via.map.ptr[i];

//                     if (kv->key.type != MSGPACK_OBJECT_STR) continue;

//                     char key[64] = {0};
//                     snprintf(key, sizeof(key), "%.*s",
//                              (int)kv->key.via.str.size,
//                              kv->key.via.str.ptr);

//                     if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
//                         snprintf(texto, sizeof(texto), "%.*s",
//                                  (int)kv->val.via.str.size,
//                                  kv->val.via.str.ptr);
//                     }

//                     if (strcmp(key, "published_timestamp") == 0) {
//                         if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64) {
//                             envio = kv->val.via.f64;
//                         } else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
//                             envio = (double)kv->val.via.u64;
//                         }
//                     }
//                 }
//             }

//             printf("[MENSAGEM RECEBIDA] canal=%s | mensagem=%s | envio=%.0f | recebimento=%.0f\n",
//                    canal, texto, envio, agora());
//         }

//         msgpack_unpacked_destroy(&msg);
//         zmq_msg_close(&topico_msg);
//         zmq_msg_close(&conteudo_msg);
//     }

//     return NULL;
// }

// void fazer_login() {
//     msgpack_sbuffer sbuf;
//     msgpack_sbuffer_init(&sbuf);
//     msgpack_packer pk;
//     msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//     msgpack_pack_map(&pk, 3);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "type", 4);
//     msgpack_pack_str(&pk, 5);
//     msgpack_pack_str_body(&pk, "login", 5);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "user", 4);
//     msgpack_pack_str(&pk, strlen(usuario));
//     msgpack_pack_str_body(&pk, usuario, strlen(usuario));

//     msgpack_pack_str(&pk, 9);
//     msgpack_pack_str_body(&pk, "timestamp", 9);
//     msgpack_pack_double(&pk, agora());

//     enviar_mensagem(req_socket, &sbuf);

//     char resposta[BUFFER];
//     zmq_recv(req_socket, resposta, sizeof(resposta), 0);

//     printf("[LOGIN] enviado\n");

//     msgpack_sbuffer_destroy(&sbuf);
// }

// int listar_canais(char canais[][64]) {
//     msgpack_sbuffer sbuf;
//     msgpack_sbuffer_init(&sbuf);
//     msgpack_packer pk;
//     msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//     msgpack_pack_map(&pk, 3);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "type", 4);
//     msgpack_pack_str(&pk, 13);
//     msgpack_pack_str_body(&pk, "list_channels", 13);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "user", 4);
//     msgpack_pack_str(&pk, strlen(usuario));
//     msgpack_pack_str_body(&pk, usuario, strlen(usuario));

//     msgpack_pack_str(&pk, 9);
//     msgpack_pack_str_body(&pk, "timestamp", 9);
//     msgpack_pack_double(&pk, agora());

//     enviar_mensagem(req_socket, &sbuf);

//     char resposta[BUFFER];
//     int tamanho = zmq_recv(req_socket, resposta, sizeof(resposta), 0);

//     int qtd = 0;

//     msgpack_unpacked msg;
//     msgpack_unpacked_init(&msg);

//     if (msgpack_unpack_next(&msg, resposta, tamanho, NULL)) {
//         msgpack_object obj = msg.data;

//         if (obj.type == MSGPACK_OBJECT_MAP) {
//             for (int i = 0; i < obj.via.map.size; i++) {
//                 msgpack_object_kv *kv = &obj.via.map.ptr[i];

//                 if (kv->key.type != MSGPACK_OBJECT_STR) continue;

//                 char key[64] = {0};
//                 snprintf(key, sizeof(key), "%.*s",
//                          (int)kv->key.via.str.size,
//                          kv->key.via.str.ptr);

//                 if (strcmp(key, "channels") == 0 && kv->val.type == MSGPACK_OBJECT_ARRAY) {
//                     for (int j = 0; j < kv->val.via.array.size; j++) {
//                         msgpack_object item = kv->val.via.array.ptr[j];
//                         if (item.type == MSGPACK_OBJECT_STR) {
//                             snprintf(canais[qtd], 64, "%.*s",
//                                      (int)item.via.str.size,
//                                      item.via.str.ptr);
//                             qtd++;
//                         }
//                     }
//                 }
//             }
//         }
//     }

//     msgpack_unpacked_destroy(&msg);
//     msgpack_sbuffer_destroy(&sbuf);

//     return qtd;
// }

// void criar_canal() {
//     char canal[64];
//     sprintf(canal, "canal_%d", 1 + rand() % 999);

//     msgpack_sbuffer sbuf;
//     msgpack_sbuffer_init(&sbuf);
//     msgpack_packer pk;
//     msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//     msgpack_pack_map(&pk, 4);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "type", 4);
//     msgpack_pack_str(&pk, 14);
//     msgpack_pack_str_body(&pk, "create_channel", 14);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "user", 4);
//     msgpack_pack_str(&pk, strlen(usuario));
//     msgpack_pack_str_body(&pk, usuario, strlen(usuario));

//     msgpack_pack_str(&pk, 7);
//     msgpack_pack_str_body(&pk, "channel", 7);
//     msgpack_pack_str(&pk, strlen(canal));
//     msgpack_pack_str_body(&pk, canal, strlen(canal));

//     msgpack_pack_str(&pk, 9);
//     msgpack_pack_str_body(&pk, "timestamp", 9);
//     msgpack_pack_double(&pk, agora());

//     enviar_mensagem(req_socket, &sbuf);

//     char resposta[BUFFER];
//     zmq_recv(req_socket, resposta, sizeof(resposta), 0);

//     printf("[CREATE CHANNEL] %s\n", canal);

//     msgpack_sbuffer_destroy(&sbuf);
// }

// void se_inscrever(char canais[][64], int qtd) {
//     char disponiveis[MAX_CANAIS][64];
//     int qtd_disponiveis = 0;

//     for (int i = 0; i < qtd; i++) {
//         if (!ja_inscrito(canais[i])) {
//             strcpy(disponiveis[qtd_disponiveis], canais[i]);
//             qtd_disponiveis++;
//         }
//     }

//     if (qtd_disponiveis == 0) return;

//     int pos = rand() % qtd_disponiveis;
//     char *canal = disponiveis[pos];

//     zmq_setsockopt(sub_socket, ZMQ_SUBSCRIBE, canal, strlen(canal));
//     adicionar_inscricao(canal);

//     printf("[SUBSCRIBE] %s inscrito em %s\n", usuario, canal);
// }

// void publicar_mensagem(char *canal, int numero) {
//     char texto[256];
//     sprintf(texto, "mensagem %d do %s", numero, usuario);

//     msgpack_sbuffer sbuf;
//     msgpack_sbuffer_init(&sbuf);
//     msgpack_packer pk;
//     msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

//     msgpack_pack_map(&pk, 5);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "type", 4);
//     msgpack_pack_str(&pk, 15);
//     msgpack_pack_str_body(&pk, "publish_message", 15);

//     msgpack_pack_str(&pk, 4);
//     msgpack_pack_str_body(&pk, "user", 4);
//     msgpack_pack_str(&pk, strlen(usuario));
//     msgpack_pack_str_body(&pk, usuario, strlen(usuario));

//     msgpack_pack_str(&pk, 7);
//     msgpack_pack_str_body(&pk, "channel", 7);
//     msgpack_pack_str(&pk, strlen(canal));
//     msgpack_pack_str_body(&pk, canal, strlen(canal));

//     msgpack_pack_str(&pk, 7);
//     msgpack_pack_str_body(&pk, "message", 7);
//     msgpack_pack_str(&pk, strlen(texto));
//     msgpack_pack_str_body(&pk, texto, strlen(texto));

//     msgpack_pack_str(&pk, 9);
//     msgpack_pack_str_body(&pk, "timestamp", 9);
//     msgpack_pack_double(&pk, agora());

//     enviar_mensagem(req_socket, &sbuf);

//     char resposta[BUFFER];
//     zmq_recv(req_socket, resposta, sizeof(resposta), 0);

//     printf("[PUBLISH] %s -> %s\n", texto, canal);

//     msgpack_sbuffer_destroy(&sbuf);
// }

// int main() {
//     setvbuf(stdout, NULL, _IONBF, 0);
//     srand(time(NULL));

//     sprintf(usuario, "bot_c_%d", 1000 + rand() % 9000);

//     contexto = zmq_ctx_new();
//     req_socket = zmq_socket(contexto, ZMQ_REQ);
//     sub_socket = zmq_socket(contexto, ZMQ_SUB);

//     zmq_connect(req_socket, "tcp://broker:5555");
//     zmq_connect(sub_socket, "tcp://proxy:5558");

//     pthread_t thread_sub;
//     pthread_create(&thread_sub, NULL, receber_publicacoes, NULL);

//     printf("[CLIENT C] Bot iniciado: %s\n", usuario);

//     fazer_login();

//     while (1) {
//         char canais[MAX_CANAIS][64];
//         int qtd_canais = listar_canais(canais);

//         if (qtd_canais < 5) {
//             criar_canal();
//             qtd_canais = listar_canais(canais);
//         }

//         if (qtd_inscritos < 3) {
//             se_inscrever(canais, qtd_canais);
//         }

//         if (qtd_canais == 0) {
//             sleep(1);
//             continue;
//         }

//         char *canal_escolhido = canais[rand() % qtd_canais];

//         for (int i = 0; i < 10; i++) {
//             publicar_mensagem(canal_escolhido, i + 1);
//             sleep(1);
//         }
//     }

//     return 0;
// 

#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_CANAIS 100
#define BUFFER 4096

void *contexto;
void *req_socket;
void *sub_socket;

char usuario[64];
char canais_inscritos[MAX_CANAIS][64];
int qtd_inscritos = 0;

int contador_local = 0;

double agora() {
    return (double)time(NULL);
}

void atualizar_contador_recebido(int contador_recebido) {
    if (contador_recebido > contador_local) {
        contador_local = contador_recebido;
    }
}

int proximo_contador() {
    contador_local++;
    return contador_local;
}

int ja_inscrito(char *canal) {
    for (int i = 0; i < qtd_inscritos; i++) {
        if (strcmp(canais_inscritos[i], canal) == 0) {
            return 1;
        }
    }
    return 0;
}

void adicionar_inscricao(char *canal) {
    if (qtd_inscritos < MAX_CANAIS) {
        strcpy(canais_inscritos[qtd_inscritos], canal);
        qtd_inscritos++;
    }
}

void enviar_mensagem(void *socket, msgpack_sbuffer *sbuf) {
    zmq_send(socket, sbuf->data, sbuf->size, 0);
}

int extrair_contador_resposta(const char *dados, int tamanho) {
    int contador = 0;

    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);

    if (msgpack_unpack_next(&msg, dados, tamanho, NULL)) {
        msgpack_object obj = msg.data;

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];

                if (kv->key.type != MSGPACK_OBJECT_STR) continue;

                char key[64] = {0};
                snprintf(key, sizeof(key), "%.*s",
                         (int)kv->key.via.str.size,
                         kv->key.via.str.ptr);

                if (strcmp(key, "contador") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                        contador = (int)kv->val.via.u64;
                    } else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                        contador = (int)kv->val.via.i64;
                    }
                }
            }
        }
    }

    msgpack_unpacked_destroy(&msg);
    return contador;
}

void *receber_publicacoes(void *arg) {
    while (1) {
        zmq_msg_t topico_msg;
        zmq_msg_t conteudo_msg;

        zmq_msg_init(&topico_msg);
        zmq_msg_init(&conteudo_msg);

        zmq_msg_recv(&topico_msg, sub_socket, 0);
        zmq_msg_recv(&conteudo_msg, sub_socket, 0);

        char canal[64];
        memset(canal, 0, sizeof(canal));
        memcpy(canal, zmq_msg_data(&topico_msg), zmq_msg_size(&topico_msg));

        char *dados = (char *)zmq_msg_data(&conteudo_msg);
        int tamanho = zmq_msg_size(&conteudo_msg);

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        if (msgpack_unpack_next(&msg, dados, tamanho, NULL)) {
            msgpack_object obj = msg.data;

            char texto[256] = "";
            double envio = 0;
            int contador_recebido = 0;

            if (obj.type == MSGPACK_OBJECT_MAP) {
                for (int i = 0; i < obj.via.map.size; i++) {
                    msgpack_object_kv *kv = &obj.via.map.ptr[i];

                    if (kv->key.type != MSGPACK_OBJECT_STR) continue;

                    char key[64] = {0};
                    snprintf(key, sizeof(key), "%.*s",
                             (int)kv->key.via.str.size,
                             kv->key.via.str.ptr);

                    if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
                        snprintf(texto, sizeof(texto), "%.*s",
                                 (int)kv->val.via.str.size,
                                 kv->val.via.str.ptr);
                    }

                    if (strcmp(key, "published_timestamp") == 0) {
                        if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64) {
                            envio = kv->val.via.f64;
                        } else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            envio = (double)kv->val.via.u64;
                        }
                    }

                    if (strcmp(key, "contador") == 0) {
                        if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            contador_recebido = (int)kv->val.via.u64;
                        } else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                            contador_recebido = (int)kv->val.via.i64;
                        }
                    }
                }
            }

            atualizar_contador_recebido(contador_recebido);

            printf("[MENSAGEM RECEBIDA] canal=%s | mensagem=%s | envio=%.0f | recebimento=%.0f | contador_local=%d\n",
                   canal, texto, envio, agora(), contador_local);
        }

        msgpack_unpacked_destroy(&msg);
        zmq_msg_close(&topico_msg);
        zmq_msg_close(&conteudo_msg);
    }

    return NULL;
}

void fazer_login() {
    int contador_envio = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 4);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "type", 4);
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "login", 5);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(usuario));
    msgpack_pack_str_body(&pk, usuario, strlen(usuario));

    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    msgpack_pack_double(&pk, agora());

    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "contador", 8);
    msgpack_pack_int(&pk, contador_envio);

    enviar_mensagem(req_socket, &sbuf);

    char resposta[BUFFER];
    int tamanho = zmq_recv(req_socket, resposta, sizeof(resposta), 0);

    int contador_resp = extrair_contador_resposta(resposta, tamanho);
    atualizar_contador_recebido(contador_resp);

    printf("[LOGIN] enviado | contador_local=%d\n", contador_local);

    msgpack_sbuffer_destroy(&sbuf);
}

int listar_canais(char canais[][64]) {
    int contador_envio = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 4);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "type", 4);
    msgpack_pack_str(&pk, 13);
    msgpack_pack_str_body(&pk, "list_channels", 13);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(usuario));
    msgpack_pack_str_body(&pk, usuario, strlen(usuario));

    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    msgpack_pack_double(&pk, agora());

    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "contador", 8);
    msgpack_pack_int(&pk, contador_envio);

    enviar_mensagem(req_socket, &sbuf);

    char resposta[BUFFER];
    int tamanho = zmq_recv(req_socket, resposta, sizeof(resposta), 0);

    int contador_resp = extrair_contador_resposta(resposta, tamanho);
    atualizar_contador_recebido(contador_resp);

    int qtd = 0;

    msgpack_unpacked msg;
    msgpack_unpacked_init(&msg);

    if (msgpack_unpack_next(&msg, resposta, tamanho, NULL)) {
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
                    for (int j = 0; j < kv->val.via.array.size; j++) {
                        msgpack_object item = kv->val.via.array.ptr[j];
                        if (item.type == MSGPACK_OBJECT_STR) {
                            snprintf(canais[qtd], 64, "%.*s",
                                     (int)item.via.str.size,
                                     item.via.str.ptr);
                            qtd++;
                        }
                    }
                }
            }
        }
    }

    msgpack_unpacked_destroy(&msg);
    msgpack_sbuffer_destroy(&sbuf);

    return qtd;
}

void criar_canal() {
    int contador_envio = proximo_contador();

    char canal[64];
    sprintf(canal, "canal_%d", 1 + rand() % 999);

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "type", 4);
    msgpack_pack_str(&pk, 14);
    msgpack_pack_str_body(&pk, "create_channel", 14);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(usuario));
    msgpack_pack_str_body(&pk, usuario, strlen(usuario));

    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    msgpack_pack_str(&pk, strlen(canal));
    msgpack_pack_str_body(&pk, canal, strlen(canal));

    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    msgpack_pack_double(&pk, agora());

    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "contador", 8);
    msgpack_pack_int(&pk, contador_envio);

    enviar_mensagem(req_socket, &sbuf);

    char resposta[BUFFER];
    int tamanho = zmq_recv(req_socket, resposta, sizeof(resposta), 0);

    int contador_resp = extrair_contador_resposta(resposta, tamanho);
    atualizar_contador_recebido(contador_resp);

    printf("[CREATE CHANNEL] %s | contador_local=%d\n", canal, contador_local);

    msgpack_sbuffer_destroy(&sbuf);
}

void se_inscrever(char canais[][64], int qtd) {
    char disponiveis[MAX_CANAIS][64];
    int qtd_disponiveis = 0;

    for (int i = 0; i < qtd; i++) {
        if (!ja_inscrito(canais[i])) {
            strcpy(disponiveis[qtd_disponiveis], canais[i]);
            qtd_disponiveis++;
        }
    }

    if (qtd_disponiveis == 0) return;

    int pos = rand() % qtd_disponiveis;
    char *canal = disponiveis[pos];

    zmq_setsockopt(sub_socket, ZMQ_SUBSCRIBE, canal, strlen(canal));
    adicionar_inscricao(canal);

    printf("[SUBSCRIBE] %s inscrito em %s\n", usuario, canal);
}

void publicar_mensagem(char *canal, int numero) {
    int contador_envio = proximo_contador();

    char texto[256];
    sprintf(texto, "mensagem %d do %s", numero, usuario);

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 6);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "type", 4);
    msgpack_pack_str(&pk, 15);
    msgpack_pack_str_body(&pk, "publish_message", 15);

    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(usuario));
    msgpack_pack_str_body(&pk, usuario, strlen(usuario));

    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    msgpack_pack_str(&pk, strlen(canal));
    msgpack_pack_str_body(&pk, canal, strlen(canal));

    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    msgpack_pack_str(&pk, strlen(texto));
    msgpack_pack_str_body(&pk, texto, strlen(texto));

    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    msgpack_pack_double(&pk, agora());

    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "contador", 8);
    msgpack_pack_int(&pk, contador_envio);

    enviar_mensagem(req_socket, &sbuf);

    char resposta[BUFFER];
    int tamanho = zmq_recv(req_socket, resposta, sizeof(resposta), 0);

    int contador_resp = extrair_contador_resposta(resposta, tamanho);
    atualizar_contador_recebido(contador_resp);

    printf("[PUBLISH] %s -> %s | contador_local=%d\n", texto, canal, contador_local);

    msgpack_sbuffer_destroy(&sbuf);
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0);
    srand(time(NULL));

    sprintf(usuario, "bot_c_%d", 1000 + rand() % 9000);

    contexto = zmq_ctx_new();
    req_socket = zmq_socket(contexto, ZMQ_REQ);
    sub_socket = zmq_socket(contexto, ZMQ_SUB);

    zmq_connect(req_socket, "tcp://broker:5555");
    zmq_connect(sub_socket, "tcp://proxy:5558");

    pthread_t thread_sub;
    pthread_create(&thread_sub, NULL, receber_publicacoes, NULL);

    printf("[CLIENT C] Bot iniciado: %s\n", usuario);

    fazer_login();

    while (1) {
        char canais[MAX_CANAIS][64];
        int qtd_canais = listar_canais(canais);

        if (qtd_canais < 5) {
            criar_canal();
            qtd_canais = listar_canais(canais);
        }

        if (qtd_inscritos < 3) {
            se_inscrever(canais, qtd_canais);
        }

        if (qtd_inscritos == 0) {
            sleep(1);
            continue;
        }

        char *canal_escolhido = canais_inscritos[rand() % qtd_inscritos];

        for (int i = 0; i < 10; i++) {
            publicar_mensagem(canal_escolhido, i + 1);
            sleep(1);
        }
    }

    return 0;
}