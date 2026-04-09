#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#define MAX_CANAIS 1000
#define MAX_LOGINS 1000
#define BUFFER 4096

char *canais[MAX_CANAIS];
int qtd_canais = 0;

char *logins[MAX_LOGINS];
int qtd_logins = 0;

const char *PASTA_DADOS = "data";
const char *ARQUIVO_CANAIS = "data/channels.json";
const char *ARQUIVO_LOGINS = "data/logins.json";
const char *ARQUIVO_REQUISICOES = "data/requests.jsonl";
const char *ARQUIVO_PUBLICACOES = "data/publications.jsonl";

double agora() {
    return (double)time(NULL);
}

void criar_pasta_dados() {
    mkdir(PASTA_DADOS, 0777);
}

int canal_existe(char *nome) {
    for (int i = 0; i < qtd_canais; i++) {
        if (strcmp(canais[i], nome) == 0) {
            return 1;
        }
    }
    return 0;
}

void salvar_canais() {
    FILE *f = fopen(ARQUIVO_CANAIS, "w");
    if (!f) return;

    fprintf(f, "[\n");
    for (int i = 0; i < qtd_canais; i++) {
        fprintf(f, "  \"%s\"", canais[i]);
        if (i < qtd_canais - 1) fprintf(f, ",");
        fprintf(f, "\n");
    }
    fprintf(f, "]\n");

    fclose(f);
}

void salvar_logins() {
    FILE *f = fopen(ARQUIVO_LOGINS, "w");
    if (!f) return;

    fprintf(f, "[\n");
    for (int i = 0; i < qtd_logins; i++) {
        fprintf(f, "%s", logins[i]);
        if (i < qtd_logins - 1) fprintf(f, ",");
        fprintf(f, "\n");
    }
    fprintf(f, "]\n");

    fclose(f);
}

void salvar_linha_jsonl(const char *arquivo, const char *linha) {
    FILE *f = fopen(arquivo, "a");
    if (!f) return;

    fprintf(f, "%s\n", linha);
    fclose(f);
}

void adicionar_canal(char *nome) {
    if (qtd_canais < MAX_CANAIS) {
        canais[qtd_canais] = strdup(nome);
        qtd_canais++;
        salvar_canais();
    }
}

void adicionar_login(char *usuario, double timestamp) {
    if (qtd_logins < MAX_LOGINS) {
        char linha[256];
        snprintf(linha, sizeof(linha), "{\"user\":\"%s\",\"timestamp\":%.0f}", usuario, timestamp);
        logins[qtd_logins] = strdup(linha);
        qtd_logins++;
        salvar_logins();
    }
}

void pack_string(msgpack_packer *pk, const char *texto) {
    msgpack_pack_str(pk, strlen(texto));
    msgpack_pack_str_body(pk, texto, strlen(texto));
}

void resposta_simples(void *socket, char *status, char *message) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 3);

    pack_string(&pk, "status");
    pack_string(&pk, status);

    pack_string(&pk, "message");
    pack_string(&pk, message);

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora());

    zmq_send(socket, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
}

void resposta_lista_canais(void *socket) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 3);

    pack_string(&pk, "status");
    pack_string(&pk, "ok");

    pack_string(&pk, "channels");
    msgpack_pack_array(&pk, qtd_canais);

    for (int i = 0; i < qtd_canais; i++) {
        pack_string(&pk, canais[i]);
    }

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora());

    zmq_send(socket, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
}

void publicar_no_canal(void *pub_socket, char *usuario, char *canal, char *texto, double request_timestamp) {
    double published_timestamp = agora();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);

    pack_string(&pk, "user");
    pack_string(&pk, usuario);

    pack_string(&pk, "channel");
    pack_string(&pk, canal);

    pack_string(&pk, "message");
    pack_string(&pk, texto);

    pack_string(&pk, "request_timestamp");
    msgpack_pack_double(&pk, request_timestamp);

    pack_string(&pk, "published_timestamp");
    msgpack_pack_double(&pk, published_timestamp);

    zmq_send(pub_socket, canal, strlen(canal), ZMQ_SNDMORE);
    zmq_send(pub_socket, sbuf.data, sbuf.size, 0);

    char linha[1024];
    snprintf(
        linha,
        sizeof(linha),
        "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\",\"request_timestamp\":%.0f,\"published_timestamp\":%.0f}",
        canal, usuario, texto, request_timestamp, published_timestamp
    );
    salvar_linha_jsonl(ARQUIVO_PUBLICACOES, linha);

    msgpack_sbuffer_destroy(&sbuf);
}

int main() {
    criar_pasta_dados();

    void *contexto = zmq_ctx_new();

    void *rep_socket = zmq_socket(contexto, ZMQ_REP);
    zmq_connect(rep_socket, "tcp://broker:5556");

    void *pub_socket = zmq_socket(contexto, ZMQ_PUB);
    zmq_connect(pub_socket, "tcp://proxy:5557");

    printf("[SERVER C] Iniciado...\n");

    while (1) {
        char buffer[BUFFER];
        int tamanho = zmq_recv(rep_socket, buffer, sizeof(buffer), 0);
        if (tamanho <= 0) continue;

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        if (!msgpack_unpack_next(&msg, buffer, tamanho, NULL)) {
            msgpack_unpacked_destroy(&msg);
            continue;
        }

        msgpack_object obj = msg.data;

        char tipo[64] = "";
        char usuario[64] = "";
        char canal[64] = "";
        char texto[256] = "";
        double timestamp = agora();

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];

                if (kv->key.type != MSGPACK_OBJECT_STR) continue;

                char key[64] = {0};
                snprintf(key, sizeof(key), "%.*s",
                         (int)kv->key.via.str.size,
                         kv->key.via.str.ptr);

                if (strcmp(key, "type") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
                    snprintf(tipo, sizeof(tipo), "%.*s",
                             (int)kv->val.via.str.size,
                             kv->val.via.str.ptr);
                } else if (strcmp(key, "user") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
                    snprintf(usuario, sizeof(usuario), "%.*s",
                             (int)kv->val.via.str.size,
                             kv->val.via.str.ptr);
                } else if (strcmp(key, "channel") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
                    snprintf(canal, sizeof(canal), "%.*s",
                             (int)kv->val.via.str.size,
                             kv->val.via.str.ptr);
                } else if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR) {
                    snprintf(texto, sizeof(texto), "%.*s",
                             (int)kv->val.via.str.size,
                             kv->val.via.str.ptr);
                } else if (strcmp(key, "timestamp") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64) {
                        timestamp = kv->val.via.f64;
                    } else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                        timestamp = (double)kv->val.via.u64;
                    }
                }
            }
        }

        char linha_req[512];
        snprintf(linha_req, sizeof(linha_req),
                 "{\"type\":\"%s\",\"user\":\"%s\",\"received_timestamp\":%.0f}",
                 tipo, usuario, agora());
        salvar_linha_jsonl(ARQUIVO_REQUISICOES, linha_req);

        if (strcmp(tipo, "login") == 0) {
            adicionar_login(usuario, timestamp);
            resposta_simples(rep_socket, "ok", "login realizado");
        } else if (strcmp(tipo, "create_channel") == 0) {
            if (strlen(canal) == 0) {
                resposta_simples(rep_socket, "error", "nome de canal inválido");
            } else if (canal_existe(canal)) {
                resposta_simples(rep_socket, "error", "canal já existe");
            } else {
                adicionar_canal(canal);

                char msg_resposta[128];
                snprintf(msg_resposta, sizeof(msg_resposta), "canal '%s' criado", canal);
                resposta_simples(rep_socket, "ok", msg_resposta);
            }
        } else if (strcmp(tipo, "list_channels") == 0) {
            resposta_lista_canais(rep_socket);
        } else if (strcmp(tipo, "publish_message") == 0) {
            if (!canal_existe(canal)) {
                resposta_simples(rep_socket, "error", "canal inexistente");
            } else {
                publicar_no_canal(pub_socket, usuario, canal, texto, timestamp);

                char msg_resposta[128];
                snprintf(msg_resposta, sizeof(msg_resposta), "mensagem publicada em '%s'", canal);
                resposta_simples(rep_socket, "ok", msg_resposta);
            }
        } else {
            resposta_simples(rep_socket, "error", "tipo inválido");
        }

        msgpack_unpacked_destroy(&msg);
        setvbuf(stdout, NULL, _IONBF, 0);
    }

    return 0;
}