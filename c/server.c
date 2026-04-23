#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_CANAIS 1000
#define MAX_LOGINS 1000
#define BUFFER 4096

char *canais[MAX_CANAIS];
int qtd_canais = 0;

char *logins[MAX_LOGINS];
int qtd_logins = 0;

const char *PASTA_DADOS = "data";
// const char *ARQUIVO_CANAIS = "data/channels.json";
// const char *ARQUIVO_LOGINS = "data/logins.json";
const char *ARQUIVO_CANAIS = "/app/shared/channels.json";
const char *ARQUIVO_LOGINS = "data/logins.json";
const char *ARQUIVO_REQUISICOES = "data/requests.jsonl";
const char *ARQUIVO_PUBLICACOES = "data/publications.jsonl";

const char *NOME_SERVIDOR = "server_c";

int contador_servidor = 0;
int contador_requisicoes = 0;
double offset_relogio = 0.0;
int rank_servidor = 0;

double agora() {
    return (double)time(NULL);
}

double agora_corrigido() {
    return agora() + offset_relogio;
}

void atualizar_contador_recebido(int contador_recebido) {
    if (contador_recebido > contador_servidor) {
        contador_servidor = contador_recebido;
    }
}

int proximo_contador() {
    contador_servidor++;
    return contador_servidor;
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

void limpar_canais_memoria() {
    for (int i = 0; i < qtd_canais; i++) {
        free(canais[i]);
    }
    qtd_canais = 0;
}

void limpar_logins_memoria() {
    for (int i = 0; i < qtd_logins; i++) {
        free(logins[i]);
    }
    qtd_logins = 0;
}

void carregar_canais() {
    limpar_canais_memoria();

    FILE *f = fopen(ARQUIVO_CANAIS, "r");
    if (!f) return;

    char linha[256];
    while (fgets(linha, sizeof(linha), f)) {
        char nome[128];
        if (sscanf(linha, " \"%127[^\"]\"", nome) == 1) {
            canais[qtd_canais] = strdup(nome);
            qtd_canais++;
        }
    }

    fclose(f);
}

void carregar_logins() {
    limpar_logins_memoria();

    FILE *f = fopen(ARQUIVO_LOGINS, "r");
    if (!f) return;

    char linha[512];
    while (fgets(linha, sizeof(linha), f)) {
        if (strstr(linha, "\"user\"") != NULL) {
            logins[qtd_logins] = strdup(linha);
            qtd_logins++;
        }
    }

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

    msgpack_pack_map(&pk, 4);

    pack_string(&pk, "status");
    pack_string(&pk, status);

    pack_string(&pk, "message");
    pack_string(&pk, message);

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, proximo_contador());

    zmq_send(socket, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
}

void resposta_lista_canais(void *socket) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 4);

    pack_string(&pk, "status");
    pack_string(&pk, "ok");

    pack_string(&pk, "channels");
    msgpack_pack_array(&pk, qtd_canais);

    for (int i = 0; i < qtd_canais; i++) {
        pack_string(&pk, canais[i]);
    }

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, proximo_contador());

    zmq_send(socket, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
}

void publicar_no_canal(void *pub_socket, char *usuario, char *canal, char *texto, double request_timestamp) {
    double published_timestamp = agora_corrigido();
    int contador_pub = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 6);

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

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, contador_pub);

    zmq_send(pub_socket, canal, strlen(canal), ZMQ_SNDMORE);
    zmq_send(pub_socket, sbuf.data, sbuf.size, 0);

    char linha[1024];
    snprintf(
        linha,
        sizeof(linha),
        "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\",\"request_timestamp\":%.0f,\"published_timestamp\":%.0f,\"contador\":%d}",
        canal, usuario, texto, request_timestamp, published_timestamp, contador_pub
    );
    salvar_linha_jsonl(ARQUIVO_PUBLICACOES, linha);

    msgpack_sbuffer_destroy(&sbuf);
}

void registrar_na_referencia(void *ref_socket) {
    char json[256];
    snprintf(json, sizeof(json),
             "{\"type\":\"register\",\"name\":\"%s\"}",
             NOME_SERVIDOR);

    zmq_send(ref_socket, json, strlen(json), 0);

    char buffer[BUFFER];
    int tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer) - 1, 0);
    if (tamanho <= 0) return;
    buffer[tamanho] = '\0';

    char *rank_ptr = strstr(buffer, "\"rank\":");
    if (rank_ptr != NULL) {
        sscanf(rank_ptr, "\"rank\":%d", &rank_servidor);
    }

    printf("[SERVER C] Meu rank: %d\n", rank_servidor);
}

void enviar_heartbeat(void *ref_socket) {
    char json[256];
    char buffer[BUFFER];

    snprintf(json, sizeof(json),
             "{\"type\":\"heartbeat\",\"name\":\"%s\"}",
             NOME_SERVIDOR);
    zmq_send(ref_socket, json, strlen(json), 0);

    int tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer) - 1, 0);
    if (tamanho > 0) {
        buffer[tamanho] = '\0';

        double tempo_ref = 0;
        char *ptr = strstr(buffer, "\"timestamp\":");
        if (ptr != NULL) {
            sscanf(ptr, "\"timestamp\":%lf", &tempo_ref);
            offset_relogio = tempo_ref - agora();
            printf("[HEARTBEAT] tempo sincronizado: %.0f\n", tempo_ref);
        }
    }

    snprintf(json, sizeof(json), "{\"type\":\"list\"}");
    zmq_send(ref_socket, json, strlen(json), 0);

    tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer) - 1, 0);
    if (tamanho > 0) {
        buffer[tamanho] = '\0';
        printf("[SERVIDORES ATIVOS] %s\n", buffer);
    }
}

int main() {
    criar_pasta_dados();
    setvbuf(stdout, NULL, _IONBF, 0);

    void *contexto = zmq_ctx_new();

    void *rep_socket = zmq_socket(contexto, ZMQ_REP);
    zmq_connect(rep_socket, "tcp://broker:5556");

    void *pub_socket = zmq_socket(contexto, ZMQ_PUB);
    zmq_connect(pub_socket, "tcp://proxy:5557");

    void *ref_socket = zmq_socket(contexto, ZMQ_REQ);
    zmq_connect(ref_socket, "tcp://referencia:5560");

    printf("[SERVER C] Iniciado...\n");

    registrar_na_referencia(ref_socket);

    while (1) {
        char buffer[BUFFER];
        int tamanho = zmq_recv(rep_socket, buffer, sizeof(buffer), 0);
        if (tamanho <= 0) continue;

        carregar_canais();
        carregar_logins();
        contador_requisicoes++;

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
        double timestamp = agora_corrigido();
        int contador_recebido = 0;

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
                } else if (strcmp(key, "contador") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                        contador_recebido = (int)kv->val.via.u64;
                    } else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
                        contador_recebido = (int)kv->val.via.i64;
                    }
                }
            }
        }

        atualizar_contador_recebido(contador_recebido);

        char linha_req[512];
        snprintf(linha_req, sizeof(linha_req),
                 "{\"type\":\"%s\",\"user\":\"%s\",\"received_timestamp\":%.0f,\"contador\":%d}",
                 tipo, usuario, agora_corrigido(), contador_servidor);

        printf("[SERVER C] tipo=%s | user=%s | canal=%s | contador=%d\n",
               tipo, usuario, canal, contador_servidor);

        salvar_linha_jsonl(ARQUIVO_REQUISICOES, linha_req);

        if (strcmp(tipo, "login") == 0) {
            adicionar_login(usuario, timestamp);
            char msg_resposta[128];
            snprintf(msg_resposta, sizeof(msg_resposta), "login realizado (%s)", usuario);
            resposta_simples(rep_socket, "ok", msg_resposta);

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

        if (contador_requisicoes % 10 == 0) {
            enviar_heartbeat(ref_socket);
        }

        msgpack_unpacked_destroy(&msg);
    }

    return 0;
}