#include <zmq.h>
#include <msgpack.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>


#define MAX_CANAIS 1000
#define MAX_LOGINS 1000
#define MAX_EVENTOS 5000
#define BUFFER 8192

#define INTERVALO_HEARTBEAT 10
#define INTERVALO_BERKELEY 15


const char *NOME_SERVIDOR = "server_c";

const char *PASTA_DADOS = "data";

const char *ARQUIVO_CANAIS = "data/channels.json";
const char *ARQUIVO_LOGINS = "data/logins.json";
const char *ARQUIVO_REQUISICOES = "data/requests.jsonl";
const char *ARQUIVO_PUBLICACOES = "data/publications.jsonl";
const char *ARQUIVO_REPLICADOS = "data/replicated_events.jsonl";
const char *ARQUIVO_EVENTOS_APLICADOS = "data/applied_events.json";

#define PORTA_DIRETA_C      "tcp://*:5570"
#define ADDR_SERVER_C       "tcp://server_c:5570"
#define ADDR_SERVER_PYTHON  "tcp://server_python:5571"
#define ADDR_SERVER_JAVA    "tcp://server_java:5572"


char *canais[MAX_CANAIS];
int qtd_canais = 0;

char *logins[MAX_LOGINS];
int qtd_logins = 0;

char *eventos_aplicados[MAX_EVENTOS];
int qtd_eventos_aplicados = 0;

int contador_servidor = 0;
int contador_requisicoes = 0;
double offset_relogio = 0.0;
int rank_servidor = 0;

char coordenador[64] = "";

void *contexto_global = NULL;
void *pub_socket_global = NULL;

pthread_mutex_t lock_estado = PTHREAD_MUTEX_INITIALIZER;


// ============================================================
// RELÓGIOS
// ============================================================

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


// ============================================================
// HELPERS
// ============================================================

void criar_pasta_dados() {
    mkdir(PASTA_DADOS, 0777);
}

char *strdup_seguro(const char *s) {
    if (!s) return strdup("");
    return strdup(s);
}

void limpar_string(char *s) {
    if (!s) return;

    for (int i = 0; s[i]; i++) {
        if (s[i] == '\n' || s[i] == '\r') {
            s[i] = '\0';
        }
    }
}

int canal_existe(const char *nome) {
    for (int i = 0; i < qtd_canais; i++) {
        if (strcmp(canais[i], nome) == 0) {
            return 1;
        }
    }

    return 0;
}

int evento_ja_aplicado(const char *event_id) {
    for (int i = 0; i < qtd_eventos_aplicados; i++) {
        if (strcmp(eventos_aplicados[i], event_id) == 0) {
            return 1;
        }
    }

    return 0;
}

void adicionar_evento_aplicado(const char *event_id);

void salvar_linha_jsonl(const char *arquivo, const char *linha) {
    FILE *f = fopen(arquivo, "a");

    if (!f) {
        return;
    }

    fprintf(f, "%s\n", linha);
    fclose(f);
}


// ============================================================
// PERSISTÊNCIA
// ============================================================

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

void limpar_eventos_memoria() {
    for (int i = 0; i < qtd_eventos_aplicados; i++) {
        free(eventos_aplicados[i]);
    }

    qtd_eventos_aplicados = 0;
}

void salvar_canais() {
    FILE *f = fopen(ARQUIVO_CANAIS, "w");

    if (!f) {
        return;
    }

    fprintf(f, "[\n");

    for (int i = 0; i < qtd_canais; i++) {
        fprintf(f, "  \"%s\"", canais[i]);

        if (i < qtd_canais - 1) {
            fprintf(f, ",");
        }

        fprintf(f, "\n");
    }

    fprintf(f, "]\n");

    fclose(f);
}

void carregar_canais() {
    limpar_canais_memoria();

    FILE *f = fopen(ARQUIVO_CANAIS, "r");

    if (!f) {
        return;
    }

    char linha[256];

    while (fgets(linha, sizeof(linha), f)) {
        char nome[128];

        if (sscanf(linha, " \"%127[^\"]\"", nome) == 1) {
            if (qtd_canais < MAX_CANAIS) {
                canais[qtd_canais++] = strdup(nome);
            }
        }
    }

    fclose(f);
}

void salvar_logins() {
    FILE *f = fopen(ARQUIVO_LOGINS, "w");

    if (!f) {
        return;
    }

    fprintf(f, "[\n");

    for (int i = 0; i < qtd_logins; i++) {
        fprintf(f, "  %s", logins[i]);

        if (i < qtd_logins - 1) {
            fprintf(f, ",");
        }

        fprintf(f, "\n");
    }

    fprintf(f, "]\n");

    fclose(f);
}

void carregar_logins() {
    limpar_logins_memoria();

    FILE *f = fopen(ARQUIVO_LOGINS, "r");

    if (!f) {
        return;
    }

    char linha[512];

    while (fgets(linha, sizeof(linha), f)) {
        limpar_string(linha);

        if (strstr(linha, "\"user\"") != NULL) {
            char *p = linha;

            while (*p == ' ' || *p == '\t') {
                p++;
            }

            int len = strlen(p);

            if (len > 0 && p[len - 1] == ',') {
                p[len - 1] = '\0';
            }

            if (qtd_logins < MAX_LOGINS) {
                logins[qtd_logins++] = strdup(p);
            }
        }
    }

    fclose(f);
}

void salvar_eventos_aplicados() {
    FILE *f = fopen(ARQUIVO_EVENTOS_APLICADOS, "w");

    if (!f) {
        return;
    }

    fprintf(f, "[\n");

    for (int i = 0; i < qtd_eventos_aplicados; i++) {
        fprintf(f, "  \"%s\"", eventos_aplicados[i]);

        if (i < qtd_eventos_aplicados - 1) {
            fprintf(f, ",");
        }

        fprintf(f, "\n");
    }

    fprintf(f, "]\n");

    fclose(f);
}

void carregar_eventos_aplicados() {
    limpar_eventos_memoria();

    FILE *f = fopen(ARQUIVO_EVENTOS_APLICADOS, "r");

    if (!f) {
        return;
    }

    char linha[512];

    while (fgets(linha, sizeof(linha), f)) {
        char id[256];

        if (sscanf(linha, " \"%255[^\"]\"", id) == 1) {
            if (qtd_eventos_aplicados < MAX_EVENTOS) {
                eventos_aplicados[qtd_eventos_aplicados++] = strdup(id);
            }
        }
    }

    fclose(f);
}

void carregar_estado() {
    carregar_canais();
    carregar_logins();
    carregar_eventos_aplicados();
}

void adicionar_canal_local(const char *nome) {
    if (qtd_canais < MAX_CANAIS && !canal_existe(nome)) {
        canais[qtd_canais++] = strdup(nome);
        salvar_canais();
    }
}

void adicionar_login_local(const char *usuario, double timestamp) {
    if (qtd_logins < MAX_LOGINS) {
        char linha[512];

        snprintf(
            linha,
            sizeof(linha),
            "{\"user\":\"%s\",\"timestamp\":%.0f}",
            usuario,
            timestamp
        );

        logins[qtd_logins++] = strdup(linha);

        salvar_logins();
    }
}

void adicionar_evento_aplicado(const char *event_id) {
    if (qtd_eventos_aplicados < MAX_EVENTOS && !evento_ja_aplicado(event_id)) {
        eventos_aplicados[qtd_eventos_aplicados++] = strdup(event_id);
        salvar_eventos_aplicados();
    }
}


// ============================================================
// MSGPACK HELPERS
// ============================================================

void pack_string(msgpack_packer *pk, const char *texto) {
    msgpack_pack_str(pk, strlen(texto));
    msgpack_pack_str_body(pk, texto, strlen(texto));
}

void extrair_string_msgpack(msgpack_object obj, char *dest, int max_len) {
    if (obj.type == MSGPACK_OBJECT_STR) {
        snprintf(dest, max_len, "%.*s", (int)obj.via.str.size, obj.via.str.ptr);
    }
}

int extrair_int_msgpack(msgpack_object obj) {
    if (obj.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        return (int)obj.via.u64;
    }

    if (obj.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        return (int)obj.via.i64;
    }

    return 0;
}

double extrair_double_msgpack(msgpack_object obj) {
    if (obj.type == MSGPACK_OBJECT_FLOAT32 || obj.type == MSGPACK_OBJECT_FLOAT64) {
        return obj.via.f64;
    }

    if (obj.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        return (double)obj.via.u64;
    }

    if (obj.type == MSGPACK_OBJECT_NEGATIVE_INTEGER) {
        return (double)obj.via.i64;
    }

    return 0.0;
}


// ============================================================
// RESPOSTAS
// ============================================================

void resposta_simples(void *socket, const char *status, const char *message) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);

    pack_string(&pk, "status");
    pack_string(&pk, status);

    pack_string(&pk, "message");
    pack_string(&pk, message);

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, proximo_contador());

    pack_string(&pk, "coordenador");
    pack_string(&pk, coordenador);

    zmq_send(socket, sbuf.data, sbuf.size, 0);

    msgpack_sbuffer_destroy(&sbuf);
}

void resposta_lista_canais(void *socket) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);

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

    pack_string(&pk, "coordenador");
    pack_string(&pk, coordenador);

    zmq_send(socket, sbuf.data, sbuf.size, 0);

    msgpack_sbuffer_destroy(&sbuf);
}


// ============================================================
// PUBLICAÇÃO NORMAL
// ============================================================

void publicar_no_canal(
    void *pub_socket,
    const char *usuario,
    const char *canal,
    const char *texto,
    double request_timestamp
) {
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

    char linha[2048];

    snprintf(
        linha,
        sizeof(linha),
        "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\",\"request_timestamp\":%.0f,\"published_timestamp\":%.0f,\"contador\":%d}",
        canal,
        usuario,
        texto,
        request_timestamp,
        published_timestamp,
        contador_pub
    );

    salvar_linha_jsonl(ARQUIVO_PUBLICACOES, linha);

    msgpack_sbuffer_destroy(&sbuf);
}


// ============================================================
// REPLICAÇÃO
// ============================================================

void gerar_event_id(char *dest, int max_len) {
    snprintf(
        dest,
        max_len,
        "%s-%ld-%d",
        NOME_SERVIDOR,
        time(NULL),
        rand()
    );
}

void publicar_replicacao_login(const char *usuario, double timestamp) {
    char event_id[256];
    gerar_event_id(event_id, sizeof(event_id));

    pthread_mutex_lock(&lock_estado);
    adicionar_evento_aplicado(event_id);
    pthread_mutex_unlock(&lock_estado);

    int contador_rep = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 7);

    pack_string(&pk, "type");
    pack_string(&pk, "replication");

    pack_string(&pk, "event_id");
    pack_string(&pk, event_id);

    pack_string(&pk, "origin");
    pack_string(&pk, NOME_SERVIDOR);

    pack_string(&pk, "operation");
    pack_string(&pk, "login");

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, contador_rep);

    pack_string(&pk, "data");
    msgpack_pack_map(&pk, 2);

    pack_string(&pk, "user");
    pack_string(&pk, usuario);

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, timestamp);

    zmq_send(pub_socket_global, "replication", strlen("replication"), ZMQ_SNDMORE);
    zmq_send(pub_socket_global, sbuf.data, sbuf.size, 0);

    char linha[512];
    snprintf(
        linha,
        sizeof(linha),
        "{\"event_id\":\"%s\",\"origin\":\"%s\",\"operation\":\"login\"}",
        event_id,
        NOME_SERVIDOR
    );

    salvar_linha_jsonl(ARQUIVO_REPLICADOS, linha);

    msgpack_sbuffer_destroy(&sbuf);
}

void publicar_replicacao_canal(const char *canal) {
    char event_id[256];
    gerar_event_id(event_id, sizeof(event_id));

    pthread_mutex_lock(&lock_estado);
    adicionar_evento_aplicado(event_id);
    pthread_mutex_unlock(&lock_estado);

    int contador_rep = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 7);

    pack_string(&pk, "type");
    pack_string(&pk, "replication");

    pack_string(&pk, "event_id");
    pack_string(&pk, event_id);

    pack_string(&pk, "origin");
    pack_string(&pk, NOME_SERVIDOR);

    pack_string(&pk, "operation");
    pack_string(&pk, "create_channel");

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, contador_rep);

    pack_string(&pk, "data");
    msgpack_pack_map(&pk, 1);

    pack_string(&pk, "channel");
    pack_string(&pk, canal);

    zmq_send(pub_socket_global, "replication", strlen("replication"), ZMQ_SNDMORE);
    zmq_send(pub_socket_global, sbuf.data, sbuf.size, 0);

    char linha[512];
    snprintf(
        linha,
        sizeof(linha),
        "{\"event_id\":\"%s\",\"origin\":\"%s\",\"operation\":\"create_channel\"}",
        event_id,
        NOME_SERVIDOR
    );

    salvar_linha_jsonl(ARQUIVO_REPLICADOS, linha);

    msgpack_sbuffer_destroy(&sbuf);
}

void publicar_replicacao_publicacao(
    const char *usuario,
    const char *canal,
    const char *texto,
    double request_timestamp
) {
    char event_id[256];
    gerar_event_id(event_id, sizeof(event_id));

    pthread_mutex_lock(&lock_estado);
    adicionar_evento_aplicado(event_id);
    pthread_mutex_unlock(&lock_estado);

    double published_timestamp = agora_corrigido();
    int contador_pub = proximo_contador();
    int contador_rep = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 7);

    pack_string(&pk, "type");
    pack_string(&pk, "replication");

    pack_string(&pk, "event_id");
    pack_string(&pk, event_id);

    pack_string(&pk, "origin");
    pack_string(&pk, NOME_SERVIDOR);

    pack_string(&pk, "operation");
    pack_string(&pk, "publish_message");

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, contador_rep);

    pack_string(&pk, "data");
    msgpack_pack_map(&pk, 6);

    pack_string(&pk, "channel");
    pack_string(&pk, canal);

    pack_string(&pk, "user");
    pack_string(&pk, usuario);

    pack_string(&pk, "message");
    pack_string(&pk, texto);

    pack_string(&pk, "request_timestamp");
    msgpack_pack_double(&pk, request_timestamp);

    pack_string(&pk, "published_timestamp");
    msgpack_pack_double(&pk, published_timestamp);

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, contador_pub);

    zmq_send(pub_socket_global, "replication", strlen("replication"), ZMQ_SNDMORE);
    zmq_send(pub_socket_global, sbuf.data, sbuf.size, 0);

    char linha[512];
    snprintf(
        linha,
        sizeof(linha),
        "{\"event_id\":\"%s\",\"origin\":\"%s\",\"operation\":\"publish_message\"}",
        event_id,
        NOME_SERVIDOR
    );

    salvar_linha_jsonl(ARQUIVO_REPLICADOS, linha);

    msgpack_sbuffer_destroy(&sbuf);
}

void aplicar_evento_replicado(msgpack_object obj) {
    char event_id[256] = "";
    char origin[128] = "";
    char operation[128] = "";

    char data_user[128] = "";
    char data_channel[128] = "";
    char data_message[512] = "";

    double data_timestamp = agora_corrigido();
    double data_request_timestamp = agora_corrigido();
    double data_published_timestamp = agora_corrigido();
    int data_contador = 0;
    int contador_recebido = 0;

    if (obj.type != MSGPACK_OBJECT_MAP) {
        return;
    }

    for (int i = 0; i < obj.via.map.size; i++) {
        msgpack_object_kv *kv = &obj.via.map.ptr[i];

        if (kv->key.type != MSGPACK_OBJECT_STR) {
            continue;
        }

        char key[64] = {0};

        snprintf(
            key,
            sizeof(key),
            "%.*s",
            (int)kv->key.via.str.size,
            kv->key.via.str.ptr
        );

        if (strcmp(key, "event_id") == 0) {
            extrair_string_msgpack(kv->val, event_id, sizeof(event_id));

        } else if (strcmp(key, "origin") == 0) {
            extrair_string_msgpack(kv->val, origin, sizeof(origin));

        } else if (strcmp(key, "operation") == 0) {
            extrair_string_msgpack(kv->val, operation, sizeof(operation));

        } else if (strcmp(key, "contador") == 0) {
            contador_recebido = extrair_int_msgpack(kv->val);

        } else if (strcmp(key, "data") == 0 && kv->val.type == MSGPACK_OBJECT_MAP) {
            msgpack_object data = kv->val;

            for (int j = 0; j < data.via.map.size; j++) {
                msgpack_object_kv *dkv = &data.via.map.ptr[j];

                if (dkv->key.type != MSGPACK_OBJECT_STR) {
                    continue;
                }

                char dkey[64] = {0};

                snprintf(
                    dkey,
                    sizeof(dkey),
                    "%.*s",
                    (int)dkv->key.via.str.size,
                    dkv->key.via.str.ptr
                );

                if (strcmp(dkey, "user") == 0) {
                    extrair_string_msgpack(dkv->val, data_user, sizeof(data_user));

                } else if (strcmp(dkey, "channel") == 0) {
                    extrair_string_msgpack(dkv->val, data_channel, sizeof(data_channel));

                } else if (strcmp(dkey, "message") == 0) {
                    extrair_string_msgpack(dkv->val, data_message, sizeof(data_message));

                } else if (strcmp(dkey, "timestamp") == 0) {
                    data_timestamp = extrair_double_msgpack(dkv->val);

                } else if (strcmp(dkey, "request_timestamp") == 0) {
                    data_request_timestamp = extrair_double_msgpack(dkv->val);

                } else if (strcmp(dkey, "published_timestamp") == 0) {
                    data_published_timestamp = extrair_double_msgpack(dkv->val);

                } else if (strcmp(dkey, "contador") == 0) {
                    data_contador = extrair_int_msgpack(dkv->val);
                }
            }
        }
    }

    atualizar_contador_recebido(contador_recebido);

    if (strlen(event_id) == 0) {
        return;
    }

    if (strcmp(origin, NOME_SERVIDOR) == 0) {
        return;
    }

    pthread_mutex_lock(&lock_estado);

    if (evento_ja_aplicado(event_id)) {
        pthread_mutex_unlock(&lock_estado);
        return;
    }

    adicionar_evento_aplicado(event_id);

    if (strcmp(operation, "login") == 0) {
        adicionar_login_local(data_user, data_timestamp);

    } else if (strcmp(operation, "create_channel") == 0) {
        adicionar_canal_local(data_channel);

    } else if (strcmp(operation, "publish_message") == 0) {
        char linha[2048];

        snprintf(
            linha,
            sizeof(linha),
            "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\",\"request_timestamp\":%.0f,\"published_timestamp\":%.0f,\"contador\":%d}",
            data_channel,
            data_user,
            data_message,
            data_request_timestamp,
            data_published_timestamp,
            data_contador
        );

        salvar_linha_jsonl(ARQUIVO_PUBLICACOES, linha);
    }

    pthread_mutex_unlock(&lock_estado);

    printf("[REPLICACAO] Evento aplicado: %s de %s\n", operation, origin);
}


// ============================================================
// THREAD REPLICAÇÃO
// ============================================================

void *thread_replicacao(void *arg) {
    void *sub = zmq_socket(contexto_global, ZMQ_SUB);

    zmq_connect(sub, "tcp://proxy:5558");
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "replication", strlen("replication"));

    while (1) {
        char topico[128];

        int n_topico = zmq_recv(sub, topico, sizeof(topico) - 1, 0);

        if (n_topico <= 0) {
            continue;
        }

        topico[n_topico] = '\0';

        char buffer[BUFFER];

        int n = zmq_recv(sub, buffer, sizeof(buffer), 0);

        if (n <= 0) {
            continue;
        }

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        if (msgpack_unpack_next(&msg, buffer, n, NULL)) {
            aplicar_evento_replicado(msg.data);
        }

        msgpack_unpacked_destroy(&msg);
    }

    return NULL;
}


// ============================================================
// REFERÊNCIA
// ============================================================

void registrar_na_referencia(void *ref_socket) {
    char json[256];

    snprintf(
        json,
        sizeof(json),
        "{\"type\":\"register\",\"name\":\"%s\"}",
        NOME_SERVIDOR
    );

    zmq_send(ref_socket, json, strlen(json), 0);

    char buffer[BUFFER];

    int tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer) - 1, 0);

    if (tamanho <= 0) {
        return;
    }

    buffer[tamanho] = '\0';

    char *rank_ptr = strstr(buffer, "\"rank\":");

    if (rank_ptr != NULL) {
        sscanf(rank_ptr, "\"rank\":%d", &rank_servidor);
    }

    printf("[SERVER C] Meu rank: %d\n", rank_servidor);
}

void enviar_heartbeat(void *ref_socket) {
    char json[256];

    snprintf(
        json,
        sizeof(json),
        "{\"type\":\"heartbeat\",\"name\":\"%s\"}",
        NOME_SERVIDOR
    );

    zmq_send(ref_socket, json, strlen(json), 0);

    char buffer[BUFFER];

    int tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer) - 1, 0);

    if (tamanho > 0) {
        buffer[tamanho] = '\0';
        printf("[HEARTBEAT] resposta=%s\n", buffer);
    }
}

const char* porta_do_servidor(const char *nome) {
    if (strcmp(nome, "server_c") == 0) {
        return ADDR_SERVER_C;
    }

    if (strcmp(nome, "server_python") == 0) {
        return ADDR_SERVER_PYTHON;
    }

    if (strcmp(nome, "server_java") == 0) {
        return ADDR_SERVER_JAVA;
    }

    return NULL;
}


// ============================================================
// THREAD DIRETA
// ============================================================

void *thread_servidor_direto(void *arg) {
    void *sock = zmq_socket(contexto_global, ZMQ_REP);

    zmq_bind(sock, PORTA_DIRETA_C);

    printf("[SERVER C] Thread direta escutando na porta 5570\n");

    char buf[256];

    while (1) {
        int n = zmq_recv(sock, buf, sizeof(buf) - 1, 0);

        if (n <= 0) {
            continue;
        }

        buf[n] = '\0';

        if (strstr(buf, "election")) {
            const char *resp = "{\"status\":\"ok\"}";
            zmq_send(sock, resp, strlen(resp), 0);

        } else if (strstr(buf, "get_time")) {
            char resp[128];

            snprintf(resp, sizeof(resp), "{\"time\":%.3f}", agora_corrigido());

            zmq_send(sock, resp, strlen(resp), 0);

        } else {
            const char *resp = "{\"status\":\"error\"}";
            zmq_send(sock, resp, strlen(resp), 0);
        }
    }

    return NULL;
}


// ============================================================
// ELEIÇÃO E BERKELEY
// ============================================================

int coordenador_esta_vivo() {
    if (strlen(coordenador) == 0) {
        return 0;
    }

    const char *porta = porta_do_servidor(coordenador);

    if (!porta) {
        return 0;
    }

    void *sock = zmq_socket(contexto_global, ZMQ_REQ);

    int timeout = 1000;

    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_connect(sock, porta);

    const char *msg = "{\"type\":\"election\"}";

    zmq_send(sock, msg, strlen(msg), 0);

    char resp[128];

    int n = zmq_recv(sock, resp, sizeof(resp) - 1, 0);

    zmq_close(sock);

    if (n > 0) {
        resp[n] = '\0';

        return strstr(resp, "ok") != NULL;
    }

    return 0;
}

void publicar_coordenador(void *pub_socket) {
    if (strlen(coordenador) == 0) {
        return;
    }

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);

    pack_string(&pk, "type");
    pack_string(&pk, "coordinator_announce");

    pack_string(&pk, "coordinator");
    pack_string(&pk, coordenador);

    pack_string(&pk, "server");
    pack_string(&pk, NOME_SERVIDOR);

    pack_string(&pk, "timestamp");
    msgpack_pack_double(&pk, agora_corrigido());

    pack_string(&pk, "contador");
    msgpack_pack_int(&pk, proximo_contador());

    zmq_send(pub_socket, "servers", strlen("servers"), ZMQ_SNDMORE);
    zmq_send(pub_socket, sbuf.data, sbuf.size, 0);

    printf("[PUB SERVERS] coordenador eleito: %s\n", coordenador);

    msgpack_sbuffer_destroy(&sbuf);
}

void eleger_coordenador_direto(char *lista_json, void *pub_socket) {
    char *p = lista_json;

    int melhor_rank = 999999;
    char melhor_nome[64] = "";

    while ((p = strstr(p, "\"name\"")) != NULL) {
        char nome[64] = "";
        int rank = 999999;

        char *colon = strchr(p, ':');

        if (!colon) {
            break;
        }

        char *q1 = strchr(colon, '"');

        if (!q1) {
            break;
        }

        char *q2 = strchr(q1 + 1, '"');

        if (!q2) {
            break;
        }

        int tamanho_nome = q2 - q1 - 1;

        if (tamanho_nome > 63) {
            tamanho_nome = 63;
        }

        strncpy(nome, q1 + 1, tamanho_nome);
        nome[tamanho_nome] = '\0';

        char *rank_ptr = strstr(q2, "\"rank\"");

        if (rank_ptr != NULL) {
            char *rank_colon = strchr(rank_ptr, ':');

            if (rank_colon != NULL) {
                rank = atoi(rank_colon + 1);
            }
        }

        const char *porta = porta_do_servidor(nome);
        int vivo = 0;

        if (porta) {
            void *sock = zmq_socket(contexto_global, ZMQ_REQ);

            int timeout = 1000;

            zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
            zmq_connect(sock, porta);

            const char *msg = "{\"type\":\"election\"}";

            zmq_send(sock, msg, strlen(msg), 0);

            char resp[128];

            int n = zmq_recv(sock, resp, sizeof(resp) - 1, 0);

            zmq_close(sock);

            if (n > 0) {
                vivo = 1;
                printf("[ELEICAO] %s respondeu OK\n", nome);
            } else {
                printf("[ELEICAO] %s nao respondeu\n", nome);
            }
        }

        if (vivo && rank < melhor_rank) {
            melhor_rank = rank;
            strcpy(melhor_nome, nome);
        }

        p = q2 + 1;
    }

    if (strlen(melhor_nome) > 0) {
        strcpy(coordenador, melhor_nome);
        publicar_coordenador(pub_socket);

        printf("[ELEICAO] Coordenador eleito: %s\n", coordenador);
    }
}

void pedir_lista_servidores(void *ref_socket, void *pub_socket) {
    char json[256];
    char buffer[BUFFER];

    snprintf(json, sizeof(json), "{\"type\":\"list\"}");

    zmq_send(ref_socket, json, strlen(json), 0);

    int tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer) - 1, 0);

    if (tamanho <= 0) {
        return;
    }

    buffer[tamanho] = '\0';

    printf("[SERVIDORES ATIVOS]\n");

    if (!coordenador_esta_vivo()) {
        if (strlen(coordenador) > 0) {
            printf("[FALHA] Coordenador caiu: %s\n", coordenador);
        }

        coordenador[0] = '\0';

        printf("[ELEICAO] Escolhendo novo coordenador...\n");

        eleger_coordenador_direto(buffer, pub_socket);

    } else {
        printf("[OK] Coordenador ainda ativo: %s\n", coordenador);
    }
}

int sou_coordenador() {
    return strcmp(coordenador, NOME_SERVIDOR) == 0;
}

void sincronizar_berkeley() {
    if (strlen(coordenador) == 0) {
        return;
    }

    if (sou_coordenador()) {
        printf(
            "[BERKELEY] Sou coordenador (%s), hora=%.0f\n",
            NOME_SERVIDOR,
            agora_corrigido()
        );

        return;
    }

    const char *porta = porta_do_servidor(coordenador);

    if (!porta) {
        return;
    }

    void *sock = zmq_socket(contexto_global, ZMQ_REQ);

    int timeout = 2000;

    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_connect(sock, porta);

    const char *req = "{\"type\":\"get_time\"}";

    zmq_send(sock, req, strlen(req), 0);

    char buf[128];

    int n = zmq_recv(sock, buf, sizeof(buf) - 1, 0);

    zmq_close(sock);

    if (n > 0) {
        buf[n] = '\0';

        double hora_coordenador = 0.0;

        char *ptr = strstr(buf, "\"time\":");

        if (ptr) {
            sscanf(ptr, "\"time\":%lf", &hora_coordenador);

            if (hora_coordenador > 0) {
                offset_relogio = hora_coordenador - agora();

                printf("[BERKELEY] Offset atualizado: %.4f\n", offset_relogio);
            }
        }

    } else {
        printf("[ERRO BERKELEY] %s\n", coordenador);
    }
}

void verificar_eleicao_berkeley(void *ref_socket, void *pub_socket) {
    pedir_lista_servidores(ref_socket, pub_socket);
    sincronizar_berkeley();
}


// ============================================================
// LOOP PRINCIPAL
// ============================================================

int main() {
    criar_pasta_dados();
    setvbuf(stdout, NULL, _IONBF, 0);

    srand(time(NULL));

    contexto_global = zmq_ctx_new();

    void *rep_socket = zmq_socket(contexto_global, ZMQ_REP);
    zmq_connect(rep_socket, "tcp://broker:5556");

    int timeout = 1000;
    zmq_setsockopt(rep_socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

    void *pub_socket = zmq_socket(contexto_global, ZMQ_PUB);
    zmq_connect(pub_socket, "tcp://proxy:5557");

    pub_socket_global = pub_socket;

    void *ref_socket = zmq_socket(contexto_global, ZMQ_REQ);
    zmq_connect(ref_socket, "tcp://referencia:5560");

    carregar_estado();

    printf("[SERVER C] Iniciado...\n");

    registrar_na_referencia(ref_socket);

    pthread_t tid_direto;
    pthread_create(&tid_direto, NULL, thread_servidor_direto, NULL);
    pthread_detach(tid_direto);

    pthread_t tid_rep;
    pthread_create(&tid_rep, NULL, thread_replicacao, NULL);
    pthread_detach(tid_rep);

    while (1) {
        char buffer[BUFFER];

        int tamanho = zmq_recv(rep_socket, buffer, sizeof(buffer), 0);

        if (tamanho == -1 && errno == EAGAIN) {
            continue;
        }

        if (tamanho <= 0) {
            continue;
        }

        pthread_mutex_lock(&lock_estado);
        carregar_estado();
        pthread_mutex_unlock(&lock_estado);

        contador_requisicoes++;

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        if (!msgpack_unpack_next(&msg, buffer, tamanho, NULL)) {
            msgpack_unpacked_destroy(&msg);
            continue;
        }

        msgpack_object obj = msg.data;

        char tipo[64] = "";
        char usuario[128] = "";
        char canal[128] = "";
        char texto[512] = "";

        double timestamp = agora_corrigido();
        int contador_recebido = 0;

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];

                if (kv->key.type != MSGPACK_OBJECT_STR) {
                    continue;
                }

                char key[64] = {0};

                snprintf(
                    key,
                    sizeof(key),
                    "%.*s",
                    (int)kv->key.via.str.size,
                    kv->key.via.str.ptr
                );

                if (strcmp(key, "type") == 0) {
                    extrair_string_msgpack(kv->val, tipo, sizeof(tipo));

                } else if (strcmp(key, "user") == 0) {
                    extrair_string_msgpack(kv->val, usuario, sizeof(usuario));

                } else if (strcmp(key, "channel") == 0) {
                    extrair_string_msgpack(kv->val, canal, sizeof(canal));

                } else if (strcmp(key, "message") == 0) {
                    extrair_string_msgpack(kv->val, texto, sizeof(texto));

                } else if (strcmp(key, "timestamp") == 0) {
                    timestamp = extrair_double_msgpack(kv->val);

                } else if (strcmp(key, "contador") == 0) {
                    contador_recebido = extrair_int_msgpack(kv->val);
                }
            }
        }

        atualizar_contador_recebido(contador_recebido);

        char linha_req[1024];

        snprintf(
            linha_req,
            sizeof(linha_req),
            "{\"type\":\"%s\",\"user\":\"%s\",\"received_timestamp\":%.0f,\"contador\":%d}",
            tipo,
            usuario,
            agora_corrigido(),
            contador_servidor
        );

        salvar_linha_jsonl(ARQUIVO_REQUISICOES, linha_req);

        printf(
            "[SERVER C] tipo=%s | user=%s | canal=%s | contador=%d | coordenador=%s\n",
            tipo,
            usuario,
            canal,
            contador_servidor,
            coordenador
        );

        if (strcmp(tipo, "login") == 0) {
            pthread_mutex_lock(&lock_estado);
            adicionar_login_local(usuario, timestamp);
            pthread_mutex_unlock(&lock_estado);

            publicar_replicacao_login(usuario, timestamp);

            char msg_resposta[256];

            snprintf(
                msg_resposta,
                sizeof(msg_resposta),
                "login realizado (%s)",
                usuario
            );

            resposta_simples(rep_socket, "ok", msg_resposta);

        } else if (strcmp(tipo, "create_channel") == 0) {
            if (strlen(canal) == 0) {
                resposta_simples(rep_socket, "error", "nome de canal inválido");

            } else if (canal_existe(canal)) {
                resposta_simples(rep_socket, "error", "canal já existe");

            } else {
                pthread_mutex_lock(&lock_estado);
                adicionar_canal_local(canal);
                pthread_mutex_unlock(&lock_estado);

                publicar_replicacao_canal(canal);

                char msg_resposta[256];

                snprintf(
                    msg_resposta,
                    sizeof(msg_resposta),
                    "canal '%s' criado",
                    canal
                );

                resposta_simples(rep_socket, "ok", msg_resposta);
            }

        } else if (strcmp(tipo, "list_channels") == 0) {
            resposta_lista_canais(rep_socket);

        } else if (strcmp(tipo, "publish_message") == 0) {
            if (!canal_existe(canal)) {
                resposta_simples(rep_socket, "error", "canal inexistente");

            } else {
                publicar_no_canal(pub_socket, usuario, canal, texto, timestamp);
                publicar_replicacao_publicacao(usuario, canal, texto, timestamp);

                char msg_resposta[256];

                snprintf(
                    msg_resposta,
                    sizeof(msg_resposta),
                    "mensagem publicada em '%s'",
                    canal
                );

                resposta_simples(rep_socket, "ok", msg_resposta);
            }

        } else {
            resposta_simples(rep_socket, "error", "tipo inválido");
        }

        if (contador_requisicoes % INTERVALO_HEARTBEAT == 0) {
            enviar_heartbeat(ref_socket);
        }

        if (contador_requisicoes % INTERVALO_BERKELEY == 0) {
            verificar_eleicao_berkeley(ref_socket, pub_socket);
        }

        msgpack_unpacked_destroy(&msg);
    }

    return 0;
}