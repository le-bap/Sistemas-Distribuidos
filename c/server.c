#include <zmq.h>
#include <msgpack.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

#define MAX_CANAIS  1000
#define MAX_LOGINS  1000
#define BUFFER      65536
#define BUFFER_REP  65536

char *canais[MAX_CANAIS];
int   qtd_canais = 0;

char *logins[MAX_LOGINS];
int   qtd_logins = 0;

const char *PASTA_DADOS         = "data";
const char *ARQUIVO_CANAIS      = "/app/shared/channels.json";
const char *ARQUIVO_LOGINS      = "data/logins.json";
const char *ARQUIVO_REQUISICOES = "data/requests.jsonl";
const char *ARQUIVO_PUBLICACOES = "data/publications.jsonl";
const char *ARQUIVO_COORDENADOR = "/app/shared/coordenador.json";

const char *NOME_SERVIDOR = "server_c";

/* ranks fixos — consistentes entre os 3 servidores */
#define RANK_SERVER_C      1
#define RANK_SERVER_PYTHON 2
#define RANK_SERVER_JAVA   3
#define MEU_RANK           RANK_SERVER_C

#define PORTA_DIRETA_C      "tcp://*:5570"
#define ADDR_SERVER_C       "tcp://server_c:5570"
#define ADDR_SERVER_PYTHON  "tcp://server_python:5571"
#define ADDR_SERVER_JAVA    "tcp://server_java:5572"

#define PORTA_REPLICACAO_C  "tcp://*:5580"
#define ADDR_REPLIC_C       "tcp://server_c:5580"
#define ADDR_REPLIC_PYTHON  "tcp://server_python:5581"
#define ADDR_REPLIC_JAVA    "tcp://server_java:5582"

char coordenador[64] = "";

int    contador_servidor    = 0;
int    contador_requisicoes = 0;
double offset_relogio       = 0.0;
int    rank_servidor        = 0;

void *contexto_global    = NULL;
void *pub_socket_global  = NULL;
void *ref_socket_global  = NULL;

pthread_mutex_t write_lock   = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t ref_lock     = PTHREAD_MUTEX_INITIALIZER;   /* protege ref_socket */

/* Atomic flag para prevenir dupla eleição */
atomic_int eleicao_em_andamento = 0;

/* ════════════════════════════════════════════════════════════════════════
   Tempo e contador
   ════════════════════════════════════════════════════════════════════════ */

double agora() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

double agora_corrigido() { return agora() + offset_relogio; }

void atualizar_contador_recebido(int cnt) {
    if (cnt > contador_servidor) contador_servidor = cnt;
}

int proximo_contador() { return ++contador_servidor; }

/* ════════════════════════════════════════════════════════════════════════
   Disco
   ════════════════════════════════════════════════════════════════════════ */

void criar_pasta_dados() { mkdir(PASTA_DADOS, 0777); }

int canal_existe(const char *nome) {
    for (int i = 0; i < qtd_canais; i++)
        if (strcmp(canais[i], nome) == 0) return 1;
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
    for (int i = 0; i < qtd_canais; i++) { free(canais[i]); canais[i] = NULL; }
    qtd_canais = 0;
}

void limpar_logins_memoria() {
    for (int i = 0; i < qtd_logins; i++) { free(logins[i]); logins[i] = NULL; }
    qtd_logins = 0;
}

void carregar_canais() {
    limpar_canais_memoria();
    FILE *f = fopen(ARQUIVO_CANAIS, "r");
    if (!f) return;
    char linha[256];
    while (fgets(linha, sizeof(linha), f)) {
        char nome[128];
        if (sscanf(linha, " \"%127[^\"]\"", nome) == 1)
            canais[qtd_canais++] = strdup(nome);
    }
    fclose(f);
}

void carregar_logins() {
    limpar_logins_memoria();
    FILE *f = fopen(ARQUIVO_LOGINS, "r");
    if (!f) return;
    char linha[512];
    while (fgets(linha, sizeof(linha), f))
        if (strstr(linha, "\"user\"") != NULL)
            logins[qtd_logins++] = strdup(linha);
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

void adicionar_canal(const char *nome) {
    if (qtd_canais < MAX_CANAIS) {
        canais[qtd_canais++] = strdup(nome);
        salvar_canais();
    }
}

void adicionar_login(const char *usuario, double timestamp) {
    if (qtd_logins < MAX_LOGINS) {
        char linha[256];
        snprintf(linha, sizeof(linha),
                 "{\"user\":\"%s\",\"timestamp\":%.6f}", usuario, timestamp);
        logins[qtd_logins++] = strdup(linha);
        salvar_logins();
    }
}

/* ════════════════════════════════════════════════════════════════════════
   Msgpack helpers
   ════════════════════════════════════════════════════════════════════════ */

void pack_string(msgpack_packer *pk, const char *texto) {
    msgpack_pack_str(pk, strlen(texto));
    msgpack_pack_str_body(pk, texto, strlen(texto));
}

void resposta_simples(void *socket, const char *status, const char *message) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);
    pack_string(&pk, "status");      pack_string(&pk, status);
    pack_string(&pk, "message");     pack_string(&pk, message);
    pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
    pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
    pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);

    zmq_send(socket, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
}

void resposta_lista_canais(void *socket) {
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 5);
    pack_string(&pk, "status");      pack_string(&pk, "ok");
    pack_string(&pk, "channels");    msgpack_pack_array(&pk, qtd_canais);
    for (int i = 0; i < qtd_canais; i++) pack_string(&pk, canais[i]);
    pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
    pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
    pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);

    zmq_send(socket, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
}

/* ════════════════════════════════════════════════════════════════════════
   Pub/Sub
   ════════════════════════════════════════════════════════════════════════ */

void publicar_no_canal(void *pub_sock, const char *usuario, const char *canal,
                        const char *texto, double request_timestamp) {
    double published_timestamp = agora_corrigido();
    int    contador_pub        = proximo_contador();

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 6);
    pack_string(&pk, "user");                pack_string(&pk, usuario);
    pack_string(&pk, "channel");             pack_string(&pk, canal);
    pack_string(&pk, "message");             pack_string(&pk, texto);
    pack_string(&pk, "request_timestamp");   msgpack_pack_double(&pk, request_timestamp);
    pack_string(&pk, "published_timestamp"); msgpack_pack_double(&pk, published_timestamp);
    pack_string(&pk, "contador");            msgpack_pack_int(&pk, contador_pub);

    zmq_send(pub_sock, canal, strlen(canal), ZMQ_SNDMORE);
    zmq_send(pub_sock, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);

    char linha[1024];
    snprintf(linha, sizeof(linha),
             "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\","
             "\"request_timestamp\":%.6f,\"published_timestamp\":%.6f,\"contador\":%d}",
             canal, usuario, texto, request_timestamp, published_timestamp, contador_pub);
    salvar_linha_jsonl(ARQUIVO_PUBLICACOES, linha);
}

/* ════════════════════════════════════════════════════════════════════════
   Declarações antecipadas
   ════════════════════════════════════════════════════════════════════════ */

void eleger_coordenador(void *pub_sock);
void pedir_lista_e_eleger(void *pub_sock);
void salvar_coordenador_arquivo(void);
void publicar_coordenador(void *pub_sock);

/* ════════════════════════════════════════════════════════════════════════
   Utilitários de eleição
   ════════════════════════════════════════════════════════════════════════ */

int sou_coordenador() { return strcmp(coordenador, NOME_SERVIDOR) == 0; }

const char *porta_do_servidor(const char *nome) {
    if (strcmp(nome, "server_c") == 0)      return ADDR_SERVER_C;
    if (strcmp(nome, "server_python") == 0) return ADDR_SERVER_PYTHON;
    if (strcmp(nome, "server_java") == 0)   return ADDR_SERVER_JAVA;
    return NULL;
}

int rank_do_servidor(const char *nome) {
    if (strcmp(nome, "server_c") == 0)      return RANK_SERVER_C;
    if (strcmp(nome, "server_python") == 0) return RANK_SERVER_PYTHON;
    if (strcmp(nome, "server_java") == 0)   return RANK_SERVER_JAVA;
    return 999;
}

int servidor_responde_eleicao(const char *nome) {
    const char *porta = porta_do_servidor(nome);
    if (!porta) return 0;
    void *sock = zmq_socket(contexto_global, ZMQ_REQ);
    int timeout = 1000;
    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_connect(sock, porta);
    const char *em = "{\"type\":\"election\"}";
    zmq_send(sock, em, strlen(em), 0);
    char resp[64];
    int n = zmq_recv(sock, resp, sizeof(resp) - 1, 0);
    zmq_close(sock);
    if (n > 0) { resp[n] = '\0'; return strstr(resp, "ok") != NULL; }
    return 0;
}

/* ════════════════════════════════════════════════════════════════════════
   Replicação Passiva (Primary-Backup)
   ════════════════════════════════════════════════════════════════════════ */

void replicar_para_backups_msgpack(const char *tipo,
                                    const char *canal,
                                    const char *usuario,
                                    const char *texto,
                                    double request_timestamp,
                                    double published_timestamp,
                                    int    contador_pub) {
    const char *todos[] = { ADDR_REPLIC_C, ADDR_REPLIC_PYTHON, ADDR_REPLIC_JAVA };
    const char *nomes[] = { "server_c",    "server_python",    "server_java"    };

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    if (strcmp(tipo, "replicate_login") == 0) {
        msgpack_pack_map(&pk, 3);
        pack_string(&pk, "type");      pack_string(&pk, tipo);
        pack_string(&pk, "user");      pack_string(&pk, usuario);
        pack_string(&pk, "timestamp"); msgpack_pack_double(&pk, request_timestamp);

    } else if (strcmp(tipo, "replicate_channel") == 0) {
        msgpack_pack_map(&pk, 2);
        pack_string(&pk, "type");    pack_string(&pk, tipo);
        pack_string(&pk, "channel"); pack_string(&pk, canal);

    } else if (strcmp(tipo, "replicate_publish") == 0) {
        msgpack_pack_map(&pk, 7);
        pack_string(&pk, "type");                pack_string(&pk, tipo);
        pack_string(&pk, "channel");             pack_string(&pk, canal);
        pack_string(&pk, "user");                pack_string(&pk, usuario);
        pack_string(&pk, "message");             pack_string(&pk, texto);
        pack_string(&pk, "request_timestamp");   msgpack_pack_double(&pk, request_timestamp);
        pack_string(&pk, "published_timestamp"); msgpack_pack_double(&pk, published_timestamp);
        pack_string(&pk, "contador");            msgpack_pack_int(&pk, contador_pub);
    }

    for (int i = 0; i < 3; i++) {
        if (strcmp(nomes[i], NOME_SERVIDOR) == 0) continue;

        void *sock = zmq_socket(contexto_global, ZMQ_REQ);
        int timeout = 2000;
        zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
        zmq_connect(sock, todos[i]);
        zmq_send(sock, sbuf.data, sbuf.size, 0);

        char ack[256];
        int n = zmq_recv(sock, ack, sizeof(ack) - 1, 0);
        if (n > 0) { ack[n] = '\0'; printf("[REPLICA] ACK de %s: %s\n", nomes[i], ack); fflush(stdout); }
        else        { printf("[REPLICA] Sem ACK de %s\n", nomes[i]); fflush(stdout); }

        zmq_close(sock);
    }

    msgpack_sbuffer_destroy(&sbuf);
}

void aplicar_replicacao_campos(const char *tipo,
                                const char *usuario,
                                const char *canal,
                                const char *texto,
                                double request_timestamp,
                                double published_timestamp,
                                int    contador_pub) {
    pthread_mutex_lock(&write_lock);

    if (strcmp(tipo, "replicate_login") == 0) {
        if (strlen(usuario) > 0 && qtd_logins < MAX_LOGINS) {
            char linha[256];
            snprintf(linha, sizeof(linha),
                     "{\"user\":\"%s\",\"timestamp\":%.6f}", usuario, request_timestamp);
            logins[qtd_logins++] = strdup(linha);
            salvar_logins();
            printf("[REPLICA] login aplicado: %s\n", usuario); fflush(stdout);
        }
    } else if (strcmp(tipo, "replicate_channel") == 0) {
        if (strlen(canal) > 0 && !canal_existe(canal)) {
            adicionar_canal(canal);
            printf("[REPLICA] canal aplicado: %s\n", canal); fflush(stdout);
        }
    } else if (strcmp(tipo, "replicate_publish") == 0) {
        /* Sincroniza contador com o do primário */
        atualizar_contador_recebido(contador_pub);
        char linha[1024];
        snprintf(linha, sizeof(linha),
                 "{\"channel\":\"%s\",\"user\":\"%s\",\"message\":\"%s\","
                 "\"request_timestamp\":%.6f,\"published_timestamp\":%.6f,\"contador\":%d}",
                 canal, usuario, texto, request_timestamp, published_timestamp, contador_pub);
        salvar_linha_jsonl(ARQUIVO_PUBLICACOES, linha);
        printf("[REPLICA] publicação aplicada: canal=%s\n", canal); fflush(stdout);
    }

    pthread_mutex_unlock(&write_lock);
}

/* processar_escrita: só executado pelo primário */
void processar_escrita(const char *tipo, const char *usuario, const char *canal,
                        const char *texto, double timestamp,
                        msgpack_sbuffer *sbuf_out) {
    pthread_mutex_lock(&write_lock);
    carregar_canais();
    carregar_logins();

    msgpack_packer pk;
    msgpack_sbuffer_init(sbuf_out);
    msgpack_packer_init(&pk, sbuf_out, msgpack_sbuffer_write);

    if (strcmp(tipo, "login") == 0) {
        adicionar_login(usuario, timestamp);
        pthread_mutex_unlock(&write_lock);

        replicar_para_backups_msgpack("replicate_login", "", usuario, "", timestamp, 0, 0);

        msgpack_pack_map(&pk, 5);
        pack_string(&pk, "status");      pack_string(&pk, "ok");
        char msg[128];
        snprintf(msg, sizeof(msg), "login realizado (%s)", usuario);
        pack_string(&pk, "message");     pack_string(&pk, msg);
        pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
        pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
        pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
        return;

    } else if (strcmp(tipo, "create_channel") == 0) {
        if (strlen(canal) == 0) {
            pthread_mutex_unlock(&write_lock);
            msgpack_pack_map(&pk, 5);
            pack_string(&pk, "status");      pack_string(&pk, "error");
            pack_string(&pk, "message");     pack_string(&pk, "nome de canal inválido");
            pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
            pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
            pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
            return;
        }
        if (canal_existe(canal)) {
            pthread_mutex_unlock(&write_lock);
            msgpack_pack_map(&pk, 5);
            pack_string(&pk, "status");      pack_string(&pk, "error");
            pack_string(&pk, "message");     pack_string(&pk, "canal já existe");
            pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
            pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
            pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
            return;
        }
        adicionar_canal(canal);
        pthread_mutex_unlock(&write_lock);

        replicar_para_backups_msgpack("replicate_channel", canal, "", "", 0, 0, 0);

        msgpack_pack_map(&pk, 5);
        pack_string(&pk, "status");      pack_string(&pk, "ok");
        char msg[128];
        snprintf(msg, sizeof(msg), "canal '%s' criado", canal);
        pack_string(&pk, "message");     pack_string(&pk, msg);
        pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
        pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
        pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
        return;

    } else if (strcmp(tipo, "publish_message") == 0) {
        if (!canal_existe(canal)) {
            pthread_mutex_unlock(&write_lock);
            msgpack_pack_map(&pk, 5);
            pack_string(&pk, "status");      pack_string(&pk, "error");
            pack_string(&pk, "message");     pack_string(&pk, "canal inexistente");
            pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
            pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
            pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
            return;
        }

        double pts = agora_corrigido();
        int    cnt = proximo_contador();
        pthread_mutex_unlock(&write_lock);

        publicar_no_canal(pub_socket_global, usuario, canal, texto, timestamp);
        replicar_para_backups_msgpack("replicate_publish", canal, usuario, texto,
                                       timestamp, pts, cnt);

        msgpack_pack_map(&pk, 5);
        pack_string(&pk, "status");      pack_string(&pk, "ok");
        char msg[128];
        snprintf(msg, sizeof(msg), "mensagem publicada em '%s'", canal);
        pack_string(&pk, "message");     pack_string(&pk, msg);
        pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
        pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
        pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
        return;
    }

    pthread_mutex_unlock(&write_lock);
    msgpack_pack_map(&pk, 5);
    pack_string(&pk, "status");      pack_string(&pk, "error");
    pack_string(&pk, "message");     pack_string(&pk, "tipo inválido");
    pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
    pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
    pack_string(&pk, "coordenador"); pack_string(&pk, coordenador);
}

/*
 * Backup encaminha mensagem para o primário.
 * Em falha: limpa coordenador e dispara eleição imediata.
 */
int encaminhar_para_primario(const char *msg_raw, size_t msg_size,
                              char *resposta_out, size_t *resposta_size) {
    const char *porta = NULL;
    if      (strcmp(coordenador, "server_c")      == 0) porta = ADDR_REPLIC_C;
    else if (strcmp(coordenador, "server_python") == 0) porta = ADDR_REPLIC_PYTHON;
    else if (strcmp(coordenador, "server_java")   == 0) porta = ADDR_REPLIC_JAVA;
    if (!porta) return 0;

    void *sock = zmq_socket(contexto_global, ZMQ_REQ);
    int timeout = 3000;
    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_connect(sock, porta);
    zmq_send(sock, msg_raw, msg_size, 0);

    int n = zmq_recv(sock, resposta_out, BUFFER_REP - 1, 0);
    zmq_close(sock);

    if (n > 0) { *resposta_size = n; return 1; }

    /* Primário falhou — limpa e elege imediatamente */
    printf("[ENCAMINHAR] Primário %s não respondeu. Iniciando eleição.\n",
           coordenador); fflush(stdout);
    coordenador[0] = '\0';
    pedir_lista_e_eleger(pub_socket_global);
    return 0;
}

/* Thread de replicação */
void *thread_replicacao(void *arg) {
    void *sock = zmq_socket(contexto_global, ZMQ_REP);
    zmq_bind(sock, PORTA_REPLICACAO_C);
    printf("[REPLICA] Thread de replicação escutando na porta 5580\n"); fflush(stdout);

    static char buf[BUFFER_REP];

    while (1) {
        int n = zmq_recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) continue;

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        if (!msgpack_unpack_next(&msg, buf, n, NULL)) {
            msgpack_unpacked_destroy(&msg);
            zmq_send(sock, "{\"status\":\"error\"}", 17, 0);
            continue;
        }

        msgpack_object obj = msg.data;

        char   tipo[64]    = "";
        char   usuario[64] = "";
        char   canal[64]   = "";
        char   texto[512]  = "";
        double timestamp            = agora_corrigido();
        double published_timestamp  = agora_corrigido();
        int    contador_pub         = 0;

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < (int)obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];
                if (kv->key.type != MSGPACK_OBJECT_STR) continue;
                char key[64] = {0};
                snprintf(key, sizeof(key), "%.*s",
                         (int)kv->key.via.str.size, kv->key.via.str.ptr);

                if (strcmp(key, "type") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(tipo, sizeof(tipo), "%.*s",
                             (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "user") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(usuario, sizeof(usuario), "%.*s",
                             (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "channel") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(canal, sizeof(canal), "%.*s",
                             (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(texto, sizeof(texto), "%.*s",
                             (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "timestamp") == 0 ||
                         strcmp(key, "request_timestamp") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_FLOAT64 ||
                        kv->val.type == MSGPACK_OBJECT_FLOAT32)
                        timestamp = kv->val.via.f64;
                    else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                        timestamp = (double)kv->val.via.u64;
                }
                else if (strcmp(key, "published_timestamp") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_FLOAT64 ||
                        kv->val.type == MSGPACK_OBJECT_FLOAT32)
                        published_timestamp = kv->val.via.f64;
                    else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                        published_timestamp = (double)kv->val.via.u64;
                }
                else if (strcmp(key, "contador") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                        contador_pub = (int)kv->val.via.u64;
                    else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER)
                        contador_pub = (int)kv->val.via.i64;
                }
            }
        }

        msgpack_unpacked_destroy(&msg);

        if (strncmp(tipo, "replicate_", 10) == 0) {
            aplicar_replicacao_campos(tipo, usuario, canal, texto,
                                      timestamp, published_timestamp, contador_pub);
            zmq_send(sock, "{\"status\":\"ok\"}", 14, 0);

        } else if (strcmp(tipo, "login") == 0 ||
                   strcmp(tipo, "create_channel") == 0 ||
                   strcmp(tipo, "publish_message") == 0) {
            msgpack_sbuffer sbuf_resp;
            processar_escrita(tipo, usuario, canal, texto, timestamp, &sbuf_resp);
            zmq_send(sock, sbuf_resp.data, sbuf_resp.size, 0);
            msgpack_sbuffer_destroy(&sbuf_resp);

        } else {
            zmq_send(sock, "{\"status\":\"error\"}", 17, 0);
        }
    }
    return NULL;
}

/* Thread SUB — escuta anúncios de coordenador via pub/sub */
void *thread_sub_servers(void *arg) {
    void *sub = zmq_socket(contexto_global, ZMQ_SUB);
    zmq_connect(sub, "tcp://proxy:5558");
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "servers", 7);
    printf("[SUB] Escutando anúncios de coordenador em 'servers'\n"); fflush(stdout);

    static char topico[64];
    static char payload[BUFFER];

    while (1) {
        /* Recebe tópico */
        int nt = zmq_recv(sub, topico, sizeof(topico) - 1, 0);
        if (nt <= 0) continue;
        topico[nt] = '\0';

        /* Recebe payload */
        int np = zmq_recv(sub, payload, sizeof(payload) - 1, 0);
        if (np <= 0) continue;

        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);
        if (!msgpack_unpack_next(&msg, payload, np, NULL)) {
            msgpack_unpacked_destroy(&msg);
            continue;
        }

        msgpack_object obj = msg.data;
        char tipo_anuncio[64] = "";
        char novo_coord[64]   = "";

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < (int)obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];
                if (kv->key.type != MSGPACK_OBJECT_STR) continue;
                char key[64] = {0};
                snprintf(key, sizeof(key), "%.*s",
                         (int)kv->key.via.str.size, kv->key.via.str.ptr);
                if (strcmp(key, "type") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(tipo_anuncio, sizeof(tipo_anuncio), "%.*s",
                             (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "coordinator") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(novo_coord, sizeof(novo_coord), "%.*s",
                             (int)kv->val.via.str.size, kv->val.via.str.ptr);
            }
        }

        msgpack_unpacked_destroy(&msg);

        if (strcmp(tipo_anuncio, "coordinator_announce") == 0 &&
            strlen(novo_coord) > 0 &&
            strcmp(novo_coord, coordenador) != 0) {
            printf("[SUB] Coordenador atualizado via anúncio: %s\n", novo_coord); fflush(stdout);
            strncpy(coordenador, novo_coord, sizeof(coordenador) - 1);
            salvar_coordenador_arquivo();
        }
    }
    return NULL;
}

/* ════════════════════════════════════════════════════════════════════════
   Eleição / Berkeley
   ════════════════════════════════════════════════════════════════════════ */

void registrar_na_referencia() {
    char json[256];
    snprintf(json, sizeof(json),
             "{\"type\":\"register\",\"name\":\"%s\"}", NOME_SERVIDOR);
    pthread_mutex_lock(&ref_lock);
    zmq_send(ref_socket_global, json, strlen(json), 0);
    static char buffer[BUFFER];
    int tamanho = zmq_recv(ref_socket_global, buffer, sizeof(buffer) - 1, 0);
    pthread_mutex_unlock(&ref_lock);
    if (tamanho <= 0) return;
    buffer[tamanho] = '\0';
    char *rank_ptr = strstr(buffer, "\"rank\":");
    if (rank_ptr) sscanf(rank_ptr, "\"rank\":%d", &rank_servidor);
    printf("[SERVER C] Meu rank: %d\n", rank_servidor); fflush(stdout);
}

void salvar_coordenador_arquivo() {
    FILE *f = fopen(ARQUIVO_COORDENADOR, "w");
    if (!f) return;
    fprintf(f,
            "{\n  \"coordinator\": \"%s\",\n  \"timestamp\": %.6f,\n  \"clock\": %d\n}\n",
            coordenador, agora_corrigido(), contador_servidor);
    fclose(f);
}

void publicar_coordenador(void *pub_sock) {
    if (strlen(coordenador) == 0) return;
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    msgpack_pack_map(&pk, 5);
    pack_string(&pk, "type");        pack_string(&pk, "coordinator_announce");
    pack_string(&pk, "coordinator"); pack_string(&pk, coordenador);
    pack_string(&pk, "server");      pack_string(&pk, NOME_SERVIDOR);
    pack_string(&pk, "timestamp");   msgpack_pack_double(&pk, agora_corrigido());
    pack_string(&pk, "contador");    msgpack_pack_int(&pk, proximo_contador());
    zmq_send(pub_sock, "servers", 7, ZMQ_SNDMORE);
    zmq_send(pub_sock, sbuf.data, sbuf.size, 0);
    msgpack_sbuffer_destroy(&sbuf);
    printf("[PUB SERVERS] coordenador eleito: %s\n", coordenador); fflush(stdout);
}

void *thread_servidor_direto(void *arg) {
    void *sock = zmq_socket(contexto_global, ZMQ_REP);
    zmq_bind(sock, PORTA_DIRETA_C);
    printf("[SERVER C] Thread direta escutando na porta 5570\n"); fflush(stdout);
    static char buf[256];
    while (1) {
        int n = zmq_recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) continue;
        buf[n] = '\0';
        if      (strstr(buf, "election")) zmq_send(sock, "{\"status\":\"ok\"}", 14, 0);
        else if (strstr(buf, "get_time")) {
            char resp[64];
            snprintf(resp, sizeof(resp), "{\"time\":%.6f}", agora_corrigido());
            zmq_send(sock, resp, strlen(resp), 0);
        } else zmq_send(sock, "{\"status\":\"error\"}", 17, 0);
    }
    return NULL;
}

/*
 * Eleição principal.
 * Usa atomic flag para garantir que apenas uma eleição rode por vez.
 * Apenas o servidor de menor rank entre os vivos publica o resultado.
 */
void eleger_coordenador(void *pub_sock) {
    int esperado = 0;
    if (!atomic_compare_exchange_strong(&eleicao_em_andamento, &esperado, 1)) {
        printf("[ELEICAO] Já em andamento, ignorando.\n"); fflush(stdout);
        return;
    }

    printf("[ELEICAO] Iniciando eleição...\n"); fflush(stdout);

    const char *todos_nomes[] = { "server_c", "server_python", "server_java" };
    int         todos_ranks[] = { RANK_SERVER_C, RANK_SERVER_PYTHON, RANK_SERVER_JAVA };

    char   nomes_vivos[3][64];
    int    ranks_vivos[3];
    int    qtd_vivos = 0;

    for (int i = 0; i < 3; i++) {
        if (servidor_responde_eleicao(todos_nomes[i])) {
            strncpy(nomes_vivos[qtd_vivos], todos_nomes[i], 63);
            ranks_vivos[qtd_vivos] = todos_ranks[i];
            qtd_vivos++;
            printf("[ELEICAO] %s respondeu OK\n", todos_nomes[i]); fflush(stdout);
        } else {
            printf("[ELEICAO] %s não respondeu\n", todos_nomes[i]); fflush(stdout);
        }
    }

    if (qtd_vivos == 0) {
        /* Fallback: assume todos */
        for (int i = 0; i < 3; i++) {
            strncpy(nomes_vivos[i], todos_nomes[i], 63);
            ranks_vivos[i] = todos_ranks[i];
        }
        qtd_vivos = 3;
    }

    /* Menor rank vence */
    int melhor_idx  = 0;
    for (int i = 1; i < qtd_vivos; i++)
        if (ranks_vivos[i] < ranks_vivos[melhor_idx])
            melhor_idx = i;

    strncpy(coordenador, nomes_vivos[melhor_idx], sizeof(coordenador) - 1);
    salvar_coordenador_arquivo();

    /* Só o servidor de menor rank entre os vivos publica o resultado */
    int menor_rank_vivo = ranks_vivos[melhor_idx];
    if (MEU_RANK == menor_rank_vivo) {
        publicar_coordenador(pub_sock);
        printf("[ELEICAO] Coordenador eleito: %s\n", coordenador); fflush(stdout);
    } else {
        printf("[ELEICAO] Aguardando anúncio. Coordenador: %s\n", coordenador); fflush(stdout);
    }

    atomic_store(&eleicao_em_andamento, 0);
}

void pedir_lista_e_eleger(void *pub_sock) {
    static char buffer[BUFFER];
    const char *json = "{\"type\":\"list\"}";
    pthread_mutex_lock(&ref_lock);
    zmq_send(ref_socket_global, json, strlen(json), 0);
    int tamanho = zmq_recv(ref_socket_global, buffer, sizeof(buffer) - 1, 0);
    pthread_mutex_unlock(&ref_lock);
    if (tamanho <= 0) return;
    buffer[tamanho] = '\0';
    printf("[SERVIDORES ATIVOS]\n"); fflush(stdout);
    eleger_coordenador(pub_sock);
}

void enviar_heartbeat() {
    static char json[256];
    static char buffer[BUFFER];
    snprintf(json, sizeof(json),
             "{\"type\":\"heartbeat\",\"name\":\"%s\"}", NOME_SERVIDOR);
    pthread_mutex_lock(&ref_lock);
    zmq_send(ref_socket_global, json, strlen(json), 0);
    int tamanho = zmq_recv(ref_socket_global, buffer, sizeof(buffer) - 1, 0);
    pthread_mutex_unlock(&ref_lock);
    if (tamanho > 0) { buffer[tamanho] = '\0'; printf("[HEARTBEAT] resposta=%s\n", buffer); fflush(stdout); }
}

void sincronizar_berkeley() {
    if (strlen(coordenador) == 0) return;
    if (sou_coordenador()) {
        printf("[BERKELEY] Sou coordenador (%s), hora=%.6f\n",
               NOME_SERVIDOR, agora_corrigido()); fflush(stdout);
        return;
    }
    const char *porta = porta_do_servidor(coordenador);
    if (!porta) return;
    void *sock = zmq_socket(contexto_global, ZMQ_REQ);
    int timeout = 2000;
    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    zmq_connect(sock, porta);
    const char *req = "{\"type\":\"get_time\"}";
    zmq_send(sock, req, strlen(req), 0);
    char buf[128]; int n = zmq_recv(sock, buf, sizeof(buf) - 1, 0);
    zmq_close(sock);
    if (n > 0) {
        buf[n] = '\0';
        double hora_coordenador = 0.0;
        char *ptr = strstr(buf, "\"time\":");
        if (ptr) {
            sscanf(ptr, "\"time\":%lf", &hora_coordenador);
            if (hora_coordenador > 0) {
                offset_relogio = hora_coordenador - agora();
                printf("[BERKELEY] Offset atualizado: %.6f\n", offset_relogio); fflush(stdout);
            }
        }
    } else { printf("[ERRO BERKELEY] sem resposta de %s\n", coordenador); fflush(stdout); }
}

void parte4_a_cada_15_mensagens() {
    enviar_heartbeat();

    /* Verifica se o coordenador atual ainda responde */
    if (strlen(coordenador) > 0 && !servidor_responde_eleicao(coordenador)) {
        printf("[FALHA] Coordenador caiu: %s\n", coordenador); fflush(stdout);
        coordenador[0] = '\0';
    }

    if (strlen(coordenador) == 0) {
        pedir_lista_e_eleger(pub_socket_global);
    } else {
        printf("[OK] Coordenador ainda ativo: %s\n", coordenador); fflush(stdout);
    }

    sincronizar_berkeley();
}

/* ════════════════════════════════════════════════════════════════════════
   main
   ════════════════════════════════════════════════════════════════════════ */

int main() {
    criar_pasta_dados();
    setvbuf(stdout, NULL, _IONBF, 0);

    contexto_global = zmq_ctx_new();

    void *rep_socket = zmq_socket(contexto_global, ZMQ_REP);
    zmq_connect(rep_socket, "tcp://broker:5556");
    int timeout = 1000;
    zmq_setsockopt(rep_socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

    pub_socket_global = zmq_socket(contexto_global, ZMQ_PUB);
    zmq_connect(pub_socket_global, "tcp://proxy:5557");

    ref_socket_global = zmq_socket(contexto_global, ZMQ_REQ);
    zmq_connect(ref_socket_global, "tcp://referencia:5560");

    printf("[SERVER C] Iniciado...\n"); fflush(stdout);

    registrar_na_referencia();

    pthread_t tid_direto;
    pthread_create(&tid_direto, NULL, thread_servidor_direto, NULL);
    pthread_detach(tid_direto);

    pthread_t tid_replic;
    pthread_create(&tid_replic, NULL, thread_replicacao, NULL);
    pthread_detach(tid_replic);

    pthread_t tid_sub;
    pthread_create(&tid_sub, NULL, thread_sub_servers, NULL);
    pthread_detach(tid_sub);

    static char buffer[BUFFER];
    static char resp_buf[BUFFER_REP];

    int contador_req = 0;

    while (1) {
        int tamanho = zmq_recv(rep_socket, buffer, sizeof(buffer) - 1, 0);

        if (tamanho == -1 && errno == EAGAIN) continue;
        if (tamanho <= 0) continue;

        carregar_canais();
        carregar_logins();
        contador_req++;

        msgpack_unpacked msg_up;
        msgpack_unpacked_init(&msg_up);

        if (!msgpack_unpack_next(&msg_up, buffer, tamanho, NULL)) {
            msgpack_unpacked_destroy(&msg_up);
            resposta_simples(rep_socket, "error", "mensagem inválida");
            continue;
        }

        msgpack_object obj = msg_up.data;

        char   tipo[64]    = "";
        char   usuario[64] = "";
        char   canal[64]   = "";
        char   texto[512]  = "";
        double timestamp         = agora_corrigido();
        int    contador_recebido = 0;

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (int i = 0; i < (int)obj.via.map.size; i++) {
                msgpack_object_kv *kv = &obj.via.map.ptr[i];
                if (kv->key.type != MSGPACK_OBJECT_STR) continue;
                char key[64] = {0};
                snprintf(key, sizeof(key), "%.*s",
                         (int)kv->key.via.str.size, kv->key.via.str.ptr);
                if (strcmp(key, "type") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(tipo,    sizeof(tipo),    "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "user") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(usuario, sizeof(usuario), "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "channel") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(canal,   sizeof(canal),   "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "message") == 0 && kv->val.type == MSGPACK_OBJECT_STR)
                    snprintf(texto,   sizeof(texto),   "%.*s", (int)kv->val.via.str.size, kv->val.via.str.ptr);
                else if (strcmp(key, "timestamp") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_FLOAT32 || kv->val.type == MSGPACK_OBJECT_FLOAT64)
                        timestamp = kv->val.via.f64;
                    else if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                        timestamp = (double)kv->val.via.u64;
                } else if (strcmp(key, "contador") == 0) {
                    if (kv->val.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                        contador_recebido = (int)kv->val.via.u64;
                    else if (kv->val.type == MSGPACK_OBJECT_NEGATIVE_INTEGER)
                        contador_recebido = (int)kv->val.via.i64;
                }
            }
        }

        atualizar_contador_recebido(contador_recebido);

        char linha_req[512];
        snprintf(linha_req, sizeof(linha_req),
                 "{\"type\":\"%s\",\"user\":\"%s\",\"received_timestamp\":%.6f,\"contador\":%d}",
                 tipo, usuario, agora_corrigido(), contador_servidor);
        salvar_linha_jsonl(ARQUIVO_REQUISICOES, linha_req);

        printf("[SERVER C] tipo=%s | user=%s | canal=%s | contador=%d | coordenador=%s\n",
               tipo, usuario, canal, contador_servidor, coordenador); fflush(stdout);

        int eh_escrita = strcmp(tipo, "login") == 0 ||
                         strcmp(tipo, "create_channel") == 0 ||
                         strcmp(tipo, "publish_message") == 0;

        if (eh_escrita) {
            if (sou_coordenador() || strlen(coordenador) == 0) {
                msgpack_sbuffer sbuf_resp;
                processar_escrita(tipo, usuario, canal, texto, timestamp, &sbuf_resp);
                zmq_send(rep_socket, sbuf_resp.data, sbuf_resp.size, 0);
                msgpack_sbuffer_destroy(&sbuf_resp);
            } else {
                printf("[BACKUP] Encaminhando %s para primário %s\n", tipo, coordenador); fflush(stdout);
                size_t resp_size = 0;
                int ok = encaminhar_para_primario(buffer, tamanho, resp_buf, &resp_size);
                if (ok) {
                    zmq_send(rep_socket, resp_buf, resp_size, 0);
                } else {
                    resposta_simples(rep_socket, "error", "primário indisponível");
                }
            }

        } else if (strcmp(tipo, "list_channels") == 0) {
            resposta_lista_canais(rep_socket);

        } else {
            resposta_simples(rep_socket, "error", "tipo inválido");
        }

        msgpack_unpacked_destroy(&msg_up);

        if (contador_req % 15 == 0) {
            parte4_a_cada_15_mensagens();
        }
    }

    return 0;
}