#include <zmq.h>
#include <msgpack.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAX_CANAIS 1000
#define MAX_LOGINS 1000
#define BUFFER 4096

char *canais[MAX_CANAIS];
int qtd_canais = 0;

char *logins[MAX_LOGINS];
int qtd_logins = 0;

const char *PASTA_DADOS = "data";
const char *ARQUIVO_CANAIS = "/app/shared/channels.json";
const char *ARQUIVO_LOGINS = "data/logins.json";
const char *ARQUIVO_REQUISICOES = "data/requests.jsonl";
const char *ARQUIVO_PUBLICACOES = "data/publications.jsonl";
const char *ARQUIVO_COORDENADOR = "/app/shared/coordenador.json";

const char *NOME_SERVIDOR = "server_c";

char coordenador[64] = "";

int contador_servidor = 0;
int contador_requisicoes = 0;
double offset_relogio = 0.0;
int rank_servidor = 0;

// ================= CLOCK =================

double agora() {
    return (double)time(NULL);
}

double agora_corrigido() {
    return agora() + offset_relogio;
}

void atualizar_contador_recebido(int contador_recebido) {
    if (contador_recebido > contador_servidor)
        contador_servidor = contador_recebido;
}

int proximo_contador() {
    return ++contador_servidor;
}

// ================= UTILS =================

void criar_pasta_dados() {
    mkdir(PASTA_DADOS, 0777);
}

int coordenador_vivo(char *lista_json) {
    if (strlen(coordenador) == 0) return 0;
    return strstr(lista_json, coordenador) != NULL;
}

// ================= FILE =================

void salvar_linha_jsonl(const char *arquivo, const char *linha) {
    FILE *f = fopen(arquivo, "a");
    if (!f) return;
    fprintf(f, "%s\n", linha);
    fclose(f);
}

void salvar_coordenador_arquivo() {
    FILE *f = fopen(ARQUIVO_COORDENADOR, "w");
    if (!f) return;

    fprintf(f,
        "{ \"coordinator\":\"%s\", \"timestamp\":%.0f, \"clock\":%d }\n",
        coordenador, agora_corrigido(), contador_servidor
    );

    fclose(f);
}

// ================= ZMQ =================

void enviar_heartbeat(void *ref_socket) {
    char json[128];
    char buffer[BUFFER];

    snprintf(json, sizeof(json),
        "{\"type\":\"heartbeat\",\"name\":\"%s\"}", NOME_SERVIDOR);

    zmq_send(ref_socket, json, strlen(json), 0);
    zmq_recv(ref_socket, buffer, sizeof(buffer), 0);
}

// ================= ELEIÇÃO =================

void escolher_coordenador_da_lista(char *lista_json) {
    char *p = lista_json;
    int melhor_rank = 999999;
    char melhor_nome[64] = "";

    while ((p = strstr(p, "\"name\"")) != NULL) {
        char nome[64] = "";
        int rank = 999999;

        sscanf(p, "\"name\":\"%63[^\"]\"", nome);

        char *rank_ptr = strstr(p, "\"rank\"");
        if (rank_ptr) sscanf(rank_ptr, "\"rank\":%d", &rank);

        if (rank < melhor_rank) {
            melhor_rank = rank;
            strcpy(melhor_nome, nome);
        }

        p += 6;
    }

    if (strlen(melhor_nome) > 0)
        strcpy(coordenador, melhor_nome);
}

void publicar_coordenador(void *pub_socket) {
    if (strlen(coordenador) == 0) return;

    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&pk, 3);

    msgpack_pack_str_with_body(&pk, "type", 4);
    msgpack_pack_str_with_body(&pk, "coordinator_announce", 20);

    msgpack_pack_str_with_body(&pk, "coordinator", 11);
    msgpack_pack_str_with_body(&pk, coordenador, strlen(coordenador));

    msgpack_pack_str_with_body(&pk, "timestamp", 9);
    msgpack_pack_double(&pk, agora_corrigido());

    zmq_send(pub_socket, "servers", 7, ZMQ_SNDMORE);
    zmq_send(pub_socket, sbuf.data, sbuf.size, 0);

    printf("[ELEICAO] Novo coordenador: %s\n", coordenador);

    msgpack_sbuffer_destroy(&sbuf);
}

// ================= BERKELEY =================

void sincronizar_berkeley() {
    if (strlen(coordenador) == 0) return;

    if (strcmp(coordenador, NOME_SERVIDOR) == 0) {
        salvar_coordenador_arquivo();
        printf("[BERKELEY] Eu sou o coordenador\n");
    } else {
        printf("[BERKELEY] Coordenador atual: %s\n", coordenador);
    }
}

// ================= PARTE 4 =================

void parte4(void *ref_socket, void *pub_socket) {
    char json[64];
    char buffer[BUFFER];

    enviar_heartbeat(ref_socket);

    snprintf(json, sizeof(json), "{\"type\":\"list\"}");
    zmq_send(ref_socket, json, strlen(json), 0);

    int tamanho = zmq_recv(ref_socket, buffer, sizeof(buffer)-1, 0);
    if (tamanho <= 0) return;
    buffer[tamanho] = '\0';

    printf("[SERVIDORES]\n%s\n", buffer);

    if (strlen(coordenador) == 0) {
        printf("[ELEICAO] Nenhum coordenador. Elegendo...\n");
        escolher_coordenador_da_lista(buffer);
        salvar_coordenador_arquivo();
        publicar_coordenador(pub_socket);
    }
    else if (!coordenador_vivo(buffer)) {
        printf("[ELEICAO] Coordenador morreu!\n");
        escolher_coordenador_da_lista(buffer);
        salvar_coordenador_arquivo();
        publicar_coordenador(pub_socket);
    }

    sincronizar_berkeley();
}

// ================= MAIN =================

int main() {
    criar_pasta_dados();

    void *contexto = zmq_ctx_new();

    void *rep = zmq_socket(contexto, ZMQ_REP);
    zmq_connect(rep, "tcp://broker:5556");

    void *pub = zmq_socket(contexto, ZMQ_PUB);
    zmq_connect(pub, "tcp://proxy:5557");

    void *ref = zmq_socket(contexto, ZMQ_REQ);
    zmq_connect(ref, "tcp://referencia:5560");

    printf("[SERVER C] Iniciado...\n");

    // registra
    char json[128], buffer[BUFFER];
    snprintf(json, sizeof(json),
        "{\"type\":\"register\",\"name\":\"%s\"}", NOME_SERVIDOR);

    zmq_send(ref, json, strlen(json), 0);
    zmq_recv(ref, buffer, sizeof(buffer), 0);

    while (1) {
        zmq_recv(rep, buffer, BUFFER, 0);

        contador_requisicoes++;

        // resposta simples
        msgpack_sbuffer sbuf;
        msgpack_sbuffer_init(&sbuf);

        msgpack_packer pk;
        msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

        msgpack_pack_map(&pk, 1);
        msgpack_pack_str_with_body(&pk, "status", 6);
        msgpack_pack_str_with_body(&pk, "ok", 2);

        zmq_send(rep, sbuf.data, sbuf.size, 0);
        msgpack_sbuffer_destroy(&sbuf);

        if (contador_requisicoes % 15 == 0) {
            parte4(ref, pub);
        }
    }

    return 0;
}