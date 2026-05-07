# Projeto de Sistemas Distribuídos - Partes 1, 2, 3, 4 e 5

## Integrantes

* Rafaela Altheman de Campos
* Letizia Lowatzki Baptistella
* Manuella Filipe Peres

---

# Sobre o projeto

Este projeto foi desenvolvido para a disciplina de Sistemas Distribuídos.

Nas Partes 1 e 2, implementamos a comunicação entre clientes e servidores utilizando os padrões **Req/Rep** e **Pub/Sub**.

Nas Partes 3 e 4, adicionamos mecanismos de sincronização em sistemas distribuídos, como relógio lógico, heartbeat entre servidores, serviço de referência, ranking dos servidores, eleição de coordenador e sincronização baseada no algoritmo de Berkeley.

Na Parte 5, implementamos a **replicação de dados** entre os servidores, garantindo que todos mantenham uma cópia completa do histórico de mensagens e operações realizadas no sistema.

Com a troca de mensagens, os clientes conseguem realizar login, criar canais, listar canais, publicar mensagens, se inscrever em canais e receber mensagens publicadas. Além disso, o sistema mantém relógios lógicos sincronizados, monitora servidores ativos e realiza eleição automática de coordenador quando necessário.

Os servidores também armazenam os dados em disco para evitar perda de informações entre execuções.

---

# Arquitetura

O sistema utiliza dois padrões principais de comunicação:

## Req/Rep

Utilizado para login, criação de canais, listagem de canais, envio de mensagens e comunicação entre os servidores e o serviço de referência.

* Porta `5555`: entrada das requisições dos clientes
* Porta `5556`: saída do broker para os servidores
* Porta `5560`: serviço de referência

## Pub/Sub

Utilizado para publicação de mensagens nos canais, distribuição entre clientes inscritos e divulgação do coordenador eleito no tópico `servers`.

* Porta `5557`: XSUB
* Porta `5558`: XPUB

Os servidores atuam como publishers e os clientes como subscribers. Cada canal funciona como um tópico independente.

---

# Linguagens utilizadas

* **Python**
* **Java**
* **C**

O sistema também possui:

* um **broker** para comunicação Req/Rep;
* um **proxy** para comunicação Pub/Sub;
* um **serviço de referência**.

---

# Serialização

O grupo utilizou **MessagePack** como formato de serialização por ser binário, leve e compatível entre diferentes linguagens. Isso permitiu a troca padronizada de mapas, listas, strings e números entre os processos.

Todas as mensagens enviadas possuem:

* tipo da operação;
* timestamp;
* contador lógico;
* campos específicos da requisição.

---

# Partes 1 e 2 - Comunicação e persistência

Implementamos as operações de:

* login;
* criação de canais;
* listagem de canais;
* publicação de mensagens;
* inscrição em canais;
* recebimento de mensagens.

Os clientes enviam requisições ao broker, que as distribui entre os servidores disponíveis. Quando uma mensagem é publicada em um canal, o servidor envia o conteúdo ao proxy Pub/Sub, permitindo que todos os clientes inscritos recebam a mensagem em tempo real.

## Persistência dos dados

Cada servidor salva informações localmente utilizando os arquivos:

* `channels.json`
* `logins.json`
* `publications.jsonl`
* `requests.jsonl`

A pasta `shared` é utilizada para armazenar dados compartilhados entre os servidores, como informações do coordenador e canais globais.

---

# Parte 3 - Relógios e heartbeat

Nesta etapa, adicionamos relógio lógico nos clientes e servidores, heartbeat entre os servidores, serviço de referência, ranking dos servidores e sincronização simples do relógio físico.

## Relógio lógico

Cada processo mantém um contador lógico local.

Antes de enviar uma mensagem, o contador é incrementado e enviado junto com a requisição. Ao receber uma mensagem, o processo atualiza seu contador para o maior valor entre o contador local e o recebido.

Isso garante uma noção consistente da ordem dos eventos distribuídos no sistema.

---

# Parte 4 - Eleição de coordenador e Berkeley

Nesta parte, implementamos:

* detecção de falha do coordenador;
* eleição automática baseada no ranking dos servidores;
* publicação do coordenador eleito;
* sincronização de relógio físico utilizando uma versão simplificada do algoritmo de Berkeley.

O coordenador é sempre o servidor ativo com menor rank. Caso ele pare de responder, uma nova eleição é iniciada automaticamente pelos demais servidores.

O coordenador eleito também publica sua identificação no tópico `servers`, permitindo que os outros processos saibam qual servidor está atuando como líder naquele momento.

---

# Parte 5 - Consistência e Replicação

## Método escolhido: Replicação Passiva (Primary-Backup)

O projeto já possuía mecanismos de eleição de coordenador e heartbeat, então a Replicação Passiva se encaixou bem na arquitetura existente.

O coordenador eleito passa a atuar como **primário**, enquanto os demais servidores funcionam como **backups**. Caso o primário falhe, o sistema de eleição já existente escolhe automaticamente um novo coordenador, que assume também o papel de primário.

## Como o problema foi resolvido

O broker distribui requisições em round-robin, fazendo com que cada servidor receba apenas parte das operações. Sem replicação, isso faria com que os dados ficassem inconsistentes entre os servidores.

Para resolver esse problema, todas as operações de escrita passam pelo primário:

1. Se o servidor que recebeu a requisição já for o primário, ele processa a operação localmente e replica o evento para os backups.
2. Se o servidor for um backup, ele encaminha a requisição ao primário, que realiza o processamento e replica os dados para os demais servidores.
3. Operações de leitura, como `list_channels`, podem ser respondidas localmente por qualquer servidor.

Dessa forma, ao final de cada escrita, todos os servidores possuem os mesmos dados.

---

# Implementação

## Portas de replicação

Foram adicionadas portas específicas para replicação, separadas das portas utilizadas pela eleição:

| Servidor      | Porta |
| ------------- | ----- |
| server_c      | 5580  |
| server_python | 5581  |
| server_java   | 5582  |

---

## Fluxo de uma escrita

O cliente envia uma requisição para o broker, que distribui as mensagens entre os servidores usando round-robin.

Se a requisição chegar diretamente no servidor primário, ele processa a operação localmente e depois replica os dados para os backups.

Caso a requisição chegue em um servidor backup, esse servidor encaminha a operação para o primário. O primário então processa a escrita e replica as informações para os outros servidores.

Com isso, todos os servidores acabam mantendo os mesmos dados, independentemente de qual servidor recebeu a requisição inicialmente.

---

## Eventos de replicação

```json
{"type": "replicate_login", "user": "...", "timestamp": 0}

{"type": "replicate_channel", "channel": "..."}

{
  "type": "replicate_publish",
  "channel": "...",
  "user": "...",
  "message": "...",
  "request_timestamp": 0,
  "published_timestamp": 0,
  "contador": 0
}
```

---

## Thread de replicação

Cada servidor possui uma thread dedicada para escutar sua porta de replicação.

Quando o servidor atua como backup, essa thread aplica os eventos recebidos do primário. Quando atua como primário, ela também recebe e processa requisições encaminhadas pelos backups.

O acesso às estruturas compartilhadas é protegido por:

* `mutex` em C e Java;
* `threading.Lock` em Python.

---

# Diferenças em relação ao modelo clássico

No modelo clássico de Replicação Passiva, o primário normalmente é fixo. Neste projeto, o primário é dinâmico e depende do resultado da eleição de coordenador.

Durante uma troca de coordenador, pode acontecer de uma escrita ser encaminhada para um primário que acabou de falhar. Nesse caso, a operação retorna erro e o cliente pode tentar novamente após a nova eleição.

A replicação também foi implementada de forma assíncrona, utilizando threads separadas. Isso reduz a latência das operações, mas permite pequenas janelas de inconsistência temporária entre os backups, caracterizando consistência eventual — suficiente para os requisitos do projeto.

---

# Como executar

```bash
docker compose up --build
```
