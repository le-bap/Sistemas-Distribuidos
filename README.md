# Projeto de Sistemas Distribuídos - Partes 1, 2, 3 e 4

## Integrantes

- Rafaela Altheman de Campos
- Letizia Lowatzki Baptistella
- Manuella Filipe Peres

---

## Sobre o projeto

Este projeto foi desenvolvido para a disciplina de Sistemas Distribuídos.

Nas Partes 1 e 2, implementamos a comunicação entre clientes e servidores utilizando os padrões **Req/Rep** e **Pub/Sub**.

Nas Partes 3 e 4, adicionamos conceitos de sincronização em sistemas distribuídos, incluindo:

- relógio lógico;
- heartbeat dos servidores;
- serviço de referência;
- ranking dos servidores;
- eleição de coordenador;
- sincronização baseada no algoritmo de Berkeley.

Com a troca de mensagens, é possível:

- fazer login;
- criar canais;
- listar canais;
- publicar mensagens;
- se inscrever em canais;
- receber mensagens publicadas;
- manter relógios lógicos nas mensagens;
- monitorar servidores ativos;
- eleger um coordenador entre os servidores.

Além disso, os servidores armazenam os dados para não perder as informações entre execuções.

---

## Arquitetura

O sistema usa dois padrões principais de comunicação:

### Req/Rep

Usado para:

- login;
- criação de canal;
- listagem de canais;
- envio de mensagens;
- comunicação dos servidores com o serviço de referência.

Portas do broker:

- `5555`: entrada das requisições dos clientes;
- `5556`: saída para os servidores.

Porta do serviço de referência:

- `5560`: comunicação entre servidores e referência.

### Pub/Sub

Usado para:

- publicação de mensagens nos canais;
- distribuição de mensagens entre os bots;
- recebimento de mensagens dos canais inscritos;
- publicação do coordenador eleito no tópico `servers`.

Portas do proxy:

- `5557`: XSUB;
- `5558`: XPUB.

O servidor atua como **publisher**, enviando mensagens para os canais.  
Os clientes atuam como **subscribers**, se inscrevendo nos canais e recebendo as mensagens.

Cada canal funciona como um **tópico**, e os clientes recebem apenas mensagens dos canais em que estão inscritos.

---

## Linguagens utilizadas

No projeto foram utilizadas 3 linguagens para implementar clientes e servidores:

- **Python**
- **Java**
- **C**

Também foram utilizados:

- um **broker**, responsável por intermediar a comunicação Req/Rep;
- um **proxy**, responsável por intermediar a comunicação Pub/Sub;
- um **serviço de referência**, responsável por manter os servidores ativos e seus rankings.

---

## Serialização escolhida

O grupo escolheu utilizar **MessagePack** como formato de serialização.

Essa escolha foi feita porque:

- é um formato binário;
- funciona entre diferentes linguagens;
- permite enviar mapas, strings, listas e números de forma padronizada;
- facilita a troca de mensagens entre Python, Java e C.

Todas as mensagens trocadas entre cliente e servidor possuem:

- tipo da mensagem;
- timestamp do envio;
- contador lógico;
- e os outros campos necessários, como usuário, canal ou mensagem.

---

# Parte 1 e 2 - Comunicação e persistência

Nas Partes 1 e 2, o objetivo foi implementar a comunicação básica entre clientes e servidores.

Foram implementadas as seguintes operações:

- login;
- criação de canais;
- listagem de canais;
- publicação de mensagens;
- inscrição em canais;
- recebimento de mensagens publicadas.

Os clientes enviam requisições para o broker, e o broker encaminha as mensagens para algum servidor disponível.

Quando uma mensagem é publicada em um canal, o servidor envia essa mensagem para o proxy Pub/Sub. Os clientes inscritos naquele canal recebem a mensagem.

---

## Persistência dos dados

Para não perder os dados entre as sessões, cada servidor salva suas informações em disco.

Os dados salvos são:

- logins realizados;
- canais criados;
- requisições recebidas;
- publicações feitas.

Foram utilizados arquivos:

- `channels.json`;
- `logins.json`;
- `publications.jsonl`;
- `requests.jsonl`.

A pasta `shared` é utilizada para informações compartilhadas entre os servidores, como canais e dados do coordenador.

---

# Parte 3 - Relógios e heartbeat

Na Parte 3, adicionamos relógios para controlar a ordem dos eventos e sincronizar os servidores.

Foram implementados:

- relógio lógico nos clientes;
- relógio lógico nos servidores;
- envio do contador em todas as mensagens;
- serviço de referência;
- heartbeat dos servidores;
- ranking dos servidores;
- lista de servidores ativos;
- sincronização simples do relógio físico usando o serviço de referência.

---

## Relógio lógico

O relógio lógico foi implementado usando um contador.

Cada processo possui seu próprio contador local.

A regra utilizada foi:

1. Antes de enviar uma mensagem, o contador é incrementado.
2. O contador é enviado junto com a mensagem.
3. Ao receber uma mensagem, o processo compara o contador recebido com o contador local.
4. O novo valor do contador local passa a ser o maior valor entre os dois.

---

## Como executar
Para executar o projeto, basta rodar:

```bash
docker compose up --build
