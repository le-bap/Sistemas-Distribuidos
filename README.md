# Projeto de Sistemas Distribuídos - Partes 1, 2, 3, 4 e 5

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

Na Parte 5, foi adicionada a **replicação dos dados entre os servidores**, para que todos os servidores ativos mantenham uma cópia das operações realizadas no sistema.

Com a troca de mensagens, é possível:

- fazer login;
- criar canais;
- listar canais;
- publicar mensagens;
- se inscrever em canais;
- receber mensagens publicadas;
- manter relógios lógicos nas mensagens;
- monitorar servidores ativos;
- eleger um coordenador entre os servidores;
- sincronizar os relógios físicos com o coordenador;
- replicar dados entre os servidores.

Além disso, os servidores armazenam os dados em disco para não perder as informações entre execuções.

---

## Arquitetura

O sistema usa dois padrões principais de comunicação:

### Req/Rep

Usado para:

- login;
- criação de canal;
- listagem de canais;
- envio de mensagens;
- comunicação dos servidores com o serviço de referência;
- comunicação direta entre servidores para eleição e sincronização do relógio.

Portas do broker:

- `5555`: entrada das requisições dos clientes;
- `5556`: saída para os servidores.

Porta do serviço de referência:

- `5560`: comunicação entre servidores e referência.

Portas diretas dos servidores:

- `5570`: servidor C;
- `5571`: servidor Python;
- `5572`: servidor Java.

Essas portas são usadas para mensagens internas entre servidores, como eleição e pedido de hora ao coordenador.

### Pub/Sub

Usado para:

- publicação de mensagens nos canais;
- distribuição de mensagens entre os bots;
- recebimento de mensagens dos canais inscritos;
- publicação do coordenador eleito no tópico `servers`;
- replicação interna dos dados no tópico `replication`.

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
- publicações feitas;
- eventos de replicação aplicados.

Foram utilizados arquivos:

- `channels.json`;
- `logins.json`;
- `publications.jsonl`;
- `requests.jsonl`;
- `replicated_events.jsonl`;
- `applied_events.json`.

Cada servidor possui sua própria pasta de dados:

- `data/python`;
- `data/java`;
- `data/c`.

Assim, cada servidor mantém seu próprio conjunto de arquivos persistidos.

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
- lista de servidores ativos.

---

## Relógio lógico

O relógio lógico foi implementado usando um contador.

Cada processo possui seu próprio contador local.

A regra utilizada foi:

1. Antes de enviar uma mensagem, o contador é incrementado.
2. O contador é enviado junto com a mensagem.
3. Ao receber uma mensagem, o processo compara o contador recebido com o contador local.
4. O novo valor do contador local passa a ser o maior valor entre os dois.

Dessa forma, cada mensagem carrega uma informação de ordem lógica, permitindo acompanhar a sequência de eventos no sistema distribuído.

---

## Heartbeat

Os servidores enviam periodicamente uma mensagem de heartbeat para o serviço de referência.

O serviço de referência mantém:

- nome do servidor;
- rank do servidor;
- horário do último heartbeat recebido.

Caso um servidor fique muito tempo sem enviar heartbeat, ele é considerado inativo e pode ser removido da lista de servidores disponíveis.

---

# Parte 4 - Eleição e sincronização Berkeley

Na Parte 4, foi implementada a eleição de coordenador entre os servidores.

Cada servidor possui um rank informado pelo serviço de referência. O servidor com menor rank disponível é escolhido como coordenador.

Quando um servidor percebe que o coordenador atual não está respondendo, ele inicia uma eleição. Durante a eleição, os servidores ativos são consultados diretamente pelas portas internas.

Após a escolha, o novo coordenador é publicado no tópico:

```text
servers
```

---

# Parte 5 - Consistência e replicação

Na Parte 5, foi implementada a replicação dos dados entre os servidores.

Como o broker faz o balanceamento de carga entre os servidores, cada requisição pode ser atendida por um servidor diferente. Sem replicação, cada servidor teria apenas uma parte dos dados do sistema. Por exemplo, uma mensagem publicada em um servidor ficaria salva somente nele, e os outros servidores não teriam esse histórico.

Para resolver isso, implementamos uma replicação ativa usando Pub/Sub.

## Método escolhido

O método escolhido foi a **replicação ativa por difusão de eventos**.

Sempre que um servidor recebe uma operação que altera o estado do sistema, ele salva essa operação localmente e publica um evento de replicação para os outros servidores.

As operações replicadas são:

- login de usuário;
- criação de canal;
- publicação de mensagem.

Dessa forma, quando um servidor recebe uma dessas operações, os demais servidores ativos também recebem uma cópia e salvam a mesma informação localmente.

## Tópico de replicação

Foi criado um tópico interno chamado:

```text
replication
```

# Como executar

Para rodar o projeto, use o comando:

```bash
docker compose up --build


e em outro terminal
docker stop server_...