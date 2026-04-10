# Projeto de Sistemas Distribuídos - Parte 1

## Integrantes
- Rafaela Altheman de Campos
- Letizia Lowatzki Baptistella
- Manuella Filipe Peres

## Sobre o projeto
Este projeto foi desenvolvido para a Parte 1 e 2 da disciplina de Sistemas Distribuídos.

Nestas etapas, implementamos a comunicação entre clientes e servidores utilizando Req/Res e Pub/Sub.

Com a troca de mensagens, é possível:

- fazer login  
- criar canais  
- listar canais  
- publicar mensagens (publish)  
- se inscrever em canais (subscribe)

Além disso, os servidores armazenam os dados para não perder as informações entre execuções.

---
## Arquitetura

O sistema usa dois padrões de comunicação:

### Req/Rep
Usado para:
- login  
- criação de canal  
- listagem de canais  
- envio de mensagens  

Portas:
- 5555 (entrada)
- 5556 (saída)


### Pub/Sub
Usado para:
- publicação de mensagens nos canais  
- distribuição de mensagens entre os bots  
- recebimento de mensagens dos canais inscritos  

Portas:
- 5557 (XSUB)
- 5558 (XPUB)

O servidor atua como **publisher**, enviando mensagens para os canais.  
Os clientes (bots) atuam como **subscribers**, se inscrevendo nos canais e recebendo as mensagens.

Cada canal funciona como um **tópico**, e os clientes recebem apenas mensagens dos canais em que estão inscritos.

---

## Linguagens utilizadas
No projeto foram utilizadas 3 linguagens para implementar clientes e servidores:

- **Python**
- **Java**
- **C**

Também foi utilizado um **broker**, responsável por intermediar a comunicação entre eles.

---

## Serialização escolhida
O grupo escolheu utilizar **MessagePack** como formato de serialização.

Essa escolha foi feita porque:

- é um formato **binário**
- funciona entre diferentes linguagens
- permite enviar mapas, strings, listas e números de forma padronizada

Todas as mensagens trocadas entre cliente e servidor possuem:

- **tipo da mensagem**
- **timestamp do envio**
- e os outros campos necessários, como usuário ou canal

---

## Persistência dos dados
Para não perder os dados entre as sessões, cada servidor salva suas informações em disco.

Cada servidor mantém seu **próprio conjunto de dados**, sem compartilhar arquivos com os outros servidores, conforme pedido no enunciado.

Os dados salvos são:

- logins realizados,
- canais criados
- publicações feitas

A persistência foi feita em arquivos dentro da pasta `data`, separados por linguagem:

- `data/python/`
- `data/java/`
- `data/c/`

Foram utilizados arquivos:

- `channels.json`
- `logins.json`
- `publications.jsonl`
- `requests.jsonl`

para serapação de cada tipo de mensagem.

---

## Estrutura geral do projeto
O projeto possui:

- clientes em Python, Java e C
- servidores em Python, Java e C
- broker
- proxy
- Dockerfiles
- docker-compose.yaml
- pasta de dados para persistência

---

## Como executar
Para executar o projeto, basta rodar:

```bash
docker compose up --build
