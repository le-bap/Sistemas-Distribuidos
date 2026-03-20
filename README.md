# Projeto de Sistemas Distribuídos - Parte 1

## Integrantes
- Nome 1
- Nome 2
- Nome 3

## Sobre o projeto
Este projeto foi desenvolvido para a Parte 1 da disciplina de Sistemas Distribuídos.

Nesta etapa, implementamos a comunicação inicial entre clientes e servidores, permitindo que os bots consigam:

- fazer login no serviço
- criar canais
- listar os canais existentes

Além disso, os servidores armazenam os dados em disco para não perder as informações entre execuções.

---

## Linguagens utilizadas
No projeto foram utilizadas 3 linguagens para implementar clientes e servidores:

- **Python**
- **Java**
- **C**

Também foi utilizado um **broker**, responsável por intermediar a comunicação entre clientes e servidores.

---

## Serialização escolhida
O grupo escolheu utilizar **MessagePack** como formato de serialização.

Essa escolha foi feita porque:

- é um formato **binário**, como pedido no enunciado
- funciona entre diferentes linguagens
- é mais simples de usar no projeto
- permite enviar mapas, strings, listas e números de forma padronizada

Todas as mensagens trocadas entre cliente e servidor possuem:

- **tipo da mensagem**
- **timestamp do envio**
- e os outros campos necessários, como usuário ou canal

Exemplos de operações implementadas:

- login
- criação de canal
- listagem de canais

---

## Persistência dos dados
Para não perder os dados entre as sessões, cada servidor salva suas informações em disco.

Cada servidor mantém seu **próprio conjunto de dados**, sem compartilhar arquivos com os outros servidores, conforme pedido no enunciado.

Os dados salvos são:

- **logins realizados**, junto com o timestamp
- **canais criados**

A persistência foi feita em arquivos dentro da pasta `data`, separados por linguagem:

- `data/python/`
- `data/java/`
- `data/c/`

Foram utilizados arquivos como:

- `channels.txt`
- `logins.txt`
- `channels.json`
- `logins.json`

Os arquivos `.txt` e `.json` são usados apenas para armazenamento em disco.  
Na comunicação entre cliente e servidor **não é usado JSON**, apenas **MessagePack**.

---

## Estrutura geral do projeto
O projeto possui:

- clientes em Python, Java e C
- servidores em Python, Java e C
- broker
- Dockerfiles
- docker-compose.yaml
- pasta de dados para persistência

---

## Como executar
Para executar o projeto, basta rodar:

```bash
docker compose up --build
