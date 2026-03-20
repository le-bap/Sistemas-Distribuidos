FROM gcc:12

RUN apt-get update && apt-get install -y \
    libzmq3-dev \
    libmsgpack-dev

WORKDIR /app

COPY client.c server.c ./

RUN gcc client.c -lzmq -lmsgpackc -o client
RUN gcc server.c -lzmq -lmsgpackc -o server

CMD ["./client"]