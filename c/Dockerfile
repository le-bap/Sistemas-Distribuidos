# FROM gcc:12

# RUN apt-get update && apt-get install -y \
#     libzmq3-dev \
#     libmsgpack-dev

# WORKDIR /app

# COPY client.c server.c ./

# RUN gcc client.c -lzmq -lmsgpackc -lpthread -o client
# RUN gcc server.c -lzmq -lmsgpackc -o server

# CMD ["./client"]

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    make \
    libzmq3-dev \
    libmsgpack-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN gcc client.c -lzmq -lmsgpackc -lpthread -o client
RUN gcc server.c -lzmq -lmsgpackc -o server

CMD ["./client"]