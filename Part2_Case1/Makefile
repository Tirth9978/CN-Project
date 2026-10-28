CC = gcc
CFLAGS = -Wall -Wextra -Werror -O2 -pthread
LDFLAGS = -pthread

all: server client

server: server.o common_net.o topic_manager.o fault_injector.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

client: client.o common_net.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o server client *.log

.PHONY: all clean
