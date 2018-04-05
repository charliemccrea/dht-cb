CC=mpicc
CFLAGS=-g -O2 --std=c99 -Wall
LDFLAGS=-g -O2 -lpthread

OBJS=main.o dht.o dhtOG.o local.o
EXE=dht dhtOG

$(EXE): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f $(OBJS) $(EXE)

cleantxt:
	rm -f dump-*.txt
