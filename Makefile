# flags
CXX = g++
CXXFLAGS = -std=c++11 -O2 -Wall
CLIENT_LIBFLAGS = -lpthread -lrdmacm -libverbs
SERVER_LIBFLAGS = -lpthread -lrdmacm -libverbs -lpmem
# LD = ld

# targets
CLIENT_TARGET = client
SERVER_TARGET = server
# COMPILETARGET = main
CLIENT_OBJS = ib.o network.o client.o
SERVER_OBJS = OCCHash.o db.o gc.o allocator.o nvm.o ib.o network.o global.o object.o lock.o server.o

.PHONY = all client server clean

all : clean client server

client : $(CLIENT_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(CLIENT_OBJS) $(CLIENT_LIBFLAGS)

server : $(SERVER_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(SERVER_OBJS) $(SERVER_LIBFLAGS)

clean : 
	rm -f $(CLIENT_TARGET) $(CLIENT_OBJS) $(SERVER_TARGET) $(SERVER_OBJS)

client.o : kvclient.cpp forcakv-client.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(CLIENT_LIBFLAGS)

server.o : kvserver.cpp forcakv-server.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)

db.o : db.cpp db.h object.h 
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)
	
nvm.o : nvm.cpp nvm.h object.h OCCHash.h 
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)

allocator.o : allocator.cpp allocator.h object.h OCCHash.h 
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)

ib.o : ib.cpp ib.h network.h 
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(CLIENT_LIBFLAGS)

network.o : network.cpp network.h 
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(CLIENT_LIBFLAGS)

OCCHash.o : OCCHash.cpp OCCHash.h util.h nvm.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)

object.o : object.cpp object.h util.h 
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)

global.o : global.cpp global.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)

gc.o : gc.cpp gc.h util.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(SERVER_LIBFLAGS)
	
lock.o : lock.cpp lock.h types.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(CLIENT_LIBFLAGS)

