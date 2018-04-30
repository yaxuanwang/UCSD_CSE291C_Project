CC=g++
CFLAGS=-std=c++11
#-Wall -Wextra -pedantic  
DEPS = httpd.h httpParser.hpp httpFramer.hpp fileHelper.hpp
SRCS = httpd.cpp httpParser.cpp httpFramer.cpp fileHelper.cpp
MAIN_SRCS =  $(wildcard *.cpp) 
MAIN_OBJS = $(MAIN_SRCS:.cpp=.o)

default: httpd 

httpd:   $(MAIN_OBJS)
	$(CC) $(CFLAGS) -o $@ $(MAIN_OBJS) -lpthread

%.o: %.cpp 
	$(CC) $(CFLAGS) -c -o $@ $< 
.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f httpd *.o
