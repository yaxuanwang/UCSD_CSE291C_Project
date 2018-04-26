#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>         // std::this_thread::sleep_for
#include <chrono>         // std::chrono::seconds
#include <iostream>
#include <string>
// Client side C/C++ program to demonstrate Socket programming
#define BUFSIZE 500
int main(int argc, char *argv[])
{
	long int port = strtol(argv[1], NULL, 10);
    std::string doc_root = argv[2];
    struct sockaddr_in address;
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    std::string message = "GET " + doc_root + " HTTP/1.1\r\nHost: localhost\r\nUser-Agent: cse291-tester/1.0\r\n\r\n";
    // char hello[BUFSIZE] = "GET /files//web.html HTTP/1.1\r\nHost: localhost\r\nUser-Agent: cse291-tester/1.0\r\n\r\n";
    char buffer[1024] = {0};
    if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }
  
    memset(&serv_addr, '0', sizeof(serv_addr));
  
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */  
  
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed \n");
        return -1;
    }
    if (send(sock , message.c_str() , message.length() , 0 ) < 0) {
    	printf("error");
    };
    // printf("Hello message sent\n");
    int numRecv;
    numRecv = recv(sock, buffer, BUFSIZE, 0);
    // valread = read( sock , buffer, 1024);
    while(numRecv > 0) {
        printf("%s\n",buffer );
        memset(buffer, 0, sizeof buffer);
        numRecv = recv(sock, buffer, BUFSIZE, 0);
    }
  //   printf("%s\n",buffer );
  //   std::cout << "countdown:\n";
  // for (int i=10; i>0; --i) {
  //   std::cout << i << std::endl;
  //   std::this_thread::sleep_for (std::chrono::seconds(1));
  // }
    return 0;
}