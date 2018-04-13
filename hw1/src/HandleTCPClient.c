#include <stdio.h>      /* for printf() and fprintf() */
#include <sys/socket.h> /* for recv() and send() */
#include <unistd.h>     /* for close() */
#include <iostream>
#include <assert.h>
#include <inttypes.h>
#include "CalcServer.h"
#include "CalcFramer.hpp"
#include "CalcParser.hpp"

using namespace std;

void HandleTCPClient(int clntSocket)
{
	// PUT YOUR CODE HERE
	char readBuffer[2048];        /* Buffer for echo string */
    int recvMsgSize;             /* Size of received message */
    CalcFramer framer;
    CalcInstruction retCalInstr;
    string ansStr;
  
    /* Receive message from client */
    while (1) {
    	// printf("Handling client1");
    	if ((recvMsgSize = recv(clntSocket, readBuffer, 1024, 0)) <= 0)
    		break;
    	string readStr(readBuffer);
    	framer.append(readStr.substr(0, recvMsgSize));
    	while (framer.hasMessage()) {      /* Contain a full request */ 
    		/* parse it */
    		// printf("Handling client2 \n");
    		retCalInstr = CalcParser::parse(framer.topMessage()); 
    		framer.popMessage();
    		 // when meet blank<CRLF>, send back client
    		if (retCalInstr.operation == "") {
    			ansStr = to_string(CalcParser::accumulator);
    			ansStr += "\r\n";	
    			CalcParser::accumulator = 0;
    			char ans[ansStr.size()+1];
    			strcpy(ans, ansStr.c_str()); 
    			send(clntSocket, ans, ansStr.size(), 0);
    		}
    	} 	
    	// cout << "ans is:" << ans <<endl;
    	// char array[20];
    	// array[0] = 0, array[1] = 1;
    	// send(clntSocket, (char*)(&array), 2*sizeof(int), 0);
    	// recvMsgSize = 0;	
    }
	
    close(clntSocket);    /* Close client socket */
}
