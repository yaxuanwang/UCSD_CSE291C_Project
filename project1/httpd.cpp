#include <sys/socket.h>    /* for socket(), bind(), and connect() */
// #include <sys/sendfile.h>
#include <netinet/in.h>    /* for sockaddr_in and inet_ntoa() */
#include <string.h>        /* for memset() */
#include <arpa/inet.h>     /* for sockaddr_in and inet_ntoa() */
#include <iostream>
// #include <pthread.h> 
#include <thread>          /* for thread() */
#include <limits.h>
#include <unistd.h>        /* for close() */
#include <fcntl.h>
#include <sstream>         /* for stringstream */
#include <errno.h>
#include "httpd.h"

using namespace std;


void start_httpd(unsigned short port, string doc_root)
{
	cerr << "Starting server (port: " << port <<
		", doc_root: " << doc_root << ")" << endl;

	int serv_sock;                    
    int clnt_sock;                     
	struct sockaddr_in serv_addr;
	struct sockaddr_in clnt_addr;
	// pthread_t thread_id; 

	if ((serv_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		cerr << "Failed to create socket" << endl;
        exit(1);
	}

    memset(&serv_addr, 0, sizeof(serv_addr));   
    serv_addr.sin_family = AF_INET;                
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = htons(port);      		  

    // bind server socket to local port and start listening
    if (::bind(serv_sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {	
        cerr << "Failed to bind" << endl;
        exit(1);
    }
    if (listen(serv_sock, 5) < 0) {
        cerr << "Failed to listen" << endl;
        exit(1);
    }

    while (1) {
    	// wait for client to connect
        unsigned int clnt_len = sizeof(clnt_addr);

        if ((clnt_sock = accept(serv_sock, (struct sockaddr *) &clnt_addr, 
                               &clnt_len)) < 0) {
        	cerr << "Failed to accept" << endl;
        	exit(1);
        }
            
        // connected to a client, handle concurrently
        cerr << "Server: got connection from " << inet_ntoa(clnt_addr.sin_addr)
        	<< " port " << ntohs(clnt_addr.sin_port) << endl;
        
     	// concurrent connect with timeout
    	struct timeval timeout;      
		timeout.tv_sec = 10;
		timeout.tv_usec = 0;

		if (setsockopt (clnt_sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,
            sizeof(timeout)) < 0) {
    		cerr << "setsockopt failed" << endl;
    		exit(1);
		}
		if (setsockopt (clnt_sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout,
            sizeof(timeout)) < 0) {
    		cerr << "setsockopt failed" << endl;
    		exit(1);
		}
     	// // multi-thread to implement concurrency
      //   if (::pthread_create(&thread_id , NULL, handle_connection, (void*) &clnt_sock) < 0)
      //   {
      //   	cerr << "Failed to create thread" << endl;
      //       exit(1);
      //   }
        std::thread td(handle_connection, clnt_sock, doc_root);
        td.detach();
    }
    close(serv_sock);
}


void handle_connection (int clientSocket, string doc_root) {
	// int clntSocket = *(int*)clientSocket;
	// string doc_root = *(string *) root;
    int clntSocket = clientSocket;
    const int BUFSIZE = 1024;
    char readBuffer[BUFSIZE+1];        
    int recvMsgSize;            
    httpFramer framer;
    httpRequest requestMsg;
    httpResponse responseMsg;
  
    // Receive message from client
    while (1) {
    	// cout << "Handling client1" << endl;
    	recvMsgSize = recv(clntSocket, readBuffer, BUFSIZE, 0);
    	if (recvMsgSize < 0 && errno == EWOULDBLOCK) {
    		cout << "Timeout" <<endl;
    		responseMsg.http_version = "HTTP/1.1";
    		responseMsg.status = "400";
    		responseMsg.text = "Client Error";
    		responseMsg.key_values["Server"] = "localhost";
    		responseMsg.body = "<html><body><h1>400 Client Error</h1></body></html>";
			responseMsg.key_values["Content-Type"] = "text/html";
			responseMsg.key_values["Content-Length"] = to_string(responseMsg.body.size());
    		string retMsg = httpParser::create_response(responseMsg);
    		const char *res = retMsg.c_str();
    		send(clntSocket, res, retMsg.size(), 0);
    		break; 	
    	}

    	if (recvMsgSize < 0) {
    		cerr << "Failed to receive" << endl;
        	exit(1);
    	}

    	if (recvMsgSize == 0) {
    		break;
    	}
    		
    	string readStr(readBuffer);
    	framer.append(readStr.substr(0, recvMsgSize));
    	while (framer.hasMessage()) {      
    		// cout << "Handling client2 \n";
    		requestMsg = httpParser::parse(framer.topMessage()); 
    		framer.popMessage();

    		responseMsg = process_request(requestMsg, doc_root);
    		string retMsg = httpParser::create_response(responseMsg);
    		
    		const char *res = retMsg.c_str();
    		send(clntSocket, res, retMsg.size(), 0);
    		if(responseMsg.status == "200") {
    			int fd;
    			if ((fd = open(responseMsg.path.c_str(), O_RDONLY)) < 0) {
    				cerr << "Failed to read file" << endl;
        			exit(1);
    			}
    			std::stringstream sstream(responseMsg.key_values["Content-Length"]);
				size_t len;
				sstream >> len;
    			// sendfile(clntSocket, fd, 0, len);
    			close(fd);
    		}

    		if (requestMsg.key_values.count("Connection") &&
    			requestMsg.key_values["Connection"] == "close")
    			break;
    	} 	
    }
    close(clntSocket); 		
}


httpResponse process_request(httpRequest requestMsg, string doc_root) {
	httpResponse response;
	response.http_version = "HTTP/1.1";
	if (requestMsg.valid) {
		string path = doc_root + '/' + requestMsg.URL;
		// cout << "path is: " << path << endl;
		char buf[PATH_MAX + 1];
		char* rp =  ::realpath(path.c_str(), buf);
		cout << "abs_path is: " << buf << endl;
		if (rp!= NULL) {
			string abs_path(buf);
			if (abs_path.size() < doc_root.size() || 
				abs_path.substr(0, doc_root.size()) != doc_root ||
				!file_exists(abs_path)) {
				response.status = "404";
				response.text = "Not Found";
				response.key_values["Content-Type"] = "text/html";
			}
			else if (has_permission(abs_path)) {
				response.status = "200";
				response.text = "OK";
				response.path = abs_path;
				response.key_values["Last-Modified"] = get_last_modified(abs_path);
				response.key_values["Content-Type"] = get_content_type(abs_path);
				response.key_values["Content-Length"] = to_string(get_content_Length(abs_path));
			}
			else {
				response.status = "403";
				response.text = "Forbidden";
				response.body = "<html><body><h1>403 Forbidden</h1></body></html>";
				response.key_values["Content-Type"] = "text/html";
				response.key_values["Content-Length"] = to_string(response.body.size());
			}
		}
		else {
			response.status = "404";
			response.text = "Not Found";
			response.body = "<html><body><h1>404 Not Found</h1></body></html>";
			response.key_values["Content-Type"] = "text/html";
			response.key_values["Content-Length"] = to_string(response.body.size());
		}
	}
	else {
		response.status = "400";
		response.text = "Client Error";
		response.body = "<html><body><h1>400 Client Error</h1></body></html>";
		response.key_values["Content-Type"] = "text/html";
		response.key_values["Content-Length"] = to_string(response.body.size());
	}

	// key values
	if (requestMsg.key_values.count("Host")) {
		response.key_values["Server"] = requestMsg.key_values["Host"];
	}
	else {
		response.key_values["Server"] = "unknown";
	}
	
	return response;
}


