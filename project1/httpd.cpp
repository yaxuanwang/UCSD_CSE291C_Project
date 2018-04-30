#include <sys/socket.h>    /* for socket(), bind(), and connect() */
#include <sys/sendfile.h>  /* for sendfile() */
#include <netinet/in.h>    /* for sockaddr_in and inet_ntoa() */
#include <string.h>        /* for memset() */
#include <arpa/inet.h>     /* for sockaddr_in and inet_ntoa() */
#include <thread>          /* for thread() */
#include <unistd.h>        /* for close() */
#include <fcntl.h>         /* for open() and O_RDONLY */
#include <sstream>         /* for stringstream */
#include <limits.h>	       /* for PATH_MAX */
#include <errno.h>		   /* for errno */
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

	if ((serv_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		cerr << "Failed to create socket" << endl;
        exit(1);
	}

	// bind server socket to local port and start listening
    memset(&serv_addr, 0, sizeof(serv_addr));   
    serv_addr.sin_family = AF_INET;                
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    serv_addr.sin_port = htons(port);      		  
    
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
            
        // connected to a client, handle concurrent connects with timeout
        cerr << "Server: got connection from " << inet_ntoa(clnt_addr.sin_addr)
        	<< " port " << ntohs(clnt_addr.sin_port) << endl;
        
    	struct timeval timeout;      
		timeout.tv_sec = 5;
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

        std::thread td(handle_connection, clnt_sock, doc_root);
        td.detach();
    }
    close(serv_sock);
}


void handle_connection (int clientSocket, string doc_root) {
    int clntSocket = clientSocket;
    const int BUFSIZE = 1024;
    char readBuffer[BUFSIZE+1];        
    int recvMsgSize;            
    httpFramer framer;
    httpRequest requestMsg;
    httpResponse responseMsg;
  
    // Receive message from client
    while (1) {
    	recvMsgSize = recv(clntSocket, readBuffer, BUFSIZE, 0);

    	// handle time out case and create 400 response
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
    	
    	// append message read to framer
    	string readStr(readBuffer);
    	framer.append(readStr.substr(0, recvMsgSize));

    	// while framer receives complete message, parser parses it into http request format
    	while (framer.hasMessage()) {      
    		requestMsg = httpParser::parse(framer.topMessage()); 
    		framer.popMessage();

    		// server processes request, parser parses it into http response message
    		responseMsg = process_request(requestMsg, doc_root);
    		string retMsg = httpParser::create_response(responseMsg);
    		
    		// send back response header
    		const char *res = retMsg.c_str();
    		send(clntSocket, res, retMsg.size(), 0);

    		// 
    		if(responseMsg.status == "200") {
    			int fd;
    			if ((fd = open(responseMsg.path.c_str(), O_RDONLY)) < 0) {
    				cerr << "Failed to read file" << endl;
        			exit(1);
    			}
    			std::stringstream sstream(responseMsg.key_values["Content-Length"]);
				size_t len;
				sstream >> len;
    			sendfile(clntSocket, fd, 0, len);
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

	// construct http response format
	if (requestMsg.valid) {
		string path = doc_root + '/' + requestMsg.URL;
		char buf[PATH_MAX + 1];
		char* rp =  ::realpath(path.c_str(), buf);   // simplify absolute path

		// handle different file types
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

	if (requestMsg.key_values.count("Host")) {
		response.key_values["Server"] = requestMsg.key_values["Host"];
	}
	else {
		response.key_values["Server"] = "unknown";
	}
	
	return response;
}


