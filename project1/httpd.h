#ifndef HTTPD_H
#define HTTPD_H

#include <string>
#include "httpFramer.hpp"
#include "httpParser.hpp"
#include "fileHelper.hpp"

using namespace std;

// start http connection
void start_httpd(unsigned short port, string doc_root);

// receive requests and send responses
void handle_connection (int clientSocket, string root);

// process input http request and create corresponding http response struct
httpResponse process_request(httpRequest requestMsg, string doc_root);


#endif // HTTPD_H
